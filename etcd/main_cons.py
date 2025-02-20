import time
import etcd3
import jsonpickle
import json
import signal
import argparse
import socket
import random
from random import randbytes

def formatServiceEndpoint(index, namespace):
    if index == -1:
        return "my-etcd."+namespace+".svc.cluster.local" #Cluster IP Service
    else:
        return "my-etcd-{}.my-etcd-headless.".format(index)+namespace+".svc.cluster.local" #Headless Service

def formatEtcdWatchKeyMetrics(cons_group_id, prod_group_id):
    return "/METRICS/CONS"+str(cons_group_id)+"/PROD"+str(prod_group_id)+"/"

def formatEtcdWatchKeyControl(cons_group_id, prod_group_id):
    return "/CTRL/CONS"+str(cons_group_id)+"/PROD"+str(prod_group_id)+"/"

def formatEtcdStateKey(cons_id):
    return "/STATE_ID/"+str(cons_id)+"/"

def getNumMembers(etcd):
    return sum(1 for member in etcd.members)

def etcdClient(pod_idx, namespace):
    cluster_ip_etcd = etcd3.client(host=formatServiceEndpoint(index=-1, namespace=namespace), port=2379)
    if pod_idx == -1: #cluster-ip svc
        return cluster_ip_etcd
    elif pod_idx == -2: #list of all clients
        etcd_list = []
        for ii in range(0, getNumMembers(cluster_ip_etcd)):
            headless_endopoint = formatServiceEndpoint(index=ii, namespace=namespace)
            etcd_list.append(etcd3.client(host=headless_endopoint, port=2379))
        return etcd_list
    else:
        return etcd3.client(host=formatServiceEndpoint(index=pod_idx, namespace=namespace), port=2379)

def selEtcdClient(etcd_list, pod_idx):
    if pod_idx == -2:
      etcd = random.choice(etcd_list) #randomly extract an headless client
    elif pod_idx == -1:
      etcd = etcdClient(pod_idx, namespace) #cluster-ip
    else:
      etcd = etcd_list[pod_idx] #specific headless client
    return etcd

def checkStateQueue(etcd, state_queue):
    head, _ = etcd.get(state_queue+"HEAD")
    tail, _ = etcd.get(state_queue+"TAIL")
    if head is None or tail is None:
        if verbose:
            print("Queue not existing")
        return False
    else:
        if verbose:
            print("Queue already existing: Head: {}, Tail: {}".format(head.decode('UTF-8'), tail.decode('UTF-8')))
        return True

def initStateQueue(etcd, state_queue, queue_len):
    if verbose:
        print("Initializing "+state_queue)

    # Initialize state queue
    init_start = time.time()
    for ii in range(queue_len):
        metrics = randbytes(metrics_msg_size)
        etcd.put(state_queue+str(ii), jsonpickle.encode(metrics))
    init_end = time.time()
    init_time = float((init_end-init_start)*1000)
    print("INIT: ", init_time, "ms")

    # Store head/tail of the state queue
    etcd.put(state_queue+"HEAD", str(0))
    etcd.put(state_queue+"TAIL", str(queue_len-1))

    print("Queues ready")

def updateStateQueue(etcd, state_queue, new_metrics):
    # Get Head/Tail of the state queue
    head, _ = etcd.get(state_queue+"HEAD")
    tail, _ = etcd.get(state_queue+"TAIL")
    head = int(head.decode('UTF-8')); tail = int(tail.decode('UTF-8'))

    # Get (Serializable) the head of the state queue
    get_s_start = time.time()
    popped_value = etcd.get(state_queue+str(head), serializable=True)
    get_s_end = time.time()
    get_s_time = float((get_s_end-get_s_start)*1000)

    # Get (Linearizable) the head of the state queue
    get_l_start = time.time()
    popped_value = etcd.get(state_queue+str(head))
    get_l_end = time.time()
    get_l_time = float((get_l_end-get_l_start)*1000)

    # Pop the head of the state queue
    pop_start = time.time()
    etcd.delete(state_queue+str(head))
    pop_end = time.time()
    pop_time = float((pop_end-pop_start)*1000)

    # Push the tail of the state queue
    push_start = time.time()
    response=etcd.put(state_queue+str(tail), new_metrics)
    push_end = time.time()
    push_time = float((push_end-push_start)*1000)

    # Update Head/Tail of the state queue
    head = head + 1
    tail = tail + 1
    etcd.put(state_queue+"HEAD", str(head))
    response = etcd.put(state_queue+"TAIL", str(tail))

    # Get Member ID
    member_id = response.header.member_id

    # Print message
    if verbose:
        print("POPPED: ", popped_value)

    return get_l_time, get_s_time, pop_time, push_time, member_id

def send_ctrl_msg(etcd, ctrl_key, ctrl_msg_size):
    # Generate a control message of <N> Bytes
    ctrl = randbytes(ctrl_msg_size)
    message = jsonpickle.encode(ctrl)

    # Add message to Redis stream
    ctrl_put_start = time.time()
    etcd.put(ctrl_key, message)
    ctrl_put_end = time.time()
    ctrl_put_time = float((ctrl_put_end-ctrl_put_start)*1000)

    # Print message
    if verbose:
        print(f"Consumer sent: {message}")

    return ctrl_put_time

def signal_handler(sig, frame):
    print("Deleting the control key")
    etcd.delete_prefix(ctrl_key)
    print("Deleting the state queue")
    res = etcd.delete_prefix(state_queue)
    res = etcd.delete_prefix(metrics_key)
    rev = res.header.revision
    etcd.compact(rev, True)
    exit(0)

if __name__ == '__main__':
    # Get unique ID
    cons_id = socket.gethostname()
    print("ID: {}".format(cons_id))

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Consumer MS emulator using ETCD to preserve state and send/receive metrics/control messages')
    parser.add_argument('--pod_idx', type=int, help='ETCD Pod Index to target. Default is -1, i.e., K8S ClusterIP SVC', default=-1)
    parser.add_argument("--namespace", type=str, help='Reference Kubernetes Namespace. Defaults to "default"', default="default")
    parser.add_argument('--state_size', type=float, help='State size in MB emulated as a queue of metrics messages. Defaults to 0.1MB', default=0.1)
    parser.add_argument('--metrics_msg_size', type=int, help='Metrics message size in bytes. Defaults to 1kB', default=1000)
    parser.add_argument('--ctrl_msg_size', type=int, help='Control message size in bytes. Defaults to 1kB', default=1000)
    parser.add_argument('--cons_group_id', type=int, help='Consumer MS group ID for multiple producer-consumer pairing. Defaults to 0.', default=0)
    parser.add_argument('--prod_group_id', type=int, help='Producer MS group ID for multiple producer-consumer pairing. Defaults to 0.', default=0)
    parser.add_argument('--latency', type=float, help='Inference latency in ms to simulate. Defaults to 50ms', default=50)
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    args = parser.parse_args()

    global pod_idx; global namespace; global state_size; global metrics_msg_size; global ctrl_msg_size; global latency; global cons_group_id; global prod_group_id; global verbose
    pod_idx = args.pod_idx; namespace = args.namespace; state_size = args.state_size; metrics_msg_size = args.metrics_msg_size; ctrl_msg_size = args.ctrl_msg_size; latency = args.latency; cons_group_id = args.cons_group_id; prod_group_id = args.prod_group_id; verbose = args.verbose

    # Register signal handler for termination signals
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Connect to Etcd
    etcd_list = etcdClient(-2, namespace) #cluser-ip/headless client
    etcd = selEtcdClient(etcd_list, pod_idx)

    # Setting keys
    metrics_key = formatEtcdWatchKeyMetrics(cons_group_id, prod_group_id)
    ctrl_key = formatEtcdWatchKeyControl(cons_group_id, prod_group_id)

    # Initialize the consumer MS State Queue
    state_queue = formatEtcdStateKey(cons_id)
    queue_len = round(state_size*1e6/metrics_msg_size) #normalize to [B] and divide by the msg size
    if not checkStateQueue(etcd, state_queue):
        initStateQueue(etcd, state_queue, queue_len)

    # Initialize Control Stream
    etcd.delete(ctrl_key)

    # Main consumer loop
    print(f"Consumer ID: {metrics_key} is ready. Waiting for messages from Etcd...")

    # Initializing index to merge json logs
    message_counter = 0

    #Initialize time and start watch
    start_time = time.time()
    events_iterator, watch_cancel = etcd.watch_prefix(metrics_key)

    for event in events_iterator:
        # Connect to Etcd
        etcd = selEtcdClient(etcd_list, pod_idx)

        idle_time = float((time.time() - start_time) * 1000)
        start_time = time.time()

        # Retrieve data from ETCD
        data_pickled = event.value
        metrics = jsonpickle.decode(data_pickled.decode())

        # Update state queue
        get_l_time, get_s_time, pop_time, push_time, member_id = updateStateQueue(etcd, state_queue, metrics)

        # Send control message
        ctrl_put_time = send_ctrl_msg(etcd, ctrl_key, ctrl_msg_size)

        # Print time measurement
        message_counter = message_counter + 1
        json_msg = {"idx" : message_counter, "timestamp" : round(time.time()), "client": member_id, "idle" : idle_time, "get_l" : get_l_time, "get_s" : get_s_time, "pop" : pop_time, "push" : push_time, "ctrl_put" : ctrl_put_time}
        print(json.dumps(json_msg))

        # Emulating ML inference
        time.sleep(latency/1e3)
