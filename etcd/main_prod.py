import time
import argparse
import jsonpickle
import sys
import etcd3
import signal
import random
from random import randbytes
import json

def formatServiceEndpoint(index, namespace):
    if index == -1:
        return "my-etcd."+namespace+".svc.cluster.local" #Cluster IP Service
    else:
        return "my-etcd-{}.my-etcd-headless.".format(index)+namespace+".svc.cluster.local" #Headless Service

def formatEtcdWatchKeyMetrics(cons_group_id, prod_group_id):
    return "/METRICS/CONS"+str(cons_group_id)+"/PROD"+str(prod_group_id)+"/"

def formatEtcdWatchKeyControl(cons_group_id, prod_group_id):
    return "/CTRL/CONS"+str(cons_group_id)+"/PROD"+str(prod_group_id)+"/"

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

def signal_handler(sig, frame):
    print("Stopping the etcd watch")
    etcd.cancel_watch(etcd_watch_id)
    exit(0)

# Callback function that receives data from the Consumer using ETCD
def recv_ctrl_msg(watch_response):
    #print("Callback Executed with: ", watch_response)
    #print("Inspecting Header: ", watch_response.header)
    #print("Inspecting Payload: ", watch_response.events)
    for event in watch_response.events:
        print(time.time(), "Received: ", event.key.decode('UTF-8'))

def send_metrics_msg(etcd_list, pod_idx, metrics_key, metrics_msg_size, metrics_period):
    print(f"Producer ID: {metrics_key} is ready. Sending messages to Etcd...")

    # Initializing index to merge json logs
    message_counter = 0

    while True:
        # Generate a metrics message of <N> Bytes
        metrics = randbytes(metrics_msg_size)
        message = jsonpickle.encode(metrics)

        # Connect to Etcd
        etcd = selEtcdClient(etcd_list, pod_idx)

        # Put message on ETCD
        put_start = time.time()
        response = etcd.put(metrics_key, message)
        put_end = time.time()
        put_time = float((put_end-put_start)*1000)

        # Print time measurement
        message_counter = message_counter + 1
        json_msg = {"idx" : message_counter, "timestamp" : round(time.time()), "client" : response.header.member_id, "put" : put_time, "metrics_size" : metrics_msg_size, "metrics_period" : metrics_period}
        print(json.dumps(json_msg))
        if verbose:
            print(f"Producer sent: {message}")

        # Metrics period in seconds
        time.sleep(metrics_period)

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Producer MS emulator using Etcd to send/receive metrics/control messages')
    parser.add_argument('--pod_idx', type=int, help='Etcd Pod Index to target. Default is -1, i.e., K8S ClusterIP SVC', default=-1)
    parser.add_argument("--namespace", type=str, help='Reference Kubernetes Namespace. Defaults to "default"', default="default")
    parser.add_argument('--period', type=float, help='Time interval across messages. Defaults to 1s', default=1)
    parser.add_argument('--metrics_msg_size', type=int, help='Metrics message size in bytes. Defaults to 1kB', default=1000)
    parser.add_argument('--cons_group_id', type=int, help='Consumer MS group ID for multiple producer-consumer pairing. Defaults to 0.', default=0)
    parser.add_argument('--prod_group_id', type=int, help='Producer MS group ID for multiple producer-consumer pairing. Defaults to 0.', default=0)
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    args = parser.parse_args()

    global pod_idx; global namespace; global period; global metrics_msg_size; global cons_group_id; global prod_group_id; global verbose
    pod_idx = args.pod_idx; namespace = args.namespace; period = args.period; metrics_msg_size = args.metrics_msg_size; cons_group_id = args.cons_group_id; prod_group_id = args.prod_group_id; verbose = args.verbose

    # Register signal handler for termination signals
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Connect to Etcd
    etcd_list = etcdClient(-2, namespace) #cluser-ip/headless client
    etcd = selEtcdClient(etcd_list, pod_idx)

    # Setting keys
    metrics_key = formatEtcdWatchKeyMetrics(cons_group_id, prod_group_id)
    ctrl_key = formatEtcdWatchKeyControl(cons_group_id, prod_group_id)

    # Enable callback to receive control messages from the consumer through ETCD
    etcd_watch_id = etcd.add_watch_prefix_callback(ctrl_key, recv_ctrl_msg)

    # Main producer loop
    send_metrics_msg(etcd_list,pod_idx,metrics_key,metrics_msg_size,period)
