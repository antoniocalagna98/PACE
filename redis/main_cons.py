import numpy as np
from rediscluster import RedisCluster
import redis as RedisStandalone
import time
import signal
import multiprocessing
from random import randbytes
import jsonpickle
import uuid
import argparse
import json

def formatStandaloneEndpoint(index):
    if index == -1:
        return "my-redis-master"
    else:
        return "my-redis-headless"

def formatClusterEndpoint(index):
    if index == -1:
        return "my-redis-redis-cluster" #Cluster IP Service
    else:
        return "my-redis-redis-cluster-{}.my-redis-redis-cluster-headless".format(index) #Headless Service

def formatMetricsKey(cons_group_id, prod_group_id):
    return "/METRICS/CONS"+str(cons_group_id)+"/PROD"+str(prod_group_id)+"/"

def formatControlKey(cons_group_id, prod_group_id):
    return "/CTRL/CONS"+str(cons_group_id)+"/PROD"+str(prod_group_id)+"/"

def formatStateKey(cons_id):
    return "/STATE_ID/"+str(cons_id)+"/"

def redisClient(redis_kind, pod_idx):
    #Selecting Redis Standalone/Cluster
    if redis_kind == "cluster":
        startup_nodes = [{"host": formatClusterEndpoint(pod_idx), "port": "6379"}]
        redis = RedisCluster(startup_nodes=startup_nodes, decode_responses=False)
    elif redis_kind == "standalone":
        redis = RedisStandalone.Redis(host=formatStandaloneEndpoint(pod_idx), port=6379, decode_responses=False)
    else:
        print("Redis kind not defined!")
        exit(-1)
    #Return Redis Client
    return redis

def initStateQueue(redis, state_queue, queue_len):
    if verbose:
        print("Initializing "+state_queue)

    # Delete state queue if existing
    for key in redis.scan_iter(state_queue+"*"):
        redis.delete(key)

    # Initialize state queue
    init_start = time.time()
    for ii in range(queue_len):
        metrics = randbytes(metrics_msg_size)
        redis.set(state_queue+str(ii), jsonpickle.encode(metrics))
    init_end = time.time()
    init_time = float((init_end-init_start)*1000)
    print("INIT: ", init_time, "ms")

    # Store head/tail of the state queue
    redis.set(state_queue+"HEAD", 0)
    redis.set(state_queue+"TAIL", queue_len-1)

    print("Queues ready")

def updateStateQueue(redis, state_queue, new_metrics, message_counter, stdout_mutex):
    # Get Head/Tail of the state queue
    head = int(redis.get(state_queue+"HEAD").decode('UTF-8'))
    tail = int(redis.get(state_queue+"TAIL").decode('UTF-8'))

    # Get the head of the state queue
    get_start = time.time()
    popped_value = redis.get(state_queue+str(head))
    get_end = time.time()
    get_time = float((get_end-get_start)*1000)

    # Pop the head of the state queue
    pop_start = time.time()
    redis.delete(state_queue+str(head))
    pop_end = time.time()
    pop_time = float((pop_end-pop_start)*1000)

    # Push the tail of the state queue
    push_start = time.time()
    redis.set(state_queue+str(tail), new_metrics)
    push_end = time.time()
    push_time = float((push_end-push_start)*1000)

    # Update Head/Tail of the state queue
    head = head + 1
    tail = tail + 1
    redis.set(state_queue+"HEAD", head)
    redis.set(state_queue+"TAIL", tail)

    # Print time measurement
    json_msg = {"idx": message_counter, "get" : get_time, "pop" : pop_time, "push" : push_time}
    stdout_mutex.acquire()
    if verbose:
        print("POPPED: ", popped_value)
    print(json.dumps(json_msg))
    stdout_mutex.release()

def send_ctrl_msg(redis, ctrl_stream, ctrl_msg_size, message_counter, stdout_mutex):
    # Generate a control message of <N> Bytes
    ctrl = randbytes(ctrl_msg_size)
    message = jsonpickle.encode(ctrl)

    # Add message to Redis stream
    ctrl_xadd_start = time.time()
    redis.xadd(ctrl_stream, {'message': message}, maxlen=1)
    ctrl_xadd_end = time.time()
    ctrl_xadd_time = float((ctrl_xadd_end-ctrl_xadd_start)*1000)

    # Print time measurement
    json_msg = {"idx": message_counter, "ctrl" : ctrl_xadd_time, "ctrl_size" : ctrl_msg_size}
    stdout_mutex.acquire()
    if verbose:
        print(f"Consumer sent: {message}")
    print(json.dumps(json_msg))
    stdout_mutex.release()

def recv_metrics_msg(redis, metrics_stream, stdout_mutex):
    print(f"Consumer ID: {metrics_stream} is ready. Waiting for messages from Redis stream...")

    # Last ID that this consumer has seen. '$' means "new messages".
    last_id = '$'

    # Initializing index to merge json logs
    message_counter = 0

    while True:
        xread_start = time.time()
        # Read one new message from the stream starting from the last seen ID and block with unlimited timeout
        response = redis.xread({metrics_stream: last_id}, count=1, block=0)
        xread_end = time.time()
        xread_time = float((xread_end-xread_start)*1000)

        # Print time measurement
        message_counter = message_counter + 1
        json_msg = {"idx" : message_counter, "timestamp" : time.time(), "xread" : xread_time}
        stdout_mutex.acquire()
        print(json.dumps(json_msg))
        stdout_mutex.release()

        # Process metrics
        if response:
            _, messages = response[0]
            for message_id, message_data in messages:
                last_id = message_id  # Update last_id to the ID of the last processed message
                metrics = message_data[b'message']
                if verbose:
                    stdout_mutex.acquire()
                    print(f"Metrics message received: {metrics}")
                    stdout_mutex.release()

                # Update state queue
                multiprocessing.active_children() # Join all defunct children processes
                state_process = multiprocessing.Process(target=updateStateQueue, args=(redis,state_queue,metrics,message_counter,stdout_mutex,), daemon=True)
                state_process.start()
                #state_process.join()

                # Send control message
                ctrl_process = multiprocessing.Process(target=send_ctrl_msg, args=(redis,ctrl_stream,ctrl_msg_size,message_counter,stdout_mutex,), daemon=True)
                ctrl_process.start()
                #ctrl_process.join()

                # Emulating ML inference
                time.sleep(latency/1e3)

def signal_handler(sig, frame):
    if multiprocessing.current_process().name == 'MainProcess':
        print("\nJoining child processes")
        for child in multiprocessing.active_children():
            child.terminate()
            child.join()
        print("Deleting the redis control stream")
        redis.delete(ctrl_stream)
        print("Deleting the state queue")
        for key in redis.scan_iter(state_queue+"*"):
            redis.delete(key)
    exit(0)


if __name__ == "__main__":
    # Get unique ID
    cons_id = uuid.uuid4()
    print("ID: {}".format(cons_id))

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Consumer MS emulator using Redis to preserve state and send/receive metrics/control messages')
    parser.add_argument("--redis", type=str, help='Redis mode to use, i.e., standalone or cluster. Defaults to "cluster"', default="cluster")
    parser.add_argument('--pod_idx', type=int, help='Redis Pod Index to target. Defaults to -1, i.e., K8S ClusterIP SVC', default=-1)
    parser.add_argument("--namespace", type=str, help='Reference Kubernetes Namespace. Defaults to "default"', default="default")
    parser.add_argument('--state_size', type=float, help='State size in MB emulated as a queue of metrics messages. Defaults to 0.1MB', default=0.1)
    parser.add_argument('--metrics_msg_size', type=int, help='Metrics message size in bytes. Defaults to 1kB', default=1000)
    parser.add_argument('--ctrl_msg_size', type=int, help='Control message size in bytes. Defaults to 100B', default=100)
    parser.add_argument('--latency', type=float, help='Inference latency in ms to simulate. Defaults to 50ms', default=50)
    parser.add_argument('--cons_group_id', type=int, help='Consumer MS group ID for multiple producer-consumer pairing. Defaults to 0.', default=0)
    parser.add_argument('--prod_group_id', type=int, help='Producer MS group ID for multiple producer-consumer pairing. Defaults to 0.', default=0)
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    args = parser.parse_args()

    global redis_kind; global pod_idx; global namespace; global state_size; global metrics_msg_size; global ctrl_msg_size; global latency; global cons_group_id; global prod_group_id; global verbose
    redis_kind = args.redis; pod_idx = args.pod_idx; namespace = args.namespace; state_size = args.state_size; metrics_msg_size = args.metrics_msg_size; ctrl_msg_size = args.ctrl_msg_size; latency = args.latency; cons_group_id = args.cons_group_id; prod_group_id = args.prod_group_id; verbose = args.verbose

    # Register signal handler for termination signals
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Connect to Redis/Redis Cluster
    redis = redisClient(redis_kind, args.pod_idx)
    redis.config_set("activedefrag", "yes") #Enable active defragmentation

    # Setting streams
    metrics_stream = formatMetricsKey(cons_group_id, prod_group_id)
    ctrl_stream = formatControlKey(cons_group_id, prod_group_id)

    # Initialize the Consumer MS State Queue
    state_queue = formatStateKey(cons_id)
    queue_len = round(state_size*1e6/metrics_msg_size) #normalize to [B] and divide by the msg size
    initStateQueue(redis, state_queue, queue_len)

    # Initialize Control Stream
    redis.delete(ctrl_stream)

    # Main consumer loop
    stdout_mutex = multiprocessing.Lock()
    recv_metrics_msg(redis, metrics_stream, stdout_mutex)