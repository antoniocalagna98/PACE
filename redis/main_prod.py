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

def signal_handler(sig, frame):
    if multiprocessing.current_process().name == 'MainProcess':
        print("\nJoining child processes")
        for child in multiprocessing.active_children():
            child.terminate()
            child.join()
        print("Deleting the redis metrics stream")
        redis.delete(metrics_stream)
    exit(0)

def send_metrics_msg(redis, metrics_stream, metrics_msg_size, metrics_period, stdout_mutex):
    print(f"Producer ID: {metrics_stream} is ready. Sending messages to Redis stream...")

    # Initializing index to merge json logs
    message_counter = 0

    while True:
        # Generate a metrics message of <N> Bytes
        metrics = randbytes(metrics_msg_size)
        message = jsonpickle.encode(metrics)
        
        # Add message to Redis stream
        xadd_start = time.time()
        redis.xadd(metrics_stream, {'message': message}, maxlen=1)
        xadd_end = time.time()
        xadd_time = float((xadd_end-xadd_start)*1000)
        
        # Print time measurement
        message_counter = message_counter + 1
        json_msg = {"idx" : message_counter, "timestamp" : time.time(), "xadd" : xadd_time, "metrics_size" : metrics_msg_size, "metrics_period" : metrics_period}
        stdout_mutex.acquire()
        print(json.dumps(json_msg))
        if verbose:
            print(f"Producer sent: {message}")
        stdout_mutex.release()

        # Metrics period in seconds
        time.sleep(metrics_period)

def recv_ctrl_msg(redis, ctrl_stream, stdout_mutex):
    # Last ID that this consumer has seen. '$' means "new messages".
    last_id = '$'

    while True:
        # Blocking read from the response stream, waits indefinitely for a new message
        xread_start = time.time()
        response = redis.xread({ctrl_stream: last_id}, count=1, block=0)
        xread_end = time.time()
        xread_time = float((xread_end-xread_start)*1000)
        
        # Print time measurement
        #json_msg = {"xread" : xread_time} #Counter index not visible
        stdout_mutex.acquire()
        print("CTRL XREAD: ", xread_time, "ms")
        #print(json.dumps(json_msg))
        stdout_mutex.release()
        
        if response:
            _, messages = response[0]
            for message_id, message_data in messages:
                last_id = message_id
                if verbose:
                    stdout_mutex.acquire()
                    print(f"Control message received: {message_data[b'message']}")
                    stdout_mutex.release()

if __name__ == "__main__":
    # Get unique ID
    prod_id = uuid.uuid4()
    print("ID: {}".format(prod_id))

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Producer MS emulator using Redis to send/receive metrics/control messages')
    parser.add_argument("--redis", type=str, help='Redis mode to use, i.e., standalone or cluster. Defaults to "cluster"', default="cluster")
    parser.add_argument('--pod_idx', type=int, help='Redis Pod Index to target. Defaults to -1, i.e., K8S ClusterIP SVC', default=-1)
    parser.add_argument("--namespace", type=str, help='Reference Kubernetes Namespace. Defaults to "default"', default="default")
    parser.add_argument('--period', type=float, help='Time interval across messages. Defaults to 1s', default=1)
    parser.add_argument('--metrics_msg_size', type=int, help='Metrics message size in bytes. Defaults to 1kB', default=1000)
    parser.add_argument('--cons_group_id', type=int, help='Consumer MS group ID for multiple producer-consumer pairing. Defaults to 0.', default=0)
    parser.add_argument('--prod_group_id', type=int, help='Producer MS group ID for multiple producer-consumer pairing. Defaults to 0.', default=0)
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    args = parser.parse_args()

    global redis_kind; global pod_idx; global namespace; global period; global metrics_msg_size; global cons_group_id; global prod_group_id; global verbose
    redis_kind = args.redis; pod_idx = args.pod_idx; namespace = args.namespace; period = args.period; metrics_msg_size = args.metrics_msg_size; cons_group_id = args.cons_group_id; prod_group_id = args.prod_group_id; verbose = args.verbose

    # Register signal handler for termination signals
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Connect to Redis/Redis Cluster
    redis = redisClient(redis_kind, pod_idx)
    redis.config_set("activedefrag", "yes") #Enable active defragmentation

    # Setting streams
    metrics_stream = formatMetricsKey(cons_group_id, prod_group_id)
    ctrl_stream = formatControlKey(cons_group_id, prod_group_id)

    # Initialize Metrics Stream
    redis.delete(metrics_stream)

    # Run Control thread
    stdout_mutex = multiprocessing.Lock()
    ctrl_process = multiprocessing.Process(target=recv_ctrl_msg, args=(redis,ctrl_stream,stdout_mutex,), daemon=True)
    ctrl_process.start()

    # Main producer loop
    send_metrics_msg(redis, metrics_stream, metrics_msg_size, period, stdout_mutex)