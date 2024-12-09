import zmq
import redis
from app import utils
import time
from app.main import TASKS_CHANNEL, REDIS_PORT, REDIS_HOST, REDIS_DB_INDEX
from pathos.multiprocessing import ProcessingPool as Pool
import json
import argparse

# dispatcher args
DISPATCHER_IP = '127.0.0.1'
# heartbeat/fault tolerant config
HEARTBEAT_INTERVAL = 1.0
HEARTBEAT_LIVENESS = 3
DEADLINE_TIMEOUT = 10

# Connect to Redis DB
r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB_INDEX,
    decode_responses=True)

def parse_args():
    parser = argparse.ArgumentParser(description='MPCSFaaS Task Dispatcher')
    parser.add_argument("-mode", required=True, type=str,
                        choices=['local', 'pull', 'push'], help="[local/pull/push]")
    parser.add_argument("-p", "--port", type=int,
                        help="port to launch task dispatcher on")
    parser.add_argument("-w", "--work", type=int,
                        help="port to launch task dispatcher on")
    args = parser.parse_args()
    # trap port not specified error for modes [push/pull]
    if args.mode in ['pull', 'push'] and args.port is None:
        parser.error(
            "The --port argument is required for 'pull' and 'push' modes.")
    return args

def write_to_redis(r, task_id, func_status, result_payload):
    """Updates task status and result in Redis."""
    r.hset(task_id, mapping={'status': func_status})
    r.hset(task_id, mapping={'result': utils.serialize(result_payload)})

def assign_task_to_worker(task_id, worker_id, router_socket, worker_type):
    """
    Assigns a task to a worker and updates task and worker states.

    1. Updates worker status in Redis
    2. Assigns a task to a worker via ZMQ message protocol

    Args:
        task_id (str): The unique identifier of the task to be assigned.
        worker_id (str): The unique identifier of the worker to which the task is assigned.
        router_socket (zmq.Socket): A ZeroMQ ROUTER socket used to send task data to the worker.
        worker_type (str): Type of worker (for messaging protocol)

    Returns:
        None
    """
    # get func and args from Redis
    func_payload_serialized = r.hget(task_id, "fn_payload")
    func_parameters_serialized = r.hget(task_id, "param_payload")
    func_parameters = utils.deserialize(func_parameters_serialized)
    args, kwargs = func_parameters
    # Update task status to RUNNING
    r.hset(task_id, mapping={'status': 'RUNNING'})

    # compose ZMQ msg
    task_data = {
        'type': 'TASK',
        'task_id': task_id,
        'fn_payload': func_payload_serialized,
        'args': args,
        'kwargs': kwargs
    }
    # dispatcher-mode specific comms channels
    if worker_type == 'PUSH_WORKER':
        router_socket.send_multipart(
            [worker_id, b'', utils.serialize(task_data).encode('utf-8')])
    elif worker_type == 'PULL_WORKER':
        serialized_data = json.dumps(task_data)
        router_socket.send_string(serialized_data)
    print(f"Sent task {task_id} to worker {worker_id}")

def send_empty_task_queue_msg(router_socket, worker_id):
    """
    Sends a message to a PULL worker indicating that there are no tasks in the queue.

    This function composes a message of type 'NO_TASKS', targeted at the specified worker, 
    serializes it into a JSON string, and sends it via the provided ZeroMQ router socket.

    Args:
        router_socket (zmq.Socket): The ZeroMQ router socket used to send the message.
        worker_id (str): The unique identifier of the worker for whom the message is intended.
    """
    empty_queue_msg = {'type': 'NO_TASKS', 'for_worker': worker_id}
    serialized_data = json.dumps(empty_queue_msg)
    router_socket.send_string(serialized_data)

def update_worker_state(worker_states, worker_id, task_id=None, new_state='IDLE'):
    """
    Updates specified worker metadata dictionary with tasks and/or state 

    Initializes the worker if it doesn't yet exist 

    Args:
        worker_states (dict): A dictionary maintaining the state of all workers, 
                              where each key is a worker ID and the value is a dictionary 
                              containing metadata such as the worker's status, assigned task, 
                              and heartbeat information.
        worker_id (str): The unique identifier of the worker to which the task is assigned.
        task_id (str): The unique identifier of the task being assigned to the worker.
        new_state (str, optional): The new state to assign to the worker. Defaults to 'IDLE'.

    Returns:
        None
    """
    # protect against KeyErrors
    if worker_id not in worker_states:
        worker_states[worker_id] = {
            "status": "new_state", "task_id": task_id, "missed_heartbeats": 0, 'deadline': None}
    else:
        worker_states[worker_id]['status'] = new_state
        worker_states[worker_id]['task_id'] = task_id
        worker_states[worker_id]['missed_heartbeats'] = 0

def get_next_queued_task():
    """
    Searches Redis for the first task with the status 'QUEUED'.

    This function scans all keys in the Redis database, looking for a task whose 
    'status' field is set to 'QUEUED'. If a queued task is found, its key is returned.

    Returns:
        str: The key of the first task found with the status 'QUEUED'.
        None: If no tasks with the status 'QUEUED' are found
    """
    for key in r.scan_iter():
        task_status = r.hget(key, 'status')
        if task_status == 'QUEUED':
            return key
    return None

def process_worker_result(worker_id, worker_data, worker_states):
    """
    Processes the results of a worker's compute task and updates its metadata.

    This function performs the following actions:
    1. Writes the task results, status, and payload to the Redis database.
    2. Logs a message indicating the outcome of the task (success or failure).
    3. Updates the worker's metadata in the `worker_states` dictionary to mark the worker 
    as 'IDLE', clear its task assignment, and reset the missed heartbeats counter.

    Args:
        worker_id (str): The unique identifier of the worker that completed the task.
        worker_data (dict): A dictionary containing the worker's task results, including:
            - `task_id` (str): The unique identifier of the completed task.
            - `status` (str): The status of the task ('COMPLETE' or 'FAILED').
            - `result` (any): The result or exception data associated with the task.
        worker_states (dict): A dictionary tracking the state of all workers, including their status, 
                              task assignments, and heartbeat information.

    Returns:
        None
    """
    task_id = worker_data['task_id']
    func_status = worker_data['status']
    result_payload = worker_data['result']

    write_to_redis(r, task_id, func_status, result_payload)

    if func_status == 'COMPLETE':
        print(
            f"Task {task_id} completed successfully with result: {result_payload}")
    elif func_status == 'FAILED':
        print(f"Task {task_id} failed with exception: {result_payload}")

    # Mark worker as IDLE and reset task_id
    worker_states[worker_id]['status'] = 'IDLE'
    worker_states[worker_id]['task_id'] = None
    worker_states[worker_id]['missed_heartbeats'] = 0

def handle_worker_error(worker_id, worker_data, worker_states):
    """
    Handles an error reported by a worker and updates its state accordingly.

    Args:
        worker_id (str): The unique identifier of the worker that reported the error.
        worker_data (dict): A dictionary containing data about the worker, including the error message.
        worker_states (dict): A dictionary tracking the state of all workers, including their status, 
                              task assignments, and heartbeat information.

    Returns:
        None
    """
    print(f"Worker {worker_id} reported error: {worker_data['error']}")
    # Mark worker as FAILED and reset task_id
    worker_states[worker_id]['status'] = 'FAILED'
    worker_states[worker_id]['task_id'] = None
    worker_states[worker_id]['missed_heartbeats'] = 0

def run_dispatcher_local():
    # Create a pubsub object
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(TASKS_CHANNEL)
    print('Listening for incoming tasks....')
    while True:
        with Pool() as pool:
            task_message = pubsub.get_message()
            if task_message:
                new_task_id = task_message['data']
                # get func, args & kwargs from Redis
                func_payload = utils.deserialize(
                    r.hget(new_task_id, "fn_payload"))
                func_parameters = utils.deserialize(
                    r.hget(new_task_id, "param_payload"))
                (func_status, result_payload) = utils.execute_task(
                    pool, func_payload, func_parameters)
                if func_status == 'COMPLETE':
                    write_to_redis(
                        r, new_task_id, func_status, result_payload)
                    print(
                        f'task ran succeffully with result: {result_payload}')

def run_dispatcher_push_mode(DISPATCHER_MODE, DISPATCHER_PORT):
    # Set up ZMQ context and sockets in ROUTER pattern to distribute tasks
    context = zmq.Context()
    router_socket = context.socket(zmq.ROUTER)
    router_socket.bind(f'tcp://*:{DISPATCHER_PORT}')

    # Subscribe to Redis tasks channel
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(TASKS_CHANNEL)
    print('Listening for incoming tasks....')
    print("[PUSH Task Dispatcher Launched]")
    print("Listening for tasks to come across the Redis [TASKS_CHANNEL]....")
    print("Listening for PUSH workers to come online....")

    # Track metadata on workers as they come online, receive tasks, crash, etc.
    worker_states = {}

    poller = zmq.Poller()
    poller.register(router_socket, zmq.POLLIN)

    next_heartbeat_time = time.time() + HEARTBEAT_INTERVAL

    while True:
        # Calculate timeout for polling
        timeout = max(0, next_heartbeat_time - time.time()) * \
            1000  # milliseconds
        sockets = dict(poller.poll(timeout=timeout))

        # Check for worker messages
        if router_socket in sockets:
            message = router_socket.recv_multipart()
            worker_id = message[0]
            empty_frame = message[1]
            worker_message = message[2]

            worker_message_str = worker_message.decode('utf-8')
            worker_data = utils.deserialize(worker_message_str)

            if worker_data['type'] == 'REGISTER':
                worker_states[worker_id] = {
                    'status': 'IDLE',
                    'missed_heartbeats': 0,
                    'task_id': None,
                    'deadline': None    # not actually used for push workers
                }
                print(f"Worker {worker_id} registered.")

            elif worker_data['type'] == 'HEARTBEAT':
                # Heartbeat response from worker
                if worker_id in worker_states:
                    worker_states[worker_id]['missed_heartbeats'] = 0
                    print(f"Received HEARTBEAT from worker {worker_id}")
                else:
                    print(
                        f"Received HEARTBEAT from unregistered worker {worker_id}")

            elif worker_data['type'] == 'RESULT':
                process_worker_result(worker_id, worker_data, worker_states)

            elif worker_data['type'] == 'ERROR':
                handle_worker_error(worker_id, worker_data, worker_states)

            # After processing, check if worker is IDLE and assign new task if available
            if worker_states.get(worker_id, {}).get('status') == 'IDLE':
                queued_task_id = get_next_queued_task()
                if queued_task_id:
                    assign_task_to_worker(
                        queued_task_id,
                        worker_id,
                        router_socket,
                        worker_type=DISPATCHER_MODE)
                    update_worker_state(
                        worker_states,
                        worker_id,
                        queued_task_id,
                        new_state='WORKING')
                else:
                    print("No QUEUED tasks available.")

        # Check for new tasks through Redis channel
        task_message = pubsub.get_message()
        if task_message:
            new_task_id = task_message['data']
            print(f"Received new task {new_task_id} from Redis")
            # Try to assign the task if there's an idle worker
            assigned = False
            for worker_id, worker_info in worker_states.items():
                if worker_info['status'] == 'IDLE':
                    assign_task_to_worker(
                        new_task_id,
                        worker_id,
                        router_socket,
                        worker_type=DISPATCHER_MODE)
                    assigned = True
                    update_worker_state(
                        worker_states,
                        worker_id,
                        new_task_id,
                        new_state='WORKING')
                    break
            # Task remains with status 'QUEUED' in Redis
            if not assigned:
                print(
                    f"No idle worker_states available for task {new_task_id}")

        # Assign tasks to any remaining idle workers
        for worker_id, worker_info in worker_states.items():
            if worker_info['status'] == 'IDLE':
                queued_task_id = get_next_queued_task()
                if queued_task_id:
                    assign_task_to_worker(
                        queued_task_id,
                        worker_id,
                        router_socket,
                        worker_type=DISPATCHER_MODE)
                    update_worker_state(
                        worker_states,
                        worker_id,
                        queued_task_id,
                        new_state='WORKING')
                else:
                    break

        # Sending and handling heartbeats
        current_time = time.time()
        if current_time >= next_heartbeat_time:
            for worker_id, worker_info in list(worker_states.items()):
                if worker_info['status'] == 'WORKING':
                    # Send heartbeat
                    heartbeat_message = {
                        'type': 'HEARTBEAT',
                    }
                    router_socket.send_multipart(
                        [worker_id, b'', utils.serialize(
                            heartbeat_message).encode('utf-8')]
                    )
                    print(f"Sent HEARTBEAT to worker {worker_id}")

                    # Increment missed heartbeats
                    worker_states[worker_id]['missed_heartbeats'] += 1

                    # Check if worker has missed too many heartbeats
                    if worker_states[worker_id]['missed_heartbeats'] >= HEARTBEAT_LIVENESS:
                        # Worker is considered dead
                        task_id = worker_states[worker_id]['task_id']
                        print(
                            f"Worker {worker_id} failed (missed heartbeats). Reassigning task {task_id}.")

                        # Update task status back to QUEUED
                        r.hset(task_id, mapping={'status': 'QUEUED'})

                        # Mark worker as FAILED instead of removing
                        worker_states[worker_id]['status'] = 'FAILED'
                        worker_states[worker_id]['task_id'] = None
                        print(f"Worker {worker_id} marked as FAILED.")

                else:
                    # For IDLE workers, reset missed_heartbeats
                    worker_states[worker_id]['missed_heartbeats'] = 0

            # Schedule next heartbeat
            next_heartbeat_time = current_time + HEARTBEAT_INTERVAL

def run_dispatcher_pull_mode(DISPATCHER_MODE, DISPATCHER_PORT):
    """
    Run the task dispatcher in PULL mode to manage worker-task interactions.

    This dispatcher listens for incoming messages from PULL workers, assigns tasks from a Redis-backed task queue,
    and tracks worker state and task deadlines. It processes three types of messages from workers:
    
    -`REQUEST_TASK`: When a worker requests a task, the dispatcher retrieves the next queued task from Redis,
    assigns it to the requesting worker, and updates worker states. If no tasks are available, it sends an
    "empty queue" message to the worker.
    
    -`RESULT`: When a worker submits results for a completed task, the dispatcher processes the result,
    updates task metadata in Redis, and assigns a new task to the worker if available.
    
    -`ERROR`: When a worker reports an error, the dispatcher logs the error and marks the worker's state as `FAILED`.

    Features:
    1. Tracks and manages worker metadata (e.g., status, task assignment, deadlines).
    2. Handles task requeueing in Redis for tasks that exceed their deadlines or fail.
    3. Maintains a ZeroMQ `REP` socket to communicate with workers.
    4. Ensures robust handling of edge cases, such as task queue exhaustion or worker errors.

    Args:
        DISPATCHER_MODE (str): The mode of the dispatcher (e.g., `PULL_WORKER`), used to determine worker communication protocols.
        DISPATCHER_PORT (int): The port on which the dispatcher listens for worker messages.
    """
    # Track metadata on workers as they come online, receive tasks, crash, etc.
    worker_states = {}
    # create container for ZeroMQ socket
    context = zmq.Context()
    # launch ZMQ socket in REPLY mode to send connex confirmed msgs to PULL workers
    dispatcher_socket = utils.create_zmq_socket(context,
                                                socket_type=zmq.REP,
                                                address_string=f"tcp://{DISPATCHER_IP}:{DISPATCHER_PORT}")
    print("[PULL Task Dispatcher Launched]")
    print("Listening for PULL workers to come online....")
    # bring the task_dispatcher online to listen for PULL workers
    while True:
        # deconstruct msg from PULL worker
        message = dispatcher_socket.recv().decode('utf-8')
        message_payload = json.loads(message)
        sender = message_payload['worker_id']

        # console announce when a new worker has come online
        if sender not in worker_states.keys():
            print(f'[{sender}] came online')

        # Scan thru existing workers, checking for timedout tasks
        current_time = time.time()
        for worker_id, state in list(worker_states.items()):
            if state['status'] == 'WORKING' and state.get('deadline') and current_time > state['deadline']:
                print(f"Task {state['task_id']} failed: deadline exceeded.")
                # requeue the task in Redis
                r.hset(worker_states[worker_id]['task_id'],
                       mapping={'status': 'QUEUED'})
                # update worker states
                worker_states[worker_id]['status'] = 'FAILED'
                worker_states[worker_id]['deadline'] = None

        # new PULL worker has come online OR existing worker has completed task
        if message_payload['type'] == 'REQUEST_TASK':
            queued_task_id = get_next_queued_task()
            # dispatch a task if theres one in the Redis Queue
            if queued_task_id:
                assign_task_to_worker(
                    queued_task_id,
                    sender,
                    dispatcher_socket,
                    worker_type=DISPATCHER_MODE)
                update_worker_state(
                    worker_states,
                    sender,
                    queued_task_id,
                    new_state='WORKING')
                # stamp worker with expected deadline of new task
                worker_states[sender]['deadline'] = current_time + \
                    DEADLINE_TIMEOUT
            # Redis Task Queue is empty - send message
            else:
                update_worker_state(
                    worker_states,
                    sender,
                    queued_task_id)
                send_empty_task_queue_msg(dispatcher_socket, sender)

        elif message_payload['type'] == 'RESULT':
            process_worker_result(
                sender,
                message_payload,
                worker_states)
            # Immediately try to dispatch a new task to the finished WORKER
            queued_task_id = get_next_queued_task()
            if queued_task_id:
                assign_task_to_worker(
                    queued_task_id,
                    sender,
                    dispatcher_socket,
                    worker_type=DISPATCHER_MODE)
                update_worker_state(
                    worker_states,
                    sender,
                    queued_task_id,
                    new_state='WORKING')
                worker_states[sender]['deadline'] = current_time + \
                    DEADLINE_TIMEOUT
            # Redis Task Queue is empty - send message
            else:
                update_worker_state(
                    worker_states,
                    sender,
                    queued_task_id)
                send_empty_task_queue_msg(dispatcher_socket, sender)

        elif message_payload['type'] == 'ERROR':
            print(f"{sender} reported error: {message_payload['error']}")
            update_worker_state(
                worker_states,
                sender,
                new_state='FAILED')
            dispatcher_socket.send_string(
                f"Dispatcher received error from {sender} and decommissioned it")

if __name__ == "__main__":
    args = parse_args()
    if args.mode == 'local':
        run_dispatcher_local()
    elif args.mode == 'pull':
        run_dispatcher_pull_mode('PULL_WORKER', DISPATCHER_PORT=args.port)
    elif args.mode == 'push':
        run_dispatcher_push_mode('PUSH_WORKER', DISPATCHER_PORT=args.port)
