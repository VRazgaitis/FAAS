"""
Usage: call from main project directory with:
python3 -m app.pull_worker 2 tcp://localhost:5555
"""
import zmq
from app import utils
from multiprocessing import Process
from pathos.multiprocessing import ProcessingPool as Pool
import argparse
import uuid
import json
import time

def parse_args():
    """
    Parse CLI arguments when launching the worker

    Returns:
        argparse.Namespace: A namespace object containing the parsed arguments:
            - num_worker_processors (int): Number of worker processors to spawn.
            - dispatcher_url (str): The URL of the task dispatcher (e.g., tcp://localhost:5555).
    """
    parser = argparse.ArgumentParser(description='Pull Worker for MPCSFaaS')
    parser.add_argument('worker_processors', type=int,
                        help='Number of worker processors')
    parser.add_argument('dispatcher_url', type=str,
                        help='Dispatcher URL (e.g., tcp://localhost:5555)')
    return parser.parse_args()

def launch_pull_worker(dispatcher_url):
    """
    Launch a ZeroMQ pull worker that connects to the task dispatcher and broadcasts its
    name and constatly requests tasks when not actively computing

    - Creates a ZeroMQ socket in `zmq.REQ` mode.
    - Generates a unique worker name using UUID.
    - Connects to the dispatcher using the provided IP and port.
    - Sends a REQUEST_TASK message to the dispatcher with the worker's UUID.
    - Prints the connection status and readiness to fetch tasks.
    
    Returns:
        None
    """
    # create container for ZeroMQ socket 
    context = zmq.Context()
    # launch ZMQ socket in REQ mode, connecting to the task_dispatcher
    socket = utils.create_zmq_socket(context, 
                                socket_type=zmq.REQ, 
                                address_string=dispatcher_url)
    worker_id = f"Worker-{str(uuid.uuid4())}"
    # Worker requests a task immediately upon coming online
    task_request = {
        'type': 'REQUEST_TASK',
        'worker_id': worker_id
    }
    # New worker status console logs
    print(f"[PULL Worker: {worker_id} Launched]")
    print(f"[IP/Port]: {dispatcher_url}")
    print(f"{worker_id} registered successfully. Fetching tasks from dispatcher....")
    # Initialize a Pool for executing tasks
    pool = Pool(processes=1)
    # launch always running hungry pull worker
    while True:
        try: 
            # Send a task request to the dispatcher
            serialized_message = json.dumps(task_request).encode('utf-8')
            socket.send(serialized_message)
            server_reply = socket.recv_string()
            message_payload = json.loads(server_reply)
            
            if message_payload['type'] == 'TASK':
                # unpack the task
                task_id = message_payload['task_id']
                fn_payload_serialized = message_payload['fn_payload']
                args = message_payload['args']
                kwargs = message_payload['kwargs']
                fn_payload = utils.deserialize(fn_payload_serialized)
                param_payload = (args, kwargs)
                
                # run the task
                func_status, result_payload = utils.execute_task(
                    pool, 
                    fn_payload, 
                    param_payload
                )

                # Send result back to dispatcher
                result_message = {
                    'type': 'RESULT',
                    'task_id': task_id,
                    'status': func_status,
                    'result': result_payload,
                    'worker_id': worker_id
                }
                # Send results back to the dispatcher
                serialized_message = json.dumps(result_message).encode('utf-8')
                socket.send(serialized_message)
                server_reply = socket.recv_string()
                
            elif message_payload['type'] == 'NO_TASKS':
                print(f"{worker_id}: No tasks available. Retrying...")
                time.sleep(0.25)
                
        except Exception as e:
            print(f"Error in {worker_id}: {e}")
            # Send error message back to dispatcher
            error_message = {
                'type': 'ERROR',
                'error': repr(e)
            }
            serialized_message = json.dumps(error_message).encode('utf-8')
            socket.send(serialized_message)
            socket.recv_string()
        
def main():
    args = parse_args()
    n_processors = args.worker_processors
    dispatcher_url = args.dispatcher_url
    processes = []
    for processor in range(n_processors):
        p = Process(target=launch_pull_worker, args=(dispatcher_url,))
        p.start()
        processes.append(p)
        print(f"Started {p.name} with PID {p.pid}")

    try:
        # Main process alive to monitor worker processes
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down workers...")
        for p in processes:
            p.terminate()
            p.join()
        print("All workers have been terminated.")

if __name__ == "__main__":
    main()