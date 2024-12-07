"""
Usage: call from main project directory with:
python3 -m app.push_worker 2 tcp://localhost:5555
"""
import zmq
import sys
from app import utils
from multiprocessing import Process
from pathos.multiprocessing import ProcessingPool as Pool
import argparse
import time
import uuid


def parse_args():
    parser = argparse.ArgumentParser(description='Push Worker for MPCSFaaS')
    parser.add_argument('num_worker_processors', type=int,
                        help='Number of worker processors')
    parser.add_argument('dispatcher_url', type=str,
                        help='Dispatcher URL (e.g., tcp://localhost:5555)')
    return parser.parse_args()


def worker_function(dispatcher_url):
    """
    Function that each worker process will run.

    Args:
        dispatcher_url (str): The URL of the dispatcher to connect to.
        worker_number (int): Unique number identifying the worker.
    """
    context = zmq.Context()
    dealer_socket = context.socket(zmq.DEALER)

    # Generate a unique identity for this worker
    worker_id = f"Worker-{str(uuid.uuid4())}"
    dealer_socket.setsockopt_string(zmq.IDENTITY, worker_id)
    dealer_socket.connect(dispatcher_url)

    # Send registration message
    registration_message = {
        'type': 'REGISTER',
    }
    print(f"{worker_id} online and ready to do your bidding.")
    dealer_socket.send_multipart(
        [b'', utils.serialize(registration_message).encode('utf-8')]
    )

    # Initialize a Pool for executing tasks
    pool = Pool(processes=1)

    while True:
        try:
            # Wait for a message from dispatcher
            message = dealer_socket.recv_multipart()
            # message[0] is empty frame / message[1] is the payload
            task_message = message[1]
            task_message_str = task_message.decode('utf-8')
            task_data = utils.deserialize(task_message_str)

            if task_data['type'] == 'TASK':
                task_id = task_data['task_id']
                fn_payload_serialized = task_data['fn_payload']
                args = task_data['args']
                kwargs = task_data['kwargs']

                # Deserialize fn_payload
                fn_payload = utils.deserialize(fn_payload_serialized)

                param_payload = (args, kwargs)
                func_status, result_payload = utils.execute_task(
                    pool, fn_payload, param_payload
                )

                # Send result back to dispatcher
                result_message = {
                    'type': 'RESULT',
                    'task_id': task_id,
                    'status': func_status,
                    'result': result_payload
                }
                dealer_socket.send_multipart(
                    [b'', utils.serialize(result_message).encode('utf-8')]
                )
                print(
                    f"{worker_id} completed task {task_id} with status {func_status}")

            elif task_data['type'] == 'HEARTBEAT':
                # Respond to heartbeat
                heartbeat_response = {
                    'type': 'HEARTBEAT',
                }
                dealer_socket.send_multipart(
                    [b'', utils.serialize(heartbeat_response).encode('utf-8')]
                )
                print(
                    f"{worker_id} sent HEARTBEAT response to dispatcher")

        except Exception as e:
            print(f"Error in {worker_id}: {e}")
            # Send error message back to dispatcher
            error_message = {
                'type': 'ERROR',
                'error': repr(e)
            }
            dealer_socket.send_multipart(
                [b'', utils.serialize(error_message).encode('utf-8')]
            )
            continue


def main():
    args = parse_args()
    n_processors = args.num_worker_processors
    dispatcher_url = args.dispatcher_url
    processes = []
    for processor in range(n_processors):
        p = Process(target=worker_function, args=(dispatcher_url,))
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
