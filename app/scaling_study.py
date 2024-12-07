import requests
import time
import pandas as pd
import logging
import os
import matplotlib.pyplot as plt
import seaborn as sns
import utils
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Base URL of the FastAPI service
BASE_URL = "http://127.0.0.1:8000/"


def get_results_dir(mode: str) -> str:
    dir_name = f"scaling_results/{mode}"
    dir_path = os.path.join(os.path.dirname(__file__), dir_name)
    os.makedirs(dir_path, exist_ok=True)
    return dir_path


def register_function(name: str, func) -> str:
    payload = utils.serialize(func)
    data = {
        "name": name,
        "payload": payload
    }
    try:
        response = requests.post(f"{BASE_URL}register_function/", json=data)
        if response.status_code in [200, 201]:
            function_id = response.json().get("function_id")
            logging.info(
                f"Registered function '{name}' with ID: {function_id}")
            return function_id
        else:
            logging.error(
                f"Failed to register function '{name}': {response.text}")
            return None
    except Exception as e:
        logging.error(f"Exception during function registration: {e}")
        return None


def execute_function(function_id: str, args: tuple, kwargs: dict) -> str:
    payload = utils.serialize((args, kwargs))
    data = {
        "function_id": function_id,
        "payload": payload
    }
    try:
        response = requests.post(f"{BASE_URL}execute_function/", json=data)
        if response.status_code in [200, 201]:
            task_id = response.json().get("task_id")
            logging.info(
                f"Executed function ID {function_id}, Task ID: {task_id}")
            return task_id
        else:
            logging.error(
                f"Failed to execute function ID {function_id}: {response.text}")
            return None
    except Exception as e:
        logging.error(f"Exception during function execution: {e}")
        return None


def get_task_result(task_id: str):
    try:
        response = requests.get(f"{BASE_URL}result/{task_id}")
        if response.status_code == 200:
            data = response.json()
            status = data.get("status")
            result = data.get("result")
            return status, result
        elif response.status_code == 404:
            logging.warning(f"Task {task_id} not found or not complete yet.")
            return None, None
        else:
            logging.error(
                f"Failed to get result for Task {task_id}: {response.text}")
            return None, None
    except Exception as e:
        logging.error(f"Exception during result retrieval: {e}")
        return None, None


def run_scaling_study(task_type='no_op', mode='push', tasks_per_worker=5, delay_between_tasks=0.05):
    """
    Run a weak scaling study.
    Args:
        task_type (str): Type of task to execute ('no_op' or 'fibonacci').
        mode (str): Dispatcher mode ('push', 'pull', or 'local').
        tasks_per_worker (int): Number of tasks per worker.
        delay_between_tasks (float): Delay in seconds between launching tasks.
    """
    # Define task functions based on task_type
    if task_type == 'no_op':
        def no_op(x):
            return x
        func = no_op
    elif task_type == 'fibonacci':
        def fibonacci(n):
            if n <= 1:
                return n
            return fibonacci(n - 1) + fibonacci(n - 2)
        func = fibonacci
    else:
        logging.error("Invalid task type specified.")
        return

    # Register the function
    function_id = register_function(task_type, func)
    if not function_id:
        logging.error("Function registration failed. Aborting scaling study.")
        return

    # Prepare to collect results
    results = []

    # Define logical worker counts to scale load
    worker_counts = [1, 2, 4, 8]

    # Iterate over different worker counts
    for num_workers in worker_counts:
        num_tasks = num_workers * tasks_per_worker
        logging.info(
            f"Starting scaling study with {num_workers} workers and {num_tasks} tasks.")

        # Execute tasks
        task_ids = []
        start_time = time.time()
        for _ in range(num_tasks):
            task_id = execute_function(function_id, args=(3,), kwargs={})
            if task_id:
                task_ids.append(task_id)
            time.sleep(delay_between_tasks)

        # Poll for task completion
        completed_tasks = 0
        polling_start = time.time()
        polling_timeout = 120
        polling_interval = 1

        task_statuses = {}

        while completed_tasks < num_tasks and (time.time() - polling_start) < polling_timeout:
            for task_id in task_ids:
                if task_id in task_statuses:
                    continue
                status, result = get_task_result(task_id)
                if status in ["COMPLETE", "FAILED"]:
                    task_statuses[task_id] = (status, result)
                    completed_tasks += 1
            time.sleep(polling_interval)

        end_time = time.time()
        elapsed_time = end_time - start_time

        # Calculate latency and throughput metrics
        throughput = num_tasks / elapsed_time if elapsed_time > 0 else 0
        average_latency = elapsed_time / num_tasks if num_tasks > 0 else 0

        logging.info(
            f"Completed {completed_tasks}/{num_tasks} tasks in {elapsed_time:.2f} seconds.")
        logging.info(f"Throughput: {throughput:.2f} tasks/second.")
        logging.info(f"Average Latency: {average_latency:.4f} seconds/task.")

        # Append results
        results.append({
            "mode": mode,
            "task_type": task_type,
            "num_workers": num_workers,
            "num_tasks": num_tasks,
            "elapsed_time_sec": elapsed_time,
            "throughput_tasks_per_sec": throughput,
            "average_latency_sec_per_task": average_latency
        })

    # Save and plot results
    df = pd.DataFrame(results)
    results_dir = get_results_dir(mode)

    csv_filename = os.path.join(results_dir, f"{task_type}_scaling_study.csv")
    df.to_csv(csv_filename, index=False)

    plot_results(df, task_type, mode)


def plot_results(df: pd.DataFrame, task_type: str, mode: str):
    """
    Generate and save plots for throughput and latency.
    Args:
        df (pd.DataFrame): DataFrame containing scaling study results.
        task_type (str): Type of task ('no_op' or 'fibonacci').
        mode (str): Dispatcher mode ('push', 'pull', or 'local').
    """
    sns.set(style="whitegrid")

    # Throughput vs Number of Workers
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x='num_workers',
                 y='throughput_tasks_per_sec', marker='o')
    plt.title(
        f'Throughput vs Number of Workers ({task_type.capitalize()} Tasks - {mode.capitalize()} Mode)')
    plt.xlabel('Number of Workers')
    plt.ylabel('Throughput (tasks/sec)')
    plt.xticks(df['num_workers'])
    throughput_plot_path = os.path.join(get_results_dir(
        mode), f"{task_type}_throughput_{mode}.png")
    plt.savefig(throughput_plot_path)
    plt.close()

    # Average Latency vs Number of Workers
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x='num_workers',
                 y='average_latency_sec_per_task', marker='o', color='orange')
    plt.title(
        f'Average Latency vs Number of Workers ({task_type.capitalize()} Tasks - {mode.capitalize()} Mode)')
    plt.xlabel('Number of Workers')
    plt.ylabel('Average Latency (sec/task)')
    plt.xticks(df['num_workers'])
    latency_plot_path = os.path.join(get_results_dir(
        mode), f"{task_type}_latency_{mode}.png")
    plt.savefig(latency_plot_path)
    plt.close()


def main():
    # Parse command-line arguments for dispatcher mode
    parser = argparse.ArgumentParser(description="Scaling Study Client")
    parser.add_argument('--mode', type=str, choices=['push', 'pull', 'local'], required=True,
                        help="Dispatcher mode: push, pull, or local")
    args = parser.parse_args()

    # Run scaling study for various task types
    run_scaling_study(task_type='no_op',
                      mode=args.mode,
                      tasks_per_worker=5,
                      delay_between_tasks=0.05)

    run_scaling_study(task_type='fibonacci',
                      mode=args.mode,
                      tasks_per_worker=5,
                      delay_between_tasks=0.05)


if __name__ == "__main__":
    main()
