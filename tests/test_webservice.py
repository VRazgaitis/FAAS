import requests
from .serialize import serialize, deserialize
import logging
import time
import random
import uuid

base_url = "http://127.0.0.1:8000/"
valid_statuses = ["QUEUED", "RUNNING", "COMPLETED", "FAILED"]

def double(x):
    return x * 2

def test_fn_registration_invalid():
    # Using a non-serialized payload data
    resp = requests.post(base_url + "register_function",
                         json={"name": "hello",
                               "payload": "payload"})

    assert resp.status_code in [500, 400]
    
def test_register_function_missing_payload():
    """
    Try registering a function without a payload.
    """
    resp = requests.post(base_url + "register_function",
                         json={"name": "missing_payload"})
    print(resp.text)
    assert resp.status_code == 422  # Missing payload should result in a client error

def test_execute_with_invalid_function_id():
    """
    Attempt to execute a function with an invalid or non-existing function_id.
    """
    invalid_function_id = str(uuid.uuid4())  # Generate a random UUID
    payload = serialize(((5,), {}))
    resp = requests.post(base_url + "execute_function",
                         json={"function_id": invalid_function_id,
                               "payload": payload})
    print(resp.text)
    assert resp.status_code == 400  # No such function exists
        
def test_fn_registration():
    # Using a real serialized function
    serialized_fn = serialize(double)
    resp = requests.post(base_url + "register_function",
                         json={"name": "double",
                               "payload": serialized_fn})

    assert resp.status_code in [200, 201]
    assert "function_id" in resp.json()

def test_execute_fn():
    resp = requests.post(base_url + "register_function",
                         json={"name": "hello",
                               "payload": serialize(double)})
    fn_info = resp.json()
    assert "function_id" in fn_info

    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],
                               "payload": serialize(((2,), {}))})

    print(resp)
    assert resp.status_code == 200 or resp.status_code == 201
    assert "task_id" in resp.json()

    task_id = resp.json()["task_id"]

    resp = requests.get(f"{base_url}status/{task_id}")
    print(resp.json())
    assert resp.status_code == 200
    assert resp.json()["task_id"] == task_id
    assert resp.json()["status"] in valid_statuses

def test_roundtrip():
    resp = requests.post(base_url + "register_function",
                         json={"name": "double",
                               "payload": serialize(double)})
    fn_info = resp.json()

    number = random.randint(0, 10000)
    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],
                               "payload": serialize(((number,), {}))})

    assert resp.status_code in [200, 201]
    assert "task_id" in resp.json()

    task_id = resp.json()["task_id"]

    for i in range(20):
        time.sleep(.75) # needed to add this so that fn has time to complete
        resp = requests.get(f"{base_url}result/{task_id}")
        print(resp.text)
        assert resp.status_code == 200
        assert resp.json()["task_id"] == task_id
        if resp.json()['status'] in ["COMPLETE", "FAILED"]:
            logging.warning(f"Task is now in {resp.json()['status']}")
            s_result = resp.json()
            logging.warning(s_result)
            result = deserialize(s_result['result'])
            assert result == number*2
            break
        time.sleep(0.01)
        
def test_roundtrip_no_sleep_still_running():
    """
    Send a task and then demand a result before func has had time to complete
    """
    resp = requests.post(base_url + "register_function",
                         json={"name": "double",
                               "payload": serialize(double)})
    fn_info = resp.json()

    number = random.randint(0, 10000)
    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],
                               "payload": serialize(((number,), {}))})

    assert resp.status_code in [200, 201]
    assert "task_id" in resp.json()

    task_id = resp.json()["task_id"]

    for i in range(20):
        resp = requests.get(f"{base_url}result/{task_id}")
        print(resp.text)
        assert resp.status_code == 404 # not finished compute
        
def test_bad_task_id():
    """
    Send a task and then demand a result before func has had time to complete
    """
    bad_task_id = 'a17371a2-b95f-4aff-9673-df971e78d791'
    resp = requests.get(f"{base_url}result/{bad_task_id}")
    print(resp.text)
    assert resp.status_code == 404 

def test_status_transitions():
    """
    Validate that a task goes through the correct status transitions.
    
    This one sometimes doesn't pass depending on how many workers are available
    """
    resp = requests.post(base_url + "register_function",
                         json={"name": "double",
                               "payload": serialize(double)})
    fn_info = resp.json()

    number = 10000
    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],
                               "payload": serialize(((number,), {}))})
    assert resp.status_code == 200
    task_id = resp.json()["task_id"]

    statuses_seen = set()
    for i in range(40):
        # time.sleep(0.5)
        resp = requests.get(f"{base_url}status/{task_id}")
        # print(resp.json())
        statuses_seen.add(resp.json()["status"])
        if resp.json()["status"] in ["COMPLETE", "FAILED"]:
            print(statuses_seen)
            break
        time.sleep(.01)

    assert "QUEUED" in statuses_seen
    assert "RUNNING" in statuses_seen
    assert resp.json()["status"] in ["COMPLETE", "FAILED"]
