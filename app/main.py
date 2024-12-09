from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uuid
import time
import redis
from . import utils
import logging

logging.basicConfig(level=logging.INFO)

TASKS_CHANNEL = 'Tasks'
REDIS_PORT = 6379
REDIS_HOST = 'localhost'
REDIS_DB_INDEX = 0

# Connect to Redis DB
r = redis.Redis(host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB_INDEX,
                decode_responses=True)
# Instantiate FastAPI
app = FastAPI()

class RegisterFunction(BaseModel):
    name: str
    payload: str

class RegisterFunctionReply(BaseModel):
    function_id: uuid.UUID

class ExecuteFunctionRequest(BaseModel):
    function_id: uuid.UUID
    payload: str

class ExecuteFunctionReply(BaseModel):
    task_id: uuid.UUID

class TaskStatusReply(BaseModel):
    task_id: uuid.UUID
    status: str

class TaskResultReply(BaseModel):
    task_id: uuid.UUID
    status: str
    result: str

def validate_serialized_payload(payload: str, error_message: str = "Payload not properly serialized"):
    """
    Validates that a client payload was properly serialized using the utils deserialize function

    Args:
        payload (str): The serialized payload as a hex string.
        error_message (str): Custom error message to use for the HTTPException.

    Raises:
        HTTPException: If the payload is not properly serialized.
    """
    try:
        # Attempt to deserialize the payload
        utils.deserialize(payload)
    except Exception as e:
        # Raise an HTTPException with a 400 status code if deserialization fails
        raise HTTPException(status_code=400, detail=f"{error_message}: {e}")

@app.get("/")
def greet_client():
    return {"Hello": "please give us an A :)"}

# https://fastapi.tiangolo.com/tutorial/response-model/#response_model-parameter
@app.post("/register_function/", response_model=RegisterFunctionReply)
def register_func(fn: RegisterFunction) -> RegisterFunctionReply:
    """
    Registers a function in the Redis database as a hash object. Clients must serialize the `fn_payload`
    using Python's `dill` library before including it in the request

    Args:
        fn (RegisterFunction): The function registration request containing:
            - `name` (str): The name of the function.
            - `payload` (str): A **SERIALIZED** representation of the function 
              created using Python's `dill` library.

    Returns:
        RegisterFunctionReply: A response containing the unique `function_id` (UUID) 
        assigned to the registered function.
    """
    logging.info(f"Received register_function request: {fn}")
    validate_serialized_payload(
        fn.payload, "Function payload not properly serialized")
    func_uuid = uuid.uuid4()
    # log the function in Redis as a Hash object
    # https://redis.io/docs/latest/develop/data-types/hashes/
    try:
        r.hset(
            str(func_uuid),
            mapping={
                "fn_name": fn.name,
                "fn_payload": fn.payload,
            },
        )
        logging.info(f"Function registered with UUID: {func_uuid}")
    except redis.RedisError as e:
        logging.error(f"Redis error during function registration: {e}")
        raise HTTPException(status_code=500, detail=f"Redis error: {e}")
    # https://fastapi.tiangolo.com/tutorial/response-model/#response-model-return-type
    return RegisterFunctionReply(function_id=func_uuid)

@app.post("/execute_function/", response_model=ExecuteFunctionReply)
def execute_func(fn: ExecuteFunctionRequest) -> ExecuteFunctionReply:
    """
    Executes a previously registered function with given parameters.

    The `function_id` must correspond to a valid, previously registered function.

    Clients must serialize the `param_payload` using Python's `dill` library 
    before including it in the request.

    Args:
        fn (ExecuteFunctionRequest): The execution request containing:
            - `function_id` (UUID): The unique identifier of the registered function.
            - `payload` (str): A serialized representation of the parameters 
              (e.g., args and kwargs) created using Python's `dill` library.

    Returns:
        ExecuteFunctionReply: A response containing:
            - `task_id` (UUID): A unique identifier for the execution task.

    Raises:
        HTTPException:
            - 400: If the function with the given `function_id` does not exist.
    """
    validate_serialized_payload(
        fn.payload, "Task payload not properly serialized")
    # UUID objects need to be queried against Redis as strings
    func_uuid_string = str(fn.function_id)
    # validate that provided func_uuid exists in Redis
    # https://redis-py-doc.readthedocs.io/en/master/#redis.Redis.hexists
    if not r.hexists(func_uuid_string, "fn_payload"):
        raise HTTPException(
            status_code=400,
            detail=f"No function with UUID {func_uuid_string} has been registered")
    # get fn from Redis func entry
    fn_payload_serialized = r.hget(func_uuid_string, "fn_payload")
    task_uuid = uuid.uuid4()
    # add the task to Redis
    try:
        r.hset(
            str(task_uuid),
            mapping={
                "fn_payload": fn_payload_serialized,
                "param_payload": fn.payload,
                "status": 'QUEUED',
                "result": str(None),
            },
        )
    except redis.RedisError as e:
        logging.error(f"Redis error during task execution: {e}")
        raise HTTPException(status_code=500, detail=f"Redis error: {e}")
    # publish the new task_id to the [Tasks] channel
    # https://redis-py.readthedocs.io/en/stable/advanced_features.html#publish-subscribe
    r.publish(TASKS_CHANNEL, str(task_uuid))
    return ExecuteFunctionReply(task_id=task_uuid)

@app.get("/status/{task_id}", response_model=TaskStatusReply)
def get_status(task_id: uuid.UUID) -> TaskStatusReply:
    """
    Retrieves the current status of a specific task.

    This endpoint queries Redis to fetch the status of the task associated with the provided `task_id`.

    Args:
        task_id (uuid.UUID): The unique identifier of the task whose status is being requested.

    Returns:
        TaskStatusReply: A response containing:
            - `task_id` (UUID): The unique identifier of the task.
            - `status` (str): The current status of the task (e.g., 'QUEUED', 'RUNNING', or 'COMPLETE').

    Raises:
        HTTPException:
            - 404: If the task with the given `task_id` does not exist in Redis or has no status.
            - 500: If a Redis connection error occurs or any unexpected error is encountered.
    """
    try:
        # Attempt to fetch task status from Redis
        task_status = r.hget(str(task_id), 'status')

    except redis.exceptions.ConnectionError as e:
        # Handle Redis connection errors
        raise HTTPException(
            status_code=500,
            detail=f"Redis connection error: {str(e)}")

    except Exception as e:
        # Catch unexpected exceptions
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}")
        
    # Check if the status exists
    if task_status is None:
        raise HTTPException(
            status_code=404,
            detail=f"Task with UUID {task_id} does not exist in Redis or has no status.")

    # Return the task status
    return TaskStatusReply(task_id=task_id, status=task_status)

@app.get("/result/{task_id}", response_model=TaskResultReply)
def get_result(task_id: uuid.UUID) -> TaskResultReply:
    """
    Retrieves the final result of a specific task, if it has completed.

    This endpoint queries Redis to fetch the status and result of the task associated 
    with the provided `task_id`. The task must have a status of 'COMPLETE' for the result 
    to be returned.

    Args:
        task_id (uuid.UUID): The unique identifier of the task whose result is being requested.

    Returns:
        TaskResultReply: A response containing:
            - `task_id` (UUID): The unique identifier of the task.
            - `status` (str): The status of the task (must be 'COMPLETE').
            - `result` (any): The result of the task.

    Raises:
        HTTPException:
            - 404: If the task with the given `task_id` does not exist in Redis or has not completed.
            - 500: If a Redis connection error occurs or any unexpected error is encountered.
    """
    try:
        # Attempt to fetch status and result from Redis
        task_status = r.hget(str(task_id), 'status')
        task_result = r.hget(str(task_id), 'result')
        print(f"Redis Task Status: {task_status}, Redis Task Result: {task_result}")
    except redis.exceptions.ConnectionError as e:
        # Handle Redis connection errors
        raise HTTPException(
            status_code=500,
            detail=f"Redis connection error: {str(e)}")
    
    # Check if the task exists in Redis
    if task_status is None:
        raise HTTPException(
            status_code=404,
            detail=f"Task {task_id} does not exist in Redis.")
        
    # Check if the task has finished running
    if task_status != 'COMPLETE':
        raise HTTPException(
            status_code=404,
            detail=f"Task {task_id} has not finished running.")

    # Return the task result
    return TaskResultReply(task_id=task_id, status=task_status, result=task_result)
