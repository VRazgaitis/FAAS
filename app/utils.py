import dill
import codecs
import zmq

def serialize(obj) -> str:
    """
    Serializes a Python object into a Base64-encoded string.

    This function uses the `dill` library to serialize the input object into a 
    byte stream and encodes it in Base64 format to make it suitable for storage 
    or transmission as a string.

    Args:
        obj (any): The Python object to serialize.

    Returns:
        str: A Base64-encoded string representation of the serialized object.
    """
    return codecs.encode(dill.dumps(obj), "base64").decode()

def deserialize(obj: str):
    """
    Deserializes a Base64-encoded string back into a Python object.

    This function decodes a Base64 string into a byte stream and uses the `dill` library 
    to reconstruct the original Python object from the serialized data.

    Args:
        obj (str): A Base64-encoded string representing the serialized object.

    Returns:
        any: The Python object reconstructed from the serialized string.
    """
    return dill.loads(codecs.decode(obj.encode(), "base64"))

def create_zmq_socket(context, socket_type, address_string):
    """
    Create and configure a ZeroMQ socket with the specified type and address.

    This function initializes a ZeroMQ socket from the provided context, sets the
    socket type (e.g., `zmq.REP` or `zmq.REQ`), and binds or connects the socket
    based on its type and the given address string.

    Parameters:
        context (zmq.Context): The ZeroMQ context used to create the socket.
        socket_type (int): The type of socket to create (e.g., `zmq.REP`, `zmq.REQ`, `zmq.ROUTER`).
                           Refer to the ZeroMQ socket types documentation:
                           https://zeromq.org/socket-api/
        address_string (str): The address string for binding or connecting the socket.
                              Examples include "tcp://*:5555" for binding and
                              "tcp://localhost:5555" for connecting.

    Returns:
        zmq.Socket: A configured ZeroMQ socket ready for communication.

    References:
        - ZeroMQ Context: https://pyzmq.readthedocs.io/en/stable/api/zmq.html#zmq.Context
        - ZeroMQ Sockets: https://pyzmq.readthedocs.io/en/stable/api/zmq.html#zmq.Socket
    """
    socket = context.socket(socket_type)
    if socket_type in (zmq.REP, zmq.ROUTER):
        socket.bind(address_string)
    elif socket_type == zmq.REQ:
        socket.connect(address_string)
    return socket

def execute_task(pool, fn_payload, param_payload):
    """
    Executes a task using a multiprocessing pool and returns its status and result.

    This function attempts to execute a provided function (`fn_payload`) with the 
    given arguments (`param_payload`) using the specified multiprocessing pool. 
    It performs error checking on the input parameters and captures exceptions 
    that occur during execution.

    Args:
        pool (multiprocessing.Pool): A multiprocessing pool used to execute the function.
        fn_payload (callable): The function to be executed. 
                               It should accept positional and keyword arguments.
        param_payload (tuple): A tuple containing:
            - args (tuple): Positional arguments to pass to the function.
            - kwargs (dict): Keyword arguments to pass to the function.

    Returns:
        tuple: A tuple containing:
            - func_status (str): The status of the execution, either 'COMPLETE' or 'FAILED'.
            - result_payload (any): The result of the function if successful, or an error message/exception if failed.
    """
    # Unpack function args with error checking
    try:
        args, kwargs = param_payload
    except (TypeError, ValueError) as e:
        func_status = 'FAILED'
        result_payload = f"Invalid param_payload: {e}"
        return func_status, result_payload

    try:
        # Execute the function using the pool
        result_payload = pool.map(lambda x: fn_payload(
            *x[0], **x[1]), [(args, kwargs)])[0]
        func_status = 'COMPLETE'
        return func_status, result_payload
    except Exception as e:
        func_status = 'FAILED'
        result_payload = repr(e)
        return func_status, result_payload
