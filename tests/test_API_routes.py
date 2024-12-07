from fastapi.testclient import TestClient
from app.main import app 
from .serialize import serialize, deserialize
from .test_webservice import double

client = TestClient(app)

def test_home_endpoint():
    response = client.get("/")
    assert response.status_code == 200
    
def test_fn_registration():
    # Using a real serialized function
    serialized_fn = serialize(double)
    resp = client.post("/register_function",
                            json={"name": "double",
                                "payload": serialized_fn})

    assert resp.status_code in [200, 201]
    assert "function_id" in resp.json()