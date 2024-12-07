import dill
import codecs

def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode()


def deserialize(obj: str):
    return dill.loads(codecs.decode(obj.encode(), "base64"))

