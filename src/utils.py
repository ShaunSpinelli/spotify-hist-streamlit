import json


def load_json(path:str) -> dict:
    with open(path) as f:
        data = json.load(f)
    return data
