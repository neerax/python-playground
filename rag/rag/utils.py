import json
def toJson(p):
    return json.dumps(p, ensure_ascii=False, indent=2)