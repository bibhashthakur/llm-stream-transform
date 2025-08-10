import json, os
from typing import List, Dict, Any
import boto3

SYSTEM_PROMPT = (
  "You transform an array of input rows into an array of equal length.\n"
  "Output ONLY a minified JSON array. No commentary.\n"
  "For each input item {\"i\":<int>, \"columns\":{...}}, return an object:\n"
  "{\"i\":<same int>, \"outputs\":{...}, \"confidence\":<0..1>}.\n"
  "Keep the same order and count as inputs. If unsure, set confidence lower and return empty strings."
)

def build_user_payload(task: str, inputs: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "task": task,
        "inputs": inputs
    }

def call_bedrock_batch(model_id: str, region: str, instruction: str, inputs: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    client = boto3.client("bedrock-runtime", region_name=region)
    body = {
        "anthropic_version":"bedrock-2023-05-31",
        "system": SYSTEM_PROMPT,
        "temperature": 0,
        "max_tokens": 2000,
        "messages": [{
            "role":"user",
            "content":[{"type":"text","text": json.dumps(build_user_payload(instruction, inputs), separators=(",",":"))}]
        }]
    }
    resp = client.invoke_model(modelId=model_id, body=json.dumps(body))
    out = json.loads(resp["body"].read())
    text = out["content"][0]["text"]
    try:
        arr = json.loads(text)
        if not isinstance(arr, list):
            raise ValueError("Model did not return a JSON array")
        return arr
    except Exception as e:
        # one retry with a stricter reminder
        body["messages"][-1]["content"][0]["text"] = "Remember: Output ONLY a minified JSON array.\n" + body["messages"][-1]["content"][0]["text"]
        resp = client.invoke_model(modelId=model_id, body=json.dumps(body))
        out = json.loads(resp["body"].read())
        text = out["content"][0]["text"]
        arr = json.loads(text)
        if not isinstance(arr, list):
            raise ValueError("Model did not return a JSON array after retry")
        return arr
