import json, boto3, os
from .plan_schema import Plan

SYSTEM = (
  "Translate user data transformation requests into STRICT JSON matching this schema: "
  '{"tables":{"pattern":"<glob>"},"operations":[{"type":"numeric_transform","column":"<str>",'
  '"coerce":"int|float","op":"add|subtract|multiply|divide","value":<num>,"write_to":"<str|optional>",'
  '"on_error":"skip|null|leave"}'
  ',{"type":"string_transform","column":"<str>","op":"lower|upper|trim","write_to":"<str|optional>"}]} '
  "Output JSON only."
)

def compile_plan_from_nl(nl: str, model_id: str, region: str) -> Plan:
    client = boto3.client("bedrock-runtime", region_name=region)
    body = {
      "anthropic_version": "bedrock-2023-05-31",
      "system": SYSTEM,
      "messages": [{"role":"user","content":[{"type":"text","text": nl}]}],
      "max_tokens": 800
    }
    resp = client.invoke_model(modelId=model_id, body=json.dumps(body))
    out = json.loads(resp["body"].read())
    text = out["content"][0]["text"]
    return Plan.model_validate_json(text)
