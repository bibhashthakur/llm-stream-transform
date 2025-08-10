from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

app = FastAPI()

INSTRUCTION: str | None = None
OUTPUT_MAPPING: Dict[str,str] | None = None  # e.g., {"first_name":"first_name","last_name":"last_name"}

class InstructionReq(BaseModel):
    instruction: str
    output_mapping: Optional[Dict[str,str]] = None

class PreviewReq(BaseModel):
    instruction: str
    rows: List[Dict[str,Any]]               # list of CDC-like records
    input_columns: Optional[List[str]] = None
    output_mapping: Optional[Dict[str,str]] = None

@app.get("/healthz")
def healthz(): return {"ok": True}

@app.get("/plan")
def get_plan():
    return {"instruction": INSTRUCTION, "output_mapping": OUTPUT_MAPPING}

@app.post("/plan/instruction")
def set_instruction(body: InstructionReq):
    global INSTRUCTION, OUTPUT_MAPPING
    INSTRUCTION = body.instruction
    OUTPUT_MAPPING = body.output_mapping
    return {"status":"ok"}

# wired in main.py to use llm_batch.preview
# in app/main.py after imports
from fastapi import Body
from typing import Dict, Any, List, Optional
from .llm_batch import call_bedrock_batch
from .validator import validate_batch_response
from .merge import merge_outputs_into_record
from .config import MODEL_ID, AWS_REGION, OUTPUT_PREFIX

@app.post("/plan/preview")
def preview(payload: Dict[str,Any] = Body(...)):
    instruction: str = payload["instruction"]
    rows: List[Dict[str,Any]] = payload["rows"]
    input_columns: Optional[List[str]] = payload.get("input_columns")
    output_mapping: Optional[Dict[str,str]] = payload.get("output_mapping")

    inputs = []
    for i, rec in enumerate(rows):
        after = rec.get("after", {}) or {}
        cols = {k: after.get(k) for k in input_columns} if input_columns else after
        inputs.append({"i": i, "columns": cols})

    results = call_bedrock_batch(MODEL_ID, AWS_REGION, instruction, inputs)
    results = validate_batch_response(inputs, results)
    merged = [merge_outputs_into_record(rows[i], results[i]["outputs"], output_mapping, OUTPUT_PREFIX) for i in range(len(rows))]
    return {"results": results, "merged": merged}

