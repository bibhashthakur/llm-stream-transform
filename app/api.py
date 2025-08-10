from fastapi import FastAPI
from pydantic import BaseModel
from .plan_schema import Plan
from .llm import compile_plan_from_nl
from .config import MODEL_ID, AWS_REGION

app = FastAPI()
CURRENT_PLAN: Plan | None = None

class NLReq(BaseModel):
    instruction: str

@app.get("/healthz")
def healthz(): return {"ok": True}

@app.post("/plan/json")
def set_plan(plan: Plan):
    global CURRENT_PLAN
    CURRENT_PLAN = plan
    return {"status":"ok","plan": plan.model_dump()}

@app.post("/plan/compile")
def compile_and_set(req: NLReq):
    global CURRENT_PLAN
    plan = compile_plan_from_nl(req.instruction, MODEL_ID, AWS_REGION)
    CURRENT_PLAN = plan
    return {"status":"ok","plan": plan.model_dump()}

def get_current_plan() -> Plan | None:
    return CURRENT_PLAN
