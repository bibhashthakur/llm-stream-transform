from fnmatch import fnmatch
from typing import Dict, Any
from .plan_schema import Plan, NumericTransform, StringTransform

def applies_to_table(table: str, pattern: str) -> bool:
    return fnmatch(table, pattern)

def _set(rec: Dict[str,Any], col: str, val: Any):
    rec["after"][col] = val

def _get(rec: Dict[str,Any], col: str):
    return rec["after"].get(col)

def transform_record(rec: Dict[str,Any], plan: Plan) -> Dict[str,Any]:
    table = rec.get("source",{}).get("table","")
    if not applies_to_table(table, plan.tables.get("pattern","*")):
        return rec
    for op in plan.operations:
        if isinstance(op, NumericTransform):
            col = op.column
            if "after" not in rec or col not in rec["after"]:
                continue
            try:
                num = float(_get(rec, col))
                match op.op:
                    case "add": num += op.value
                    case "subtract": num -= op.value
                    case "multiply": num *= op.value
                    case "divide": num /= op.value
                out_col = op.write_to or col
                _set(rec, out_col, str(num))  # keep schema safe as string
            except Exception:
                if op.on_error == "null":
                    _set(rec, op.write_to or col, None)
                elif op.on_error == "leave":
                    pass
                # "skip" -> do nothing
        elif isinstance(op, StringTransform):
            col = op.column
            if "after" not in rec or col not in rec["after"]:
                continue
            val = rec["after"][col]
            if not isinstance(val, str):
                continue
            if op.op == "lower": val = val.lower()
            elif op.op == "upper": val = val.upper()
            elif op.op == "trim": val = val.strip()
            _set(rec, op.write_to or col, val)
    return rec
