from typing import Dict, Any, Optional

def merge_outputs_into_record(rec: Dict[str,Any], outs: Dict[str,Any], mapping: Optional[Dict[str,str]], prefix: str):
    # rec is a CDC-style dict with rec["after"] holding the row payload
    if "after" not in rec or not isinstance(rec["after"], dict):
        rec["after"] = {}
    if mapping:
        for k, v in mapping.items():
            rec["after"][v] = outs.get(k, "")
    else:
        # write all outputs with a prefix
        for k, v in outs.items():
            rec["after"][f"{prefix}{k}"] = v
    return rec
