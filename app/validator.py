from typing import List, Dict, Any

def validate_batch_response(inputs: List[dict], outputs: List[dict]) -> List[dict]:
    if len(inputs) != len(outputs):
        raise ValueError(f"Length mismatch: inputs={len(inputs)} outputs={len(outputs)}")
    # index map from i to position
    idx = {item["i"]: pos for pos, item in enumerate(inputs)}
    cleaned = []
    for obj in outputs:
        if not isinstance(obj, dict): continue
        if "i" not in obj or "outputs" not in obj: continue
        i = obj["i"]
        if i not in idx: continue
        outs = obj["outputs"]
        if not isinstance(outs, dict): outs = {}
        conf = obj.get("confidence", 0.0)
        cleaned.append({"i": i, "outputs": outs, "confidence": float(conf)})
    # reorder by original order
    cleaned.sort(key=lambda x: idx[x["i"]])
    if len(cleaned) != len(inputs):
        # fill missing with empty outputs
        present = {c["i"] for c in cleaned}
        for item in inputs:
            if item["i"] not in present:
                cleaned.insert(idx[item["i"]], {"i": item["i"], "outputs": {}, "confidence": 0.0})
    return cleaned
