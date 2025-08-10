from app.plan_schema import Plan
from app.transformer import transform_record

plan = Plan.model_validate({
  "tables": {"pattern": "Magic*"},
  "operations": [{
    "type": "numeric_transform",
    "column": "MagicVariables",
    "coerce": "float",
    "op": "multiply",
    "value": 5,
    "on_error": "skip"
  }]
})

record = {
  "source": {"table": "MagicOrders"},
  "op": "c",
  "after": {"id": 1, "MagicVariables": "12.4", "other": "x"}
}

print(transform_record(record, plan))
