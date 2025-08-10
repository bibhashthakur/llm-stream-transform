from pydantic import BaseModel, Field
from typing import Literal, List, Optional

class NumericTransform(BaseModel):
    type: Literal["numeric_transform"]
    column: str
    coerce: Literal["int","float"] = "float"
    op: Literal["add","subtract","multiply","divide"] = "multiply"
    value: float
    write_to: Optional[str] = None
    on_error: Literal["skip","null","leave"] = "skip"

class StringTransform(BaseModel):
    type: Literal["string_transform"]
    column: str
    op: Literal["lower","upper","trim"]  # keep it tiny for hackathon
    write_to: Optional[str] = None

Operation = NumericTransform | StringTransform

class Plan(BaseModel):
    tables: dict  # {"pattern": "Magic*"}
    operations: List[Operation]
