from typing import Any, Optional
from _pytest import config
from pydantic import BaseSettings, BaseModel

class Config(BaseSettings):
  id: str
  secret: str

class BinaryObj(BaseModel):
  id: str
  data: bytes

class ConfigResult(BaseModel):
  status_code: int

class NestedObject(BaseModel):
  id: str
  bin: Optional[BinaryObj]
  conf: Optional[Config]