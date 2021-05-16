from typing import Any
from pydantic import BaseSettings

class Config(BaseSettings):
  id: str
  secret: str

class ConfigResult(BaseSettings):
  status_code: int
