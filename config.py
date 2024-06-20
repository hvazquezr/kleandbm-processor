from functools import lru_cache
from typing import List

from pydantic_settings import BaseSettings
from pathlib import Path

class Settings(BaseSettings):
    kafka_server: str
    kafka_username: str
    kafka_password: str
    mongo_uri: str
    mongo_db: str

    class Config:
        env_file = Path(__file__).parent / ".env"

@lru_cache()
def get_settings():
    return Settings()