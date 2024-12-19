from functools import lru_cache
from pydantic_settings import BaseSettings
import os
from dotenv import load_dotenv

load_dotenv(override=True)

class Settings(BaseSettings):
    POSTGRES_HOST: str = os.getenv('POSTGRES_HOST')
    POSTGRES_PORT: str = os.getenv('POSTGRES_PORT')
    POSTGRES_DB: str = os.getenv('POSTGRES_DB')
    POSTGRES_USER: str = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD: str = os.getenv('POSTGRES_PASSWORD')

    @property
    def POSTGRES_URL(self) -> str:
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

@lru_cache
def get_settings() -> Settings:
    return Settings()