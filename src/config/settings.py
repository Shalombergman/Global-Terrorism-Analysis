from functools import lru_cache
from pydantic_settings import BaseSettings
import os
from dotenv import load_dotenv

load_dotenv(override=True)

class Settings(BaseSettings):
    POSTGRES_HOST: str = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT: str = os.getenv('POSTGRES_PORT', '5432')
    POSTGRES_DB: str = os.getenv('POSTGRES_DB', 'terrorism_db')
    POSTGRES_USER: str = os.getenv('POSTGRES_USER', 'postgres')
    POSTGRES_PASSWORD: str = os.getenv('POSTGRES_PASSWORD', 'postgres')
    ELASTICSEARCH_HOST: str = os.getenv('ELASTICSEARCH_HOST', 'http://localhost:9200')
    NEWS_API_URL: str = os.getenv('NEWS_API_URL', 'http://localhost:5001')
    NEWS_API_KEY: str = os.getenv('NEWS_API_KEY', '')
    GROQ_API_KEY: str = os.getenv('GROQ_API_KEY', '')
    OPENCAGE_API_KEY: str = os.getenv('OPENCAGE_API_KEY', '')
    OPENCAGE_GEOCODE_URL: str = os.getenv('OPENCAGE_GEOCODE_URL', '')

    @property
    def POSTGRES_URL(self) -> str:
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

@lru_cache
def get_settings() -> Settings:
    return Settings()