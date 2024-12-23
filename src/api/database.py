from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.config.settings import get_settings

settings = get_settings()
engine = create_engine(settings.POSTGRES_URL)
Session = sessionmaker(bind=engine) 