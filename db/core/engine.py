#engine.py

from sqlalchemy import create_engine
from config.settings import settings


engine = create_engine(settings.DATABASE_URL)
