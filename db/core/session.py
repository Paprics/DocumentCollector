#session.py
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from .engine import engine

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=True,          # по умолчанию True
    autocommit=False,        # по умолчанию False
    expire_on_commit=True,   # по умолчанию True
)

