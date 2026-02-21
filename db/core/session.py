# session.py
from .engine import engine, async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import async_sessionmaker

# синхронная сессия для Alembic
SessionLocal = sessionmaker(
    bind=engine,
    autoflush=True,
    autocommit=False,
    expire_on_commit=True,
)

# асинхронная сессия для скриптов
async_session = async_sessionmaker(
    async_engine,
    expire_on_commit=False
)
