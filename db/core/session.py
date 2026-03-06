# db/core/session.py
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import async_sessionmaker

from .engine import engine, async_engine


# ────────────────────────────────────────────────
# Синхронные сессии
# ────────────────────────────────────────────────

SyncSessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,          # чаще всего лучше False, чтобы явно контролировать flush
    autocommit=False,
    expire_on_commit=False,   # обычно False — объекты не теряют состояние после commit
)


# ────────────────────────────────────────────────
# Асинхронные сессии
# ────────────────────────────────────────────────

AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    expire_on_commit=False,
    autoflush=False,          # для async тоже часто ставят False
)

