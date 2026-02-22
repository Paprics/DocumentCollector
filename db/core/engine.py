# engine.py
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "db.sqlite"

# sync engine для миграций
DATABASE_URL = f"sqlite:///{DB_PATH}"
engine = create_engine(
    DATABASE_URL,
    echo=True,  # True — выводит каждый SQL, False — не выводит
    connect_args={"check_same_thread": False}
)

# async engine для скриптов
ASYNC_DATABASE_URL = f"sqlite+aiosqlite:///{DB_PATH}"
async_engine = create_async_engine(
    ASYNC_DATABASE_URL,
    echo=False,
    future=True
)