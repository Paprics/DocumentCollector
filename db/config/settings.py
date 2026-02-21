#settings.py
import os
from pathlib import Path


class Settings:
    BASE_DIR: Path = Path(__file__).resolve().parent.parent

    # Postgres параметры (для Docker)
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "postgres")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "postgres")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "core")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", 5432))

    MODE: str = os.getenv("MODE", "LOCAL").upper()

    if os.getenv('MODE') == 'DOCKER':
        DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    else:
        DATABASE_URL: str = f"sqlite:///{BASE_DIR / 'database.core'}"





settings = Settings()


if __name__ == '__main__':
    print(settings.BASE_DIR)
