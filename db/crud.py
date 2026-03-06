# crud.py

from sqlalchemy import select, insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from .core.session import SyncSessionLocal

from db.models.file_hash import FileHash, Tender


# -------------------------
# Tender
# -------------------------
def sync_tender_exists(tender_id: str) -> bool:
    """
    Проверяет, есть ли tender с данным tender_id в базе.
    Возвращает True, если есть, False — если нет или ошибка.
    """
    try:
        with SyncSessionLocal() as session:
            stmt = select(Tender).where(Tender.tender_id == tender_id)
            result = session.execute(stmt).first()
            return result is not None
    except SQLAlchemyError:
        return False


def sync_insert_tender_to_db(tender_id: str) -> bool:
    """
    Добавляет новый tender с tender_id в базу.
    Возвращает True, если успешно добавлен, False при ошибке или если уже есть.
    """
    try:
        with SyncSessionLocal() as session:
            # проверка на существование
            if session.query(Tender).filter(Tender.tender_id == tender_id).first():
                return False

            new_tender = Tender(tender_id=tender_id)
            session.add(new_tender)
            session.commit()
            return True
    except SQLAlchemyError:
        return False


# -------------------------
# FileHash
# -------------------------

async def insert_file_hash(session: AsyncSession, hash_value: str) -> bool | None:
    """
    Вставляет хеш файла.
    Возвращает:
        True  — если реально вставлен
        None  — если вставка не произошла (дубликат или ошибка)
    """
    try:
        stmt = (
            insert(FileHash)
            .values(hash=hash_value)
            .prefix_with("OR IGNORE")  # SQLite: тихо игнорировать дубликаты
        )

        result = await session.execute(stmt)
        await session.commit()

        return True if result.rowcount == 1 else None
    except Exception:
        await session.rollback()
        return None


async def file_hash_exists(session: AsyncSession, hash_value: str) -> bool:
    """
    Быстрая проверка существования хеша.
    """
    stmt = select(FileHash.id).where(FileHash.hash == hash_value).limit(1)
    result = await session.execute(stmt)
    return result.scalar_one_or_none() is not None