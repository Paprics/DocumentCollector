# crud.py

from sqlalchemy import select, insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.file_hash import FileHash, Tender


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


# -------------------------
# Tender
# -------------------------

async def insert_tender(session: AsyncSession, tender_id: str) -> bool | None:
    """
    Вставляет идентификатор тендера.
    Возвращает:
        True  — если реально вставлен
        None  — если вставка не произошла (дубликат или ошибка)
    """
    try:
        stmt = (
            insert(Tender)
            .values(tender_id=tender_id)
            .prefix_with("OR IGNORE")  # SQLite: тихо игнорировать дубликаты
        )

        result = await session.execute(stmt)
        await session.commit()

        return True if result.rowcount == 1 else None
    except Exception:
        await session.rollback()
        return None


async def tender_exists(session: AsyncSession, tender_id: str) -> bool:
    """
    Быстрая проверка существования тендера.
    """
    stmt = select(Tender.id).where(Tender.tender_id == tender_id).limit(1)
    result = await session.execute(stmt)
    return result.scalar_one_or_none() is not None