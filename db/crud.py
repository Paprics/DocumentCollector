from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.file_hash import FileHash


async def insert_file_hash(session: AsyncSession, hash_value: str) -> bool:
    """
    Пытается вставить хеш.
    Возвращает:
        True  — если вставлен
        False — если уже был в БД
    """
    obj = FileHash(hash=hash_value)
    session.add(obj)

    try:
        await session.commit()
        return True
    except IntegrityError:
        await session.rollback()
        return False


async def file_hash_exists(session: AsyncSession, hash_value: str) -> bool:
    """
    Быстрая проверка существования хеша.
    """
    stmt = select(FileHash.id).where(FileHash.hash == hash_value).limit(1)
    result = await session.execute(stmt)
    return result.scalar_one_or_none() is not None