from sqlalchemy import String

class FileHash(Base):
    __tablename__ = "file_hashes"

    id: Mapped[int] = mapped_column(primary_key=True)

    hash: Mapped[str] = mapped_column(
        String(64),  # SHA-256 hex
        unique=True,
        nullable=False,
        index=True
    )


# бинарное хранение (рекомендую)
# from sqlalchemy.orm import Mapped, mapped_column
# from sqlalchemy import LargeBinary
# from typing import Optional
# from datetime import datetime
# from sqlalchemy import DateTime
# from sqlalchemy.sql import func
#
# class FileHash(Base):
#     __tablename__ = "file_hashes"
#
#     id: Mapped[int] = mapped_column(primary_key=True)
#
#     # 32 байта для SHA-256
#     hash: Mapped[bytes] = mapped_column(
#         LargeBinary(32),
#         unique=True,
#         nullable=False,
#         index=True
#     )
#
#     created_at: Mapped[datetime] = mapped_column(
#         DateTime,
#         server_default=func.now(),
#         nullable=False
#     )