from sqlalchemy import text
from db.core.session import SessionLocal

print("Проверка подключения к БД...")

try:
    with SessionLocal() as session:
        result = session.execute(text("SELECT 1"))
        print(f"[DEBUG] Подключение успешно! Ответ БД: {result.scalar()}")
except Exception as e:
    print(f"[ERROR] Ошибка подключения: {str(e)}")