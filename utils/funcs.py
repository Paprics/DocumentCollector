import os
from pathlib import Path
import yaml


def save_files_as_html(url: str, files: list, filename: str):

    # убеждаемся, что директория существует
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)

    with open(filename, "a", encoding="utf-8") as f:
        f.write(f"<p><strong>Страница:</strong> <a href='{url}'>{url}</a></p>\n")
        f.write("<ul>\n")
        for name, href in files:
            f.write(f"  <li><a href='{href}'>{name}</a></li>\n")
        f.write("</ul>\n")
        f.write("<hr>\n")

    print(f'[OK]  ✅ Записано - {len(files)} документов.')


def update_yaml_status(
        key: int,
        new_status: str = "✅Done"
) -> bool:
    """
    Обновляет значение 'status' для указанного ключа в YAML-файле.

    Args:
        yaml_file_path: путь к файлу sources.yaml (строка или Path)
        key: ключ словаря (например 0, 26, 27 и т.д.)
        new_status: новое значение поля status (по умолчанию '✅Done')

    Returns:
        bool: True — если успешно изменено, False — если ошибка или ключ не найден
    """
    file_path = Path('../_OLD_modeles/sources.yaml')

    if not file_path.exists():
        print(f"❌ Файл не найден: {file_path}")
        return False

    try:
        # Читаем и парсим
        with file_path.open(encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if data is None:
            print("❌ Файл пустой или некорректный YAML")
            return False

        if key not in data:
            print(f"❌ Ключ {key} не найден в файле")
            return False

        old_status = data[key].get('status', '(не было)')
        data[key]['status'] = new_status

        print(f"✓ Изменение для ключа {key}:")

        # Сохраняем с красивым форматированием
        with file_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(
                data,
                f,
                allow_unicode=True,  # эмодзи и кириллица без проблем
                sort_keys=False,  # сохраняем исходный порядок ключей
                default_flow_style=False  # читаемый многострочный вид
            )

        print(f"Файл успешно обновлён: {file_path}")
        return True

    except yaml.YAMLError as e:
        print(f"Ошибка парсинга YAML: {e}")
        return False
    except Exception as e:
        print(f"Неожиданная ошибка: {type(e).__name__}: {e}")
        return False

if "__main__" == __name__:
    update_yaml_status(key=23)