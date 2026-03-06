
import os

def save_files_as_html(tender_id: str, files: list, base_name: str, source_idx: int):
    """
    Сама решает, куда сохранять: в output/output_data/{base_name}.html
    Создаёт папку, если нет.
    Если файла нет — создаёт с началом HTML.
    Дописывает тендер и документы.
    """

    output_folder = "output_data"
    file_name = f'{source_idx}. {base_name}'
    if not file_name.lower().endswith('.html'):
        file_name += '.html'

    output_filename = os.path.join(output_folder, file_name)

    try:

        os.makedirs(output_folder, exist_ok=True)

        # Проверяем, существует ли файл — если нет, пишем начало HTML
        if not os.path.exists(output_filename):
            with open(output_filename, "w", encoding="utf-8") as f:
                f.write('<!DOCTYPE html>\n')
                f.write('<html lang="uk">\n<head>\n<meta charset="UTF-8">\n<title>Документы Prozorro</title>\n</head>\n')
                f.write('<body style="font-family: Arial, sans-serif; margin:20px;">\n')
                f.write(f'<h1>Джерело: {base_name}</h1>\n')
                f.write('<hr style="border:1px solid #444;">\n')

        tender_url = f"https://prozorro.gov.ua/tender/{tender_id}"

        with open(output_filename, "a", encoding="utf-8") as f:
            f.write(f'<h2>Тендер: {tender_id}</h2>\n')
            f.write(f'<p><a href="{tender_url}" target="_blank" style="color:#0066cc;">Открыть тендер на Prozorro</a></p>\n')
            f.write('<ul>\n')

            if not files:
                f.write('<li>Документов не найдено</li>\n')
            else:
                for name, href in files:
                    safe_name = name.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')
                    f.write(f'<li><a href="{href}" target="_blank">{safe_name}</a></li>\n')

            f.write('</ul>\n')
            f.write('<hr style="border:1px dashed #999; margin:20px 0;">\n')

        if len(files) > 0:
            print(f'[ОК] Сохранено {len(files)} документов → {output_filename}')

    except Exception as e:
        print(f"[ОШИБКА СОХРАНЕНИЯ] Тендер {tender_id}: {e}")


if "__main__" == __name__:
    pass