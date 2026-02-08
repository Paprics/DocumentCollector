import os

def save_files_as_html(url: str, files: list, filename="output_data/output.html"):

    # убеждаемся, что директория существует
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)

    with open(filename, "a", encoding="utf-8") as f:
        f.write(f"<p><strong>Страница:</strong> <a href='{url}'>{url}</a></p>\n")
        f.write("<ul>\n")
        for name, href in files:
            f.write(f"  <li><a href='{href}'>{name}</a></li>\n")
        f.write("</ul>\n")
        f.write("<hr>\n")

    print(f'Записано - {len(files)}')


def is_title_allowed(title):
    return True