#download_file.py
import os
from pathlib import Path
from typing import Iterator, Tuple
import requests

BASE_DIR = Path(__file__).resolve().parent


def iter_links(html_path: str) -> Iterator[Tuple[str, str]]:
    """
    Генератор (name, url) из HTML-файла формата:
    <li><a href='URL'>NAME</a></li>
    """
    with open(html_path, encoding="utf-8") as f:
        for line in f:
            if "<a href=" not in line:
                continue

            try:
                href_part = line.split("href=", 1)[1]
                quote = href_part[0]
                url = href_part.split(quote, 2)[1]

                name = href_part.split(">", 1)[1].split("</a>", 1)[0].strip()

                yield name, url
            except IndexError:
                continue


def sanitize_name(name: str) -> str:
    """
    Превращает имя файла из HTML в безопасное для Windows:
    заменяет запрещённые символы на _
    """
    forbidden = '<>:"/\\|?*'
    table = str.maketrans({c: "_" for c in forbidden})
    return name.translate(table) or "file"


def download_files_by_name(
    html_path: str,
    output_dir: str,
    keywords: tuple[str, ...],
):
    """
    Скачивает файлы, если в названии ссылки есть
    совпадение с любым словом из keywords (без учёта регистра).
    Генератор: возвращает путь к каждому скачанному файлу.
    """
    os.makedirs(output_dir, exist_ok=True)
    keywords = tuple(k.lower() for k in keywords)

    for name, url in iter_links(html_path):
        name_lower = name.lower()
        if not any(k in name_lower for k in keywords):
            continue

        filename = sanitize_name(name)
        out_path = Path(output_dir) / filename

        with requests.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(8192):
                    if chunk:
                        f.write(chunk)

        yield out_path


if __name__ == "__main__":
    for saved in download_files_by_name(
        html_path=BASE_DIR / "output_data" / "Легковые авато [100 -200 стр.].html",
        output_dir=BASE_DIR / "downloads",
        keywords=("pas", "паспорт", "пасспорт"),
    ):
        print("Скачан:", saved)
