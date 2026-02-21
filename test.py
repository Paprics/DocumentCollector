from pathlib import Path

from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent
html_file = BASE_DIR / 'output_data' / 'Легковые авто [100 -200 стр.].html'

def iter_links(html_path: Path):
    with open(html_path, encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')
        for a in soup.find_all('a', href=True):
            yield a.get_text(strip=True), a['href']

count = 0
for i in iter_links(html_file):
    if any(k in i[0].lower() for k in ('pas', 'пас')):
        count += 1
        print(f'{count}: {i[0]}')