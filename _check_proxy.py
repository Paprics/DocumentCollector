import os
import httpx
from dotenv import load_dotenv

load_dotenv()

TEST_URL = "https://api.ipify.org?format=json"
TIMEOUT = 10


def load_proxies():
    proxies = os.getenv("PROXIES", "")
    return [p.strip() for p in proxies.split(",") if p.strip()]


def check_proxy(proxy: str) -> bool:
    try:
        with httpx.Client(proxy=proxy, timeout=TIMEOUT) as client:
            r = client.get(TEST_URL)
        if r.status_code == 200:
            print(f"[OK] {proxy} → {r.json()}")
            return True
    except Exception as e:
        print(f"[FAIL] {proxy} → {e}")
    return False


def main():
    proxies = load_proxies()

    if not proxies:
        print("Прокси не найдены в .env")
        return

    working = []
    failed = []

    for proxy in proxies:
        print(f"\nПроверка: {proxy}")
        if check_proxy(proxy):
            working.append(proxy)
        else:
            failed.append(proxy)

    print("\n" + "=" * 50)
    print(f"Рабочие: {len(working)}")
    print(f"Нерабочие: {len(failed)}")

    if working:
        print("\nWORKING PROXIES:")
        for p in working:
            print(p)


if __name__ == "__main__":
    main()