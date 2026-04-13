"""
ШАГ 1 — Парсинг таблицы Global Peace Index с Wikipedia
========================================================

Установка:
  pip install requests beautifulsoup4

Запуск:
  python GPIparser.py

Опционально: --output, --reference-json, --manual-map.
Без справочника стран iso2: null.
Для Wikipedia: в .env задайте WIKIPEDIA_USER_AGENT (контакт оператора), иначе возможны HTTP 403 / rate limit.
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from country_reference import enrich_country_entries, load_reference

URL = "https://en.wikipedia.org/wiki/Global_Peace_Index"
ROOT = Path(__file__).resolve().parent


def _wikipedia_headers() -> dict[str, str]:
    """
    Wikimedia требует осмысленный User-Agent с контактом оператора.
    См. https://meta.wikimedia.org/wiki/User-Agent_policy
    """
    lib_ua = requests.utils.default_user_agent()
    custom = (os.environ.get("WIKIPEDIA_USER_AGENT") or "").strip()
    if custom:
        ua = custom
    else:
        ua = (
            "visamap_safety-GPIparser/1.0 "
            "(set WIKIPEDIA_USER_AGENT in .env with your repo URL or email per Wikimedia policy) "
            f"{lib_ua}"
        )
    return {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }


def fetch_gpi_html() -> str:
    response = requests.get(URL, headers=_wikipedia_headers(), timeout=30)
    response.raise_for_status()
    return response.text


def parse_gpi_countries(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    all_tables = soup.find_all("table", class_="wikitable")
    target_table = None

    for table in all_tables:
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        headers_text = " ".join(headers).lower()
        if "rank" in headers_text and "country" in headers_text and "score" in headers_text:
            target_table = table
            break

    if target_table is None:
        raise ValueError("Таблица GPI на странице не найдена")

    all_rows = target_table.find_all("tr")
    countries = []

    for row in all_rows[1:]:
        cells = row.find_all(["td", "th"])
        if len(cells) < 3:
            continue
        values = [cell.get_text(strip=True) for cell in cells]
        country_raw = values[1]
        score_raw = values[2]
        country_clean = re.sub(r"\[.*?\]", "", country_raw).strip()
        if country_clean.lower() in ("country", "nation", ""):
            continue
        try:
            score = float(score_raw)
        except ValueError:
            score = None
        countries.append({
            "country": country_clean,
            "score": score,
        })

    return countries


def main() -> None:
    parser = argparse.ArgumentParser(description="Парсер Global Peace Index (Wikipedia)")
    parser.add_argument("--output", "-o", default="gpi_data.json")
    parser.add_argument("--reference-json", default=None)
    parser.add_argument("--manual-map", default=None)
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")

    print("Шаг 1: Скачиваем страницу Wikipedia...")
    try:
        html = fetch_gpi_html()
    except requests.exceptions.ConnectionError:
        print("  ✗ Ошибка: нет подключения к интернету")
        sys.exit(1)
    except requests.exceptions.Timeout:
        print("  ✗ Ошибка: сайт не ответил за 15 секунд")
        sys.exit(1)
    except requests.exceptions.HTTPError as e:
        print(f"  ✗ Ошибка HTTP: {e}")
        if e.response is not None and e.response.status_code in (403, 429):
            print(
                "  Подсказка: Wikipedia часто отклоняет запросы без корректного User-Agent.\n"
                "  Задайте в .env переменную WIKIPEDIA_USER_AGENT (см. .env.example и User-Agent policy Wikimedia).\n"
                "  Если был rate limit — подождите несколько минут и повторите.",
                file=sys.stderr,
            )
        sys.exit(1)

    print(f"  Статус: OK, размер HTML: {len(html)} символов")

    try:
        countries = parse_gpi_countries(html)
    except ValueError as e:
        print(f"  ✗ {e}")
        sys.exit(1)

    print(f"  Извлечено стран: {len(countries)}")

    ref = load_reference(args.reference_json, os.environ.get("API_COUNTRIES_URL"))
    enrich_country_entries(countries, ref=ref, manual_map_path=args.manual_map)

    result = {
        "source": "Wikipedia — Global Peace Index 2025",
        "url": URL,
        "total_countries": len(countries),
        "countries": countries,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"  ✓ Сохранено в файл: {args.output}")
    print("\nГотово!")


if __name__ == "__main__":
    main()
