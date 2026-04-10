"""
ШАГ 2 — Парсинг таблицы Crime & Safety Index с Numbeo
=======================================================

Запуск:
  python NumbeoParser.py

Опционально: --output, --reference-json, --manual-map.
Без справочника стран iso2: null.
"""

import argparse
import json
import os
import re
import sys

import requests
from bs4 import BeautifulSoup

from country_reference import enrich_country_entries, load_reference

URL = "https://www.numbeo.com/crime/rankings_by_country.jsp"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch_numbeo_html() -> str:
    response = requests.get(URL, headers=HEADERS, timeout=15)
    response.raise_for_status()
    return response.text


def parse_numbeo_countries(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": "t2"})
    if not table:
        raise ValueError("Таблица с id='t2' не найдена")
    tbody = table.find("tbody")
    if not tbody:
        raise ValueError("tbody не найден")
    rows = tbody.find_all("tr")
    countries = []

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        country_raw = cells[1].get_text(strip=True)
        crime_raw = cells[2].get_text(strip=True)
        safety_raw = cells[3].get_text(strip=True) if len(cells) > 3 else None
        country_clean = re.sub(r"\[.*?\]", "", country_raw).strip()
        if not country_clean:
            continue
        try:
            crime_index = float(crime_raw)
        except ValueError:
            crime_index = None
        try:
            safety_index = float(safety_raw) if safety_raw else None
        except ValueError:
            safety_index = None
        countries.append({
            "country": country_clean,
            "crime_index": crime_index,
            "safety_index": safety_index,
        })

    return countries


def main() -> None:
    parser = argparse.ArgumentParser(description="Парсер Numbeo Crime & Safety Index")
    parser.add_argument("--output", "-o", default="numbeo_data.json")
    parser.add_argument("--reference-json", default=None)
    parser.add_argument("--manual-map", default=None)
    args = parser.parse_args()

    print("Шаг 1: Скачиваем страницу Numbeo...")
    try:
        html = fetch_numbeo_html()
    except requests.exceptions.ConnectionError:
        print("  ✗ Нет подключения к интернету")
        sys.exit(1)
    except requests.exceptions.HTTPError as e:
        print(f"  ✗ Ошибка HTTP: {e}")
        sys.exit(1)

    print(f"  Статус: OK, размер HTML: {len(html)} символов")

    try:
        countries = parse_numbeo_countries(html)
    except ValueError as e:
        print(f"  ✗ {e}")
        sys.exit(1)

    print(f"  Извлечено стран: {len(countries)}")

    ref = load_reference(args.reference_json, os.environ.get("API_COUNTRIES_URL"))
    enrich_country_entries(countries, ref=ref, manual_map_path=args.manual_map)

    result = {
        "source": "Numbeo — Crime & Safety Index by Country 2026",
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
