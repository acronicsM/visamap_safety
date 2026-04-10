"""
ШАГ 4 — Парсинг PDF: Gallup Global Safety Report 2025
======================================================

Что получаем (две таблицы):
  1. Law and Order Index      — составной индекс (0-100)
  2. Safe to Walk at Night %  — % людей чувствующих себя в безопасности

Запуск:
  pip install pdfplumber
  python GallupParser.py inputdata\\Gallup_Global-Safety-Report-2025.pdf

Опционально: --output, --reference-json (кэш API), --manual-map.
Поле iso2 добавляется, если задан API_COUNTRIES_URL, --reference-json или есть алиасы в manual_country_map.json;
иначе iso2: null.
"""

import argparse
import json
import os
import re
import sys

import pdfplumber

from country_reference import enrich_country_entries, load_reference

SKIP_WORDS = {
    "Country", "Territory", "Country / Territory",
    "Law and Order", "Index Score", "Safe to Walk",
    "Alone at Night", "The Global Safety Report",
    "Copyright", "Gallup", "GlobalSafetyReport",
    "Methodology", "Analytics", "Chart", "Charts",
}


def extract_table(all_lines, value_re, value_cast):
    results = {}

    for line in all_lines:
        # Law and Order lines look like "Tajikistan 97 Indonesia 89" — split after a digit.
        # Safe to Walk lines look like "Singapore 98% Egypt 82%" — the % breaks the digit
        # lookbehind, so also split after "%" before the next country name.
        parts = re.split(r'(?:(?<=\d)|(?<=%))\s+(?=[A-Z])', line)

        for part in parts:
            part = part.strip()

            m = re.match(
                r'^([A-Za-z][A-Za-z\s()\',./-]+?)\s+(' + value_re + r')\s*$',
                part
            )
            if not m:
                continue

            country = m.group(1).strip()

            if len(country) < 3:
                continue
            if len(country) > 50:
                continue
            if country in SKIP_WORDS:
                continue
            if re.search(r'\d', country):
                continue
            if len(country.split()) > 6:
                continue

            try:
                results[country] = value_cast(m.group(2))
            except ValueError:
                continue

    return results


def parse_gallup_pdf(pdf_path: str) -> list[dict]:
    pdf = pdfplumber.open(pdf_path)
    all_lines = []
    for page in pdf.pages:
        text = page.extract_text()
        if text:
            all_lines.extend(text.split("\n"))
    pdf.close()

    law_order = extract_table(all_lines, value_re=r'\d{2,3}', value_cast=int)
    law_order = {k: v for k, v in law_order.items() if 49 <= v <= 100}

    safe_walk = extract_table(
        all_lines,
        value_re=r'\d{2,3}%',
        value_cast=lambda x: int(x.rstrip('%'))
    )
    safe_walk = {k: v for k, v in safe_walk.items() if 0 <= v <= 100}

    all_countries = set(law_order.keys()) | set(safe_walk.keys())
    countries = []
    for country in sorted(all_countries):
        entry = {"country": country}
        if country in law_order:
            entry["law_and_order_index"] = law_order[country]
        if country in safe_walk:
            entry["safe_walking_night_pct"] = safe_walk[country]
        countries.append(entry)

    countries.sort(key=lambda x: x.get("law_and_order_index", 0), reverse=True)
    return countries


def main() -> None:
    parser = argparse.ArgumentParser(description="Парсер Gallup Global Safety Report PDF")
    parser.add_argument("pdf", help="Путь к PDF файлу")
    parser.add_argument(
        "--output", "-o",
        default="gallup_data.json",
        help="Выходной JSON (по умолчанию gallup_data.json)",
    )
    parser.add_argument(
        "--reference-json",
        default=None,
        help="Кэш ответа GET …/countries/names (вместо запроса по API_COUNTRIES_URL)",
    )
    parser.add_argument(
        "--manual-map",
        default=None,
        help="Путь к manual_country_map.json (иначе MANUAL_COUNTRY_MAP_FILE или файл рядом со скриптом)",
    )
    args = parser.parse_args()

    pdf_path = args.pdf
    print(f"Открываем: {pdf_path}")

    try:
        countries = parse_gallup_pdf(pdf_path)
    except FileNotFoundError:
        print(f"  ✗ Файл не найден: {pdf_path}")
        sys.exit(1)

    print(f"  Итого стран (до iso2): {len(countries)}")

    ref = load_reference(args.reference_json, os.environ.get("API_COUNTRIES_URL"))
    enrich_country_entries(countries, ref=ref, manual_map_path=args.manual_map)

    result = {
        "source": "Gallup Global Safety Report 2025",
        "url": "https://www.gallup.com/analytics/356996/gallup-global-law-and-order.aspx",
        "provider": "Gallup",
        "year": 2024,
        "note": (
            "law_and_order_index: 0-100 (выше = безопаснее), составной индекс из 4 вопросов. "
            "safe_walking_night_pct: % людей ответивших 'да' на вопрос о безопасности ночью."
        ),
        "total_countries": len(countries),
        "countries": countries,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"  ✓ Сохранено: {args.output}")
    print(f"  ✓ Стран: {len(countries)}")
    print("\nГотово!")


if __name__ == "__main__":
    main()
