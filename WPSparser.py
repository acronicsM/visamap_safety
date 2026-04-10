"""
ШАГ 3 — Парсинг PDF: Women, Peace and Security Index 2025/26
=============================================================

Что получаем:
  - Страна
  - WPS Score (индекс безопасности женщин, от 0 до 1)

Установка:
  pip install pdfplumber

Запуск:
  python WPSparser.py <pdf>

Опционально: --output, --reference-json, --manual-map.
Без справочника стран iso2 в JSON будет null.
"""

import argparse
import json
import os
import re
import sys

import pdfplumber

from country_reference import enrich_country_entries, load_reference


def parse_wps_pdf(pdf_path: str) -> list[dict]:
    pdf = pdfplumber.open(pdf_path)
    try:
        target_pages = []
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            if text and "Afghanistan" in text and ".279" in text:
                target_pages.append(i)

        if not target_pages:
            raise ValueError("Страница с данными не найдена")

        full_text = ""
        for page_idx in target_pages:
            full_text += pdf.pages[page_idx].extract_text() + "\n"
        lines = full_text.split("\n")

        pattern = re.compile(
            r"(\d{1,3})\s+"
            r"([A-Za-z][A-Za-z\s,.'()'-]{2,50}?)\s+"
            r"(\.\d{3})"
        )

        countries = []
        seen = set()

        for line in lines:
            line = line.strip()
            if not line:
                continue
            for match in pattern.findall(line):
                rank_raw, country_raw, score_raw = match
                country_clean = country_raw.strip()
                if country_clean.lower() in ("rank country score", "country", ""):
                    continue
                if country_clean in seen:
                    continue
                try:
                    score = float(score_raw)
                    int(rank_raw)
                except ValueError:
                    continue
                if not (0 < score <= 1):
                    continue
                seen.add(country_clean)
                countries.append({
                    "country": country_clean,
                    "wps_score": score,
                })

        countries.sort(key=lambda x: x["wps_score"], reverse=True)
        return countries
    finally:
        pdf.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Парсер WPS Index PDF")
    parser.add_argument("pdf", help="Путь к PDF файлу")
    parser.add_argument("--output", "-o", default="wps_data.json")
    parser.add_argument("--reference-json", default=None)
    parser.add_argument("--manual-map", default=None)
    args = parser.parse_args()

    print("Шаг 1: Открываем PDF...")
    try:
        countries = parse_wps_pdf(args.pdf)
    except FileNotFoundError:
        print(f"  ✗ Файл не найден: {args.pdf}")
        sys.exit(1)
    except ValueError as e:
        print(f"  ✗ {e}")
        sys.exit(1)

    print(f"  Извлечено стран: {len(countries)}")

    ref = load_reference(args.reference_json, os.environ.get("API_COUNTRIES_URL"))
    enrich_country_entries(countries, ref=ref, manual_map_path=args.manual_map)

    result = {
        "source": "Women, Peace and Security Index 2025/26",
        "url": "https://giwps.georgetown.edu/the-index",
        "provider": "Georgetown Institute for Women, Peace and Security (GIWPS)",
        "total_countries": len(countries),
        "countries": countries,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"  ✓ Сохранено в файл: {args.output}")
    print("\nГотово!")


if __name__ == "__main__":
    main()
