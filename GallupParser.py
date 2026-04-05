"""
ШАГ 4 — Парсинг PDF: Gallup Global Safety Report 2025
======================================================

Что получаем (две таблицы):
  1. Law and Order Index      — составной индекс (0-100)
  2. Safe to Walk at Night %  — % людей чувствующих себя в безопасности

Запуск:
  pip install pdfplumber
  python GallupParser.py inputdata\Gallup_Global-Safety-Report-2025.pdf
"""

import pdfplumber
import json
import re
import sys
import argparse

# ─── Аргументы командной строки ──────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Парсер Gallup Global Safety Report PDF")
parser.add_argument("pdf", help="Путь к PDF файлу")
args = parser.parse_args()

PDF_FILE = args.pdf

# ─── Открываем PDF ───────────────────────────────────────────────────────────
print(f"Открываем: {PDF_FILE}")

try:
    pdf = pdfplumber.open(PDF_FILE)
except FileNotFoundError:
    print(f"  ✗ Файл не найден: {PDF_FILE}")
    sys.exit(1)

print(f"  Страниц: {len(pdf.pages)}")


# ─── Вспомогательная функция ─────────────────────────────────────────────────
SKIP_WORDS = {
    "Country", "Territory", "Country / Territory",
    "Law and Order", "Index Score", "Safe to Walk",
    "Alone at Night", "The Global Safety Report",
    "Copyright", "Gallup", "GlobalSafetyReport",
    "Methodology", "Analytics", "Chart", "Charts",
}


def extract_table(all_lines, value_re, value_cast):
    """
    Парсит таблицы где строки бывают двух форматов:
      'Tajikistan 97'                      — одна запись
      'Tajikistan 97 Indonesia 89'         — две записи (две колонки PDF)
      'Taiwan (Province of China) 89'      — страна со скобками

    Ключевая идея: разбиваем строку по границе "цифра → пробел → Заглавная",
    получаем отдельные части, каждую парсим независимо.
    """
    results = {}

    for line in all_lines:
        # Разбиваем строку с двумя колонками на отдельные части
        # '(?<=\d)' — позиция после цифры
        # '(?=[A-Z])' — позиция перед заглавной буквой
        parts = re.split(r'(?<=\d)\s+(?=[A-Z])', line)

        for part in parts:
            part = part.strip()

            # Паттерн: Название страны + значение + конец строки
            m = re.match(
                r'^([A-Za-z][A-Za-z\s()\',./-]+?)\s+(' + value_re + r')\s*$',
                part
            )
            if not m:
                continue

            country = m.group(1).strip()

            # Фильтры от мусора
            if len(country) < 3:
                continue
            if len(country) > 50:          # длинные строки = текст статьи
                continue
            if country in SKIP_WORDS:
                continue
            if re.search(r'\d', country):  # цифры в названии = мусор
                continue
            if len(country.split()) > 6:   # слишком много слов = предложение
                continue

            try:
                results[country] = value_cast(m.group(2))
            except ValueError:
                continue

    return results


# ─── Шаг 1: Собираем текст со всех страниц ───────────────────────────────────
print("\nШаг 1: Читаем текст страниц...")

all_lines = []
for page in pdf.pages:
    text = page.extract_text()
    if text:
        all_lines.extend(text.split("\n"))

pdf.close()
print(f"  Всего строк: {len(all_lines)}")


# ─── Шаг 2: Парсим Law and Order Index ───────────────────────────────────────
print("\nШаг 2: Парсим Law and Order Index...")

law_order = extract_table(
    all_lines,
    value_re=r'\d{2,3}',
    value_cast=int
)
# Допустимый диапазон индекса: 49-100
law_order = {k: v for k, v in law_order.items() if 49 <= v <= 100}

print(f"  Найдено стран: {len(law_order)}")


# ─── Шаг 3: Парсим Safe to Walk Alone at Night ───────────────────────────────
print("\nШаг 3: Парсим Safe to Walk Alone at Night %...")

safe_walk = extract_table(
    all_lines,
    value_re=r'\d{2,3}%',
    value_cast=lambda x: int(x.rstrip('%'))
)
# Допустимый диапазон: 0-100%
safe_walk = {k: v for k, v in safe_walk.items() if 0 <= v <= 100}

print(f"  Найдено стран: {len(safe_walk)}")


# ─── Шаг 4: Объединяем в один список ─────────────────────────────────────────
print("\nШаг 4: Объединяем данные...")

all_countries = set(law_order.keys()) | set(safe_walk.keys())

countries = []
for country in sorted(all_countries):
    entry = {"country": country}

    if country in law_order:
        entry["law_and_order_index"] = law_order[country]

    if country in safe_walk:
        entry["safe_walking_night_pct"] = safe_walk[country]

    countries.append(entry)

# Сортируем по law_and_order_index убыванию
countries.sort(key=lambda x: x.get("law_and_order_index", 0), reverse=True)

print(f"  Итого стран: {len(countries)}")
print("\n  Первые 5:")
for c in countries[:5]:
    print(f"    {c}")
print("\n  Последние 5:")
for c in countries[-5:]:
    print(f"    {c}")

# Показываем записи где есть только один из двух индексов
only_safe = [c for c in countries if "law_and_order_index" not in c]
only_law  = [c for c in countries if "safe_walking_night_pct" not in c]
if only_safe:
    print(f"\n  Только safe_walking (нет law_and_order): {[c['country'] for c in only_safe]}")
if only_law:
    print(f"\n  Только law_and_order (нет safe_walking): {[c['country'] for c in only_law]}")


# ─── Шаг 5: Сохраняем в JSON ─────────────────────────────────────────────────
print("\nШаг 5: Сохраняем в JSON...")

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

OUTPUT_FILE = "gallup_data.json"

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2, ensure_ascii=False)

print(f"  ✓ Сохранено: {OUTPUT_FILE}")
print(f"  ✓ Стран: {len(countries)}")
print("\nГотово!")