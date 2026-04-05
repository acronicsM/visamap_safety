"""
ШАГ 3 — Парсинг PDF: Women, Peace and Security Index 2025/26
=============================================================

Что получаем:
  - Страна
  - WPS Score (индекс безопасности женщин, от 0 до 1)

Библиотека: pdfplumber
  - Умеет извлекать текст с учётом расположения на странице
  - Лучше чем pypdf для сложных макетов и колонок

Установка:
  pip install pdfplumber
"""

import pdfplumber   # читает PDF и извлекает текст/таблицы
import json
import re
import sys
import argparse

# ─── Аргументы командной строки ──────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Парсер WPS Index PDF")
parser.add_argument("pdf", help="Путь к PDF файлу")
args = parser.parse_args()

PDF_FILE = args.pdf

# ─── ШАГ 1: Открыть PDF и посмотреть что внутри ─────────────────────────────
print("Шаг 1: Открываем PDF...")

try:
    pdf = pdfplumber.open(PDF_FILE)
except FileNotFoundError:
    print(f"  ✗ Файл не найден: {PDF_FILE}")
    print("  Положи PDF рядом со скриптом!")
    sys.exit(1)

print(f"  ✓ Файл открыт")
print(f"  Страниц в PDF: {len(pdf.pages)}")


# ─── ШАГ 2: Найти нужную страницу ───────────────────────────────────────────
print("\nШаг 2: Ищем страницу с данными...")

# В этом PDF данные о странах есть на двух местах:
#   - Страница 2: список по рангу (но в 3 колонки — сложнее парсить)
#   - Последняя страница (27): алфавитный список — тоже 3 колонки, но чище
#
# Стратегия: перебираем все страницы, ищем ту где есть "Afghanistan" и числа

target_pages = []

for i, page in enumerate(pdf.pages):
    text = page.extract_text()
    if text and "Afghanistan" in text and ".279" in text:
        target_pages.append(i)
        print(f"  Найдена подходящая страница: #{i + 1}")

if not target_pages:
    print("  ✗ Страница с данными не найдена!")
    sys.exit(1)


# ─── ШАГ 3: Извлечь текст со страниц ────────────────────────────────────────
print("\nШаг 3: Извлекаем текст...")

# Соберём текст со всех найденных страниц
full_text = ""
for page_idx in target_pages:
    page_text = pdf.pages[page_idx].extract_text()
    full_text += page_text + "\n"

# Посмотрим первые строки для понимания структуры
print("  Первые 20 строк текста:")
lines = full_text.split("\n")
for line in lines[:20]:
    print(f"    '{line}'")


# ─── ШАГ 4: Разобрать строки ─────────────────────────────────────────────────
print("\nШаг 4: Парсим строки...")

# Структура строк на странице выглядит примерно так:
#   "181 Afghanistan .279"
#   "68 Albania .731"
#   "1 Denmark .939"
#
# Регулярное выражение для поиска таких строк:
# (\d+)        — число (ранг)
# \s+          — пробел(ы)
# ([A-Za-z .,'()-]+?) — название страны (буквы, пробелы, спецсимволы)
# \s+          — пробел(ы)
# (\.\d+)      — score вида .279

# Паттерн который ищет: РАНГ  СТРАНА  SCORE
pattern = re.compile(
    r"(\d{1,3})\s+"           # ранг: 1-3 цифры
    r"([A-Za-z][A-Za-z\s,.'()'-]{2,50}?)\s+"  # страна
    r"(\.\d{3})"              # score: .xxx
)

countries = []
seen = set()  # чтобы не добавлять дубликаты (страница может дублироваться)

for line in lines:
    line = line.strip()
    if not line:
        continue

    # Ищем все совпадения в строке
    matches = pattern.findall(line)

    for match in matches:
        rank_raw, country_raw, score_raw = match

        country_clean = country_raw.strip()
        # Убираем мусор в конце названия
        country_clean = re.sub(r"\s+$", "", country_clean)

        # Пропускаем заголовки и мусор
        if country_clean.lower() in ("rank country score", "country", ""):
            continue

        # Пропускаем дубликаты
        if country_clean in seen:
            continue

        try:
            score = float(score_raw)
            rank = int(rank_raw)
        except ValueError:
            continue

        # Проверка что score в разумных пределах (0-1)
        if not (0 < score <= 1):
            continue

        seen.add(country_clean)
        countries.append({
            "country": country_clean,
            "wps_score": score,
        })

# Сортируем по score убыванию (самые безопасные — первые)
countries.sort(key=lambda x: x["wps_score"], reverse=True)

print(f"  Извлечено стран: {len(countries)}")
print("\n  Первые 5 записей:")
for c in countries[:5]:
    print(f"    {c}")
print("\n  Последние 5 записей:")
for c in countries[-5:]:
    print(f"    {c}")

pdf.close()


# ─── ШАГ 5: Сохранить в JSON ─────────────────────────────────────────────────
print("\nШаг 5: Сохраняем в JSON...")

result = {
    "source": "Women, Peace and Security Index 2025/26",
    "url": "https://giwps.georgetown.edu/the-index",
    "provider": "Georgetown Institute for Women, Peace and Security (GIWPS)",
    "total_countries": len(countries),
    "countries": countries
}

OUTPUT_FILE = "wps_data.json"

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2, ensure_ascii=False)

print(f"  ✓ Сохранено в файл: {OUTPUT_FILE}")
print(f"  ✓ Всего стран: {len(countries)}")
print("\nГотово!")