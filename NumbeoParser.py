"""
ШАГ 2 — Парсинг таблицы Crime & Safety Index с Numbeo
=======================================================

Что получаем:
  - Crime Index  (чем выше — тем опаснее)
  - Safety Index (чем выше — тем безопаснее)

Таблица доступна прямо в HTML — JavaScript не нужен.
"""

import requests
from bs4 import BeautifulSoup
import json
import re

# ─── ШАГ 1: Скачать страницу ────────────────────────────────────────────────
print("Шаг 1: Скачиваем страницу Numbeo...")

URL = "https://www.numbeo.com/crime/rankings_by_country.jsp"

# Numbeo более строгий к ботам — добавляем более полные заголовки
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

try:
    response = requests.get(URL, headers=headers, timeout=15)
    response.raise_for_status()
except requests.exceptions.ConnectionError:
    print("  ✗ Нет подключения к интернету")
    exit(1)
except requests.exceptions.HTTPError as e:
    print(f"  ✗ Ошибка HTTP: {e}")
    exit(1)

print(f"  Статус: {response.status_code}")
print(f"  Размер HTML: {len(response.text)} символов")


# ─── ШАГ 2: Найти таблицу ───────────────────────────────────────────────────
print("\nШаг 2: Ищем таблицу...")

soup = BeautifulSoup(response.text, "html.parser")

# На Numbeo таблица имеет id="t2" — это надёжнее чем искать по классу
# id уникален на странице, поэтому сразу найдём нужную
table = soup.find("table", {"id": "t2"})

if not table:
    print("  ✗ Таблица с id='t2' не найдена!")
    exit(1)

print("  ✓ Таблица найдена (id='t2')")


# ─── ШАГ 3: Извлечь данные ──────────────────────────────────────────────────
print("\nШаг 3: Извлекаем данные...")

# Структура таблицы Numbeo:
# <thead> содержит строку заголовков
# <tbody> содержит строки с данными

rows = table.find("tbody").find_all("tr")
print(f"  Строк в таблице: {len(rows)}")

countries = []

for row in rows:
    cells = row.find_all("td")

    # В таблице 4 колонки: Rank | Country | Crime Index | Safety Index
    if len(cells) < 3:
        continue

    country_raw  = cells[1].get_text(strip=True)
    crime_raw    = cells[2].get_text(strip=True)
    safety_raw   = cells[3].get_text(strip=True) if len(cells) > 3 else None

    # Очищаем название страны от возможных сносок
    country_clean = re.sub(r"\[.*?\]", "", country_raw).strip()
    if not country_clean:
        continue

    # Конвертируем индексы в числа
    try:
        crime_index = float(crime_raw)
    except ValueError:
        crime_index = None

    try:
        safety_index = float(safety_raw) if safety_raw else None
    except ValueError:
        safety_index = None

    countries.append({
        "country":      country_clean,
        "crime_index":  crime_index,
        "safety_index": safety_index,
    })

print(f"  Извлечено стран: {len(countries)}")

# Покажем первые 5
print("\n  Первые 5 записей:")
for c in countries[:5]:
    print(f"    {c}")


# ─── ШАГ 4: Сохранить в JSON ─────────────────────────────────────────────────
print("\nШаг 4: Сохраняем в JSON...")

result = {
    "source": "Numbeo — Crime & Safety Index by Country 2026",
    "url": URL,
    "total_countries": len(countries),
    "countries": countries
}

OUTPUT_FILE = "numbeo_data.json"

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2, ensure_ascii=False)

print(f"  ✓ Сохранено в файл: {OUTPUT_FILE}")
print(f"  ✓ Всего стран: {len(countries)}")
print("\nГотово!")