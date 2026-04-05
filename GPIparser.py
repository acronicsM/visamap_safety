"""
ШАГ 1 — Парсинг таблицы Global Peace Index с Wikipedia
========================================================

Что мы делаем:
  1. Скачиваем HTML страницы с помощью библиотеки `requests`
  2. Разбираем HTML с помощью `BeautifulSoup`
  3. Находим нужную таблицу
  4. Извлекаем строки и столбцы
  5. Сохраняем в JSON файл

Установка зависимостей (один раз в терминале):
  pip install requests beautifulsoup4
"""

# ─── ИМПОРТЫ ────────────────────────────────────────────────────────────────
import requests          # скачивает HTML страницы из интернета
from bs4 import BeautifulSoup  # разбирает (парсит) HTML
import json              # сохраняет данные в JSON файл
import re                # регулярные выражения — для очистки текста


# ─── ШАГ 1: Скачать HTML страницы ───────────────────────────────────────────
print("Шаг 1: Скачиваем страницу Wikipedia...")

URL = "https://en.wikipedia.org/wiki/Global_Peace_Index"

# Заголовки нужны, чтобы сайт не заблокировал нас как "бота"
# Wikipedia обычно не блокирует, но это хорошая привычка
headers = {
    "User-Agent": "Mozilla/5.0 (compatible; learning-parser/1.0)"
}

try:
    response = requests.get(URL, headers=headers, timeout=15)
    response.raise_for_status()  # выбросит ошибку если код не 200
except requests.exceptions.ConnectionError:
    print("  ✗ Ошибка: нет подключения к интернету")
    exit(1)
except requests.exceptions.Timeout:
    print("  ✗ Ошибка: сайт не ответил за 15 секунд")
    exit(1)
except requests.exceptions.HTTPError as e:
    print(f"  ✗ Ошибка HTTP: {e}")
    exit(1)

# Проверяем что страница скачалась успешно (код 200 = OK)
print(f"  Статус ответа: {response.status_code}")  # должно быть 200
print(f"  Размер HTML: {len(response.text)} символов")


# ─── ШАГ 2: Разобрать HTML ───────────────────────────────────────────────────
print("\nШаг 2: Разбираем HTML...")

# BeautifulSoup превращает сырой HTML-текст в удобный объект
# "html.parser" — встроенный Python парсер, не нужно ничего ставить
soup = BeautifulSoup(response.text, "html.parser")

# Для интереса: найдём заголовок страницы
title = soup.find("h1")
print(f"  Заголовок страницы: {title.get_text()}")


# ─── ШАГ 3: Найти нужную таблицу ────────────────────────────────────────────
print("\nШаг 3: Ищем таблицу с рейтингом стран...")

# На странице Wikipedia таблицы имеют класс "wikitable"
# Найдём ВСЕ такие таблицы
all_tables = soup.find_all("table", class_="wikitable")
print(f"  Найдено таблиц с классом 'wikitable': {len(all_tables)}")

# Нам нужна таблица "2025 Global Peace Index Ranking"
# Ищем ту, где в заголовках есть слова "Rank", "Country", "Score"
target_table = None

for i, table in enumerate(all_tables):
    # Берём текст всех заголовков таблицы (<th>)
    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    headers_text = " ".join(headers).lower()
    
    print(f"  Таблица #{i}: заголовки = {headers[:5]}")  # первые 5 заголовков
    
    # Нужная таблица содержит "rank", "country" и "score"
    if "rank" in headers_text and "country" in headers_text and "score" in headers_text:
        target_table = table
        print(f"  ✓ Нашли нужную таблицу! (таблица #{i})")
        break

if target_table is None:
    print("  ✗ Таблица не найдена!")
    exit(1)


# ─── ШАГ 4: Извлечь данные из строк таблицы ─────────────────────────────────
print("\nШаг 4: Извлекаем данные из строк таблицы...")

# HTML таблица выглядит так:
# <table>
#   <tr>  ← строка (row)
#     <th>Rank</th>  <th>Country</th>  <th>Score</th>   ← заголовки
#   </tr>
#   <tr>
#     <td>1</td>  <td>Iceland</td>  <td>1.095</td>       ← данные
#   </tr>
# ...

# Получаем ВСЕ строки таблицы
all_rows = target_table.find_all("tr")
print(f"  Всего строк в таблице: {len(all_rows)}")

# Первая строка — это заголовки, пропускаем её
# Остальные — данные стран
countries = []

for row in all_rows[1:]:  # [1:] = пропускаем первую строку
    
    # В каждой строке ищем ячейки: <td> (данные) или <th> (заголовок)
    cells = row.find_all(["td", "th"])
    
    # Если меньше 3 ячеек — это не строка с данными, пропускаем
    if len(cells) < 3:
        continue
    
    # Извлекаем текст из каждой ячейки и очищаем от пробелов
    values = [cell.get_text(strip=True) for cell in cells]
    
    # values[0] = ранг, values[1] = страна, values[2] = score
    country_raw = values[1]
    score_raw  = values[2]

    # Очищаем название страны от сносок вида [1] [2] [note 1]
    # re.sub заменяет найденное на пустую строку
    country_clean = re.sub(r"\[.*?\]", "", country_raw).strip()
    
    # Пропускаем строки где "страна" — это снова заголовок
    if country_clean.lower() in ("country", "nation", ""):
        continue
    
    try:
        score = float(score_raw)
    except ValueError:
        score = None
    
    # Собираем словарь для одной страны
    country_data = {
        "country": country_clean,
        "score":   score
    }
    
    countries.append(country_data)

print(f"  Извлечено стран: {len(countries)}")

# Покажем первые 5 для проверки
print("\n  Первые 5 записей:")
for c in countries[:5]:
    print(f"    {c}")


# ─── ШАГ 5: Сохранить в JSON ─────────────────────────────────────────────────
print("\nШаг 5: Сохраняем в JSON...")

# Финальная структура данных
result = {
    "source": "Wikipedia — Global Peace Index 2025",
    "url": URL,
    "total_countries": len(countries),
    "countries": countries
}

OUTPUT_FILE = "gpi_data.json"

# json.dump записывает Python-словарь в файл
# indent=2 — красивое форматирование с отступами
# ensure_ascii=False — чтобы не ломались не-латинские буквы
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2, ensure_ascii=False)

print(f"  ✓ Сохранено в файл: {OUTPUT_FILE}")
print(f"  ✓ Всего стран: {len(countries)}")
print("\nГотово!")