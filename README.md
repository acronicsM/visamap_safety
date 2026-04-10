# visamap_safety

Сбор и нормализация **показателей безопасности по странам** из нескольких открытых источников (веб и PDF) в единый JSON. Данные можно использовать для карт, визовых сервисов или аналитики.

## Что внутри

- **Парсеры** выгружают метрики в отдельные `*_data.json`.
- **`run_safety_pipeline.py`** последовательно запускает все парсеры, объединяет строки по **ISO 3166-1 alpha-2**, подтягивает имена из вашего API (или кэша), считает **единый индекс безопасности 0–100** (выше — безопаснее) и пишет итог в **`MERGED_OUTPUT_FILE`** (по умолчанию **`safety_merged.json`**) — **полный** или **минимальный** JSON в зависимости от флагов и `.env` (см. раздел ниже).
- Модуль **`safety_composite.py`** — нормализация метрик, взвешенное среднее с учётом пропусков и опциональные ручные оценки.

## Требования

- **Python 3.10+** (рекомендуется актуальная 3.x)
- Доступ в интернет для GPI и Numbeo (и при первом запросе справочника стран)
- PDF-файлы для Gallup и WPS (пути задаются в `.env`)

## Быстрый старт

```powershell
cd c:\visamap_safety
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

Отредактируйте `.env`: укажите **`API_COUNTRIES_URL`** (базовый URL API **без** суффикса `/countries/names` — он добавляется в коде) и пути к PDF (**`GALLUP_DATA_FILE`**, **`WOMEN_PEACE_SECURITY_INDEX_FILE`**). Положите PDF в каталог, например `input_data\`, как в примере.

Запуск пайплайна:

```powershell
python run_safety_pipeline.py
```

Минимальный вывод только с **`safety_final_score`** по каждому ISO2:

```powershell
python run_safety_pipeline.py --minimal-by-iso2-only
```

### Полный и минимальный `safety_merged.json`

| Режим | Содержимое файла |
|--------|------------------|
| **Полный** (по умолчанию) | **`meta`**, **`countries`**, **`by_iso2`** (метрики источников, нормализации, **`safety_composite_score`**, **`safety_final_score`**, флаги ручных правок и т.д.), **`unmatched`**. JSON с отступами (`indent=2`). |
| **Минимальный** | Только **`by_iso2`**: для каждого ключа ISO2 — объект с одним полем **`safety_final_score`** (число или **`null`**). Компактный JSON в одну строку, без отступов. |

Минимальный режим включается так:

- флаг **`--minimal-by-iso2-only`**, или
- переменная **`SAFETY_MERGED_MINIMAL_BY_ISO2_ONLY`** в **`.env`** со значением из списка **`1`**, **`true`**, **`yes`**, **`on`**, **`y`** (регистр не важен).

Чтобы **принудительно** получить **полный** файл даже при включённой переменной в `.env`, передайте **`--full-merged-out`** (этот флаг отключает и env, и **`--minimal-by-iso2-only`**).

Если ни флага минимального режима, ни «истинной» переменной нет, файл **не** сужается.

После успешного merge промежуточные `gallup_data.json`, `gpi_data.json`, `numbeo_data.json`, `wps_data.json` и кэш **`reference_countries.json`** в корне репозитория **удаляются** (см. ниже, как оставить их).

## Источники и скрипты

| Скрипт | Источник | Выход |
|--------|----------|--------|
| `GPIparser.py` | Wikipedia — Global Peace Index | `gpi_data.json` |
| `NumbeoParser.py` | Numbeo Crime & Safety | `numbeo_data.json` |
| `WPSparser.py` | PDF Women, Peace and Security Index | `wps_data.json` |
| `GallupParser.py` | PDF Gallup Global Safety Report | `gallup_data.json` |

Вспомогательные модули: **`country_reference.py`** (загрузка справочника стран и ручные алиасы), **`safety_composite.py`** (композитный индекс).

## Запуск парсеров по отдельности

Для сопоставления названий со странами парсерам нужен либо **`API_COUNTRIES_URL`** в `.env`, либо готовый JSON справочника:

```powershell
python GPIparser.py --reference-json reference_countries.json
python NumbeoParser.py --reference-json reference_countries.json
python WPSparser.py input_data\Women_Peace_Security_Index_2025.pdf --reference-json reference_countries.json
python GallupParser.py input_data\Gallup_Global-Safety-Report-2025.pdf --reference-json reference_countries.json
```

Опционально: **`--manual-map path\to\manual_country_map.json`** — алиасы и отображаемые имена (формат — в разделе **`manual_country_map.json`** ниже).

У большинства парсеров есть **`--output` / `-o`** для имени выходного файла.

## `run_safety_pipeline.py`: аргументы

| Флаг | Назначение |
|------|------------|
| `--reference-json` | Готовый кэш API вместо запроса по `API_COUNTRIES_URL` |
| `--reference-out` | Куда сохранить кэш при fetch (по умолчанию `reference_countries.json` в корне) |
| `--merged-out` | Итоговый файл (по умолчанию `safety_merged.json`) |
| `--manual-map` | Путь к `manual_country_map.json` |
| `--skip-fetch` | Не ходить в API; обязателен существующий `--reference-json` |
| `--keep-intermediate` | Не удалять промежуточные `*_data.json` и кэш справочника в корне репо |
| `--safety-config` | JSON весов и границ нормализации (см. env ниже) |
| `--safety-manual-scores` | JSON ручных поправок к индексу |
| `--minimal-by-iso2-only` | Минимальный вывод в **`--merged-out`** (см. таблицу «Полный и минимальный» выше). Дублируется переменной **`SAFETY_MERGED_MINIMAL_BY_ISO2_ONLY`** |
| `--full-merged-out` | Всегда **полный** merge в **`--merged-out`**; отключает минимальный режим и от **`.env`**, и от **`--minimal-by-iso2-only`** |

Поведение удаления промежуточных файлов можно задать переменной **`SAFETY_PIPELINE_DELETE_INTERMEDIATE`** (`1` / `0` и т.п., см. `.env.example`).

Пример с **`--minimal-by-iso2-only`** (идентификатор страны — ключ словаря **`by_iso2`**):

```json
{"by_iso2":{"DE":{"safety_final_score":72.5},"XX":{"safety_final_score":null}}}
```

## Переменные окружения (`.env`)

Основные (подробности в **`.env.example`**):

- **`API_COUNTRIES_URL`** — база вашего бэкенда со справочником стран.
- **`GALLUP_DATA_FILE`**, **`WOMEN_PEACE_SECURITY_INDEX_FILE`** — пути к PDF.
- **`REFERENCE_COUNTRIES_FILE`**, **`MERGED_OUTPUT_FILE`** — переопределение путей кэша и merge.
- **`SAFETY_MERGED_MINIMAL_BY_ISO2_ONLY`** — при истинном значении (**`1`**, **`true`**, **`yes`**, **`on`**, **`y`**) итоговый JSON в **`MERGED_OUTPUT_FILE`** сужается до **`by_iso2` → `safety_final_score`**. Полный файл при этом: **`--full-merged-out`**.
- **`SAFETY_PIPELINE_DELETE_INTERMEDIATE`** — после merge удалять промежуточные `*_data.json` и кэш справочника в корне репо (**`1`** по умолчанию; **`0`** / **`false`** и т.п. — оставить; флаг **`--keep-intermediate`** имеет приоритет).
- **`SAFETY_INDEX_CONFIG_FILE`**, **`SAFETY_MANUAL_SCORES_FILE`**, **`MANUAL_COUNTRY_MAP_FILE`** — опциональные пути к JSON конфигурации и ручным данным.

## `manual_country_map.json`

Файл **не обязателен**: если его нет или путь не задан, парсеры и merge используют только справочник API. Путь по умолчанию — **`manual_country_map.json`** в корне репозитория (или **`MANUAL_COUNTRY_MAP_FILE`** / **`--manual-map`**).

Назначение — помочь сопоставить названия из источников с **ISO2** и задать **отображаемое имя** страны в итоговых данных.

Структура:

| Поле | Тип | Описание |
|------|-----|----------|
| **`aliases_to_iso2`** | объект | Ключ — вариант названия страны (после нормализации: регистр, пробелы, Unicode NFKC). Значение — код **ISO2** из двух латинских букв. Используется при сопоставлении строк из PDF и сайтов со справочником. |
| **`iso2_to_name`** | объект | Ключ — **ISO2**, значение — строка для поля **`name`** в merge (если нужно переопределить или дополнить имя из API). |

Пример:

```json
{
  "aliases_to_iso2": {
    "ivory coast": "CI",
    "czechia": "CZ"
  },
  "iso2_to_name": {
    "UA": "Ukraine"
  }
}
```

Пустой шаблон (как в репозитории): оба ключа могут быть пустыми объектами `{}`.

## `safety_manual_scores.json`

Файл **опционален**. Если файла нет или в нём нет корректного массива **`entries`**, ручные оценки не применяются. Путь по умолчанию — **`safety_manual_scores.json`** в корне (или **`SAFETY_MANUAL_SCORES_FILE`** / **`--safety-manual-scores`**).

Назначение — задать для отдельных стран **ручной балл 0–100** и при необходимости **смешать** его с автоматическим композитом **`safety_composite_score`**.

Структура:

- **`entries`** — массив объектов. Каждый элемент:
  - **`iso2`** (обязательно) — двухбуквенный код страны.
  - **`score`** (обязательно) — число от **0** до **100** (выше — безопаснее); значения за пределами диапазона обрезаются.
  - **`weight`** (необязательно) — доля ручной оценки в итоге, **от 0 до 1** (не включая 0); по умолчанию **1.0**. Итог: `weight * score + (1 - weight) * safety_composite_score`. Если автоматического композита нет, в **`safety_final_score`** попадает только **`score`**.
  - **`note`** (необязательно) — произвольная строка-комментарий (в JSON не попадает в строки стран; в коде используется при разборе записи).

Пример:

```json
{
  "entries": [
    {
      "iso2": "DE",
      "score": 72.5,
      "weight": 0.3,
      "note": "Временная правка до обновления источников"
    }
  ]
}
```

В **полном** **`safety_merged.json`** для затронутых стран появляются поля **`safety_manual_applied`**, при смешивании — **`safety_manual_weight`**; итоговый балл — **`safety_final_score`** (см. `safety_composite.py`). В **`meta.safety_index`** указывается путь к файлу и **`manual_overrides_count`**. В **минимальном** режиме в файле остаётся только **`safety_final_score`** по ISO2 (ручные правки уже «вшиты» в это число).

## Формат `safety_merged.json` (кратко)

- **`meta`** — время генерации и сведения об источниках.
- **`countries`** — список записей по странам с плоскими полями метрик (`gpi_score`, `numbeo_safety_index`, `gallup_law_and_order_index`, и т.д.) и полями композитного индекса (см. `safety_composite.py`).
- **`by_iso2`** — словарь `ISO2 → запись`.
- **`unmatched`** — строки, для которых не удалось надёжно вывести `iso2`.

В **минимальном** режиме (**`--minimal-by-iso2-only`** или **`SAFETY_MERGED_MINIMAL_BY_ISO2_ONLY`**) в файле только **`by_iso2`**, внутри — по одному полю **`safety_final_score`** на страну (см. пример JSON в разделе аргументов).

## Ограничения

- Вёрстка Wikipedia и Numbeo может измениться — HTML-парсеры перестанут извлекать таблицы.
- PDF зависят от года издания и вёрстки страниц.
- Справочник стран и ручной маппинг критичны для качества сопоставления названий.

## Зависимости

См. **`requirements.txt`**: `requests`, `beautifulsoup4`, `pdfplumber`, `python-dotenv` и транзитивные пакеты.

## Дополнительные заметки

В каталоге **`memory-bank/`** лежат внутренние заметки по проекту и пайплайну; для пользователей репозитория достаточно этого README и `.env.example`.
