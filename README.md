# visamap_safety

Сбор и нормализация **показателей безопасности по странам** из нескольких открытых источников (веб и PDF) в единый JSON. Данные можно использовать для карт, визовых сервисов или аналитики.

## Что внутри

- **Парсеры** выгружают метрики в отдельные `*_data.json`.
- **`run_safety_pipeline.py`** последовательно запускает все парсеры, объединяет строки по **ISO 3166-1 alpha-2**, подтягивает имена из **локального JSON справочника** (`REFERENCE_COUNTRIES_FILE`), считает **единый индекс безопасности 0–100** (выше — безопаснее) и пишет **полный** итог в **`MERGED_OUTPUT_FILE`**. После merge по умолчанию **удаляет** промежуточные `gallup_data.json`, `gpi_data.json`, `numbeo_data.json`, `wps_data.json` в корне репо; файл справочника **не** удаляется. Сохранить промежуточные файлы: **`--keep-intermediate`**.
- **`sync_safety_reference.py`** — полный цикл: проверка справочника → PUT минимального `by_iso2` → GET в **`REFERENCE_COUNTRIES_FILE`**. Режим первого запуска: **`--download-reference-only`** (только GET по **`REFERENCE_COUNTRIES_DOWNLOAD_URL`**).
- **`analyze_safety_merged.py`** — отчёт по `unmatched` и разбросу нормализованных индексов в UTF-8 файл (рядом с merged по умолчанию: **`safety_merged_analysis.txt`**).
- Модуль **`safety_composite.py`** — нормализация метрик, взвешенное среднее с учётом пропусков и опциональные ручные оценки.

## Требования

- **Python 3.10+** (рекомендуется актуальная 3.x)
- Доступ в интернет для GPI и Numbeo
- Локальный файл справочника стран для пайплайна (см. **`REFERENCE_COUNTRIES_FILE`**)
- PDF-файлы для Gallup и WPS (пути задаются в `.env`)

## Быстрый старт

```powershell
cd c:\visamap_safety
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

Отредактируйте `.env`: укажите **`REFERENCE_COUNTRIES_FILE`** (путь к JSON справочника, файл должен существовать), пути к PDF (**`GALLUP_DATA_FILE`**, **`WOMEN_PEACE_SECURITY_INDEX_FILE`**). Положите PDF в каталог, например `input_data\`, как в примере.

Запуск пайплайна:

```powershell
python run_safety_pipeline.py
```

Итоговый **`safety_merged.json`** всегда **полный**: **`meta`**, **`countries`**, **`by_iso2`** (метрики источников, нормализации, **`safety_composite_score`**, **`safety_final_score`**, флаги ручных правок и т.д.), **`unmatched`**. JSON с отступами (`indent=2`).

### Типовой цикл (пайплайн → анализ → sync)

**Первый запуск** (ещё нет локального справочника):

1. **`python sync_safety_reference.py --download-reference-only`** — только загрузка: GET **`REFERENCE_COUNTRIES_DOWNLOAD_URL`** → **`REFERENCE_COUNTRIES_FILE`** (нужны оба в `.env`).
2. **`python run_safety_pipeline.py`** — парсеры, merge, по умолчанию удаление четырёх `*_data.json`.
3. **`python analyze_safety_merged.py`**
4. **`python sync_safety_reference.py`** — PUT баллов из merged, затем снова GET справочника.

**Повторный запуск** (справочник уже есть):

2. **`python run_safety_pipeline.py`**
3. **`python analyze_safety_merged.py`**
4. **`python sync_safety_reference.py`**

### Отправка на сервер и обновление справочника

Пайплайн **`run_safety_pipeline.py`** только собирает полный **`safety_merged.json`** на диск и при необходимости чистит промежуточные `*_data.json`. Отправка на API — отдельно:

- **`python sync_safety_reference.py --download-reference-only`** — только **`REFERENCE_COUNTRIES_DOWNLOAD_URL`** → **`REFERENCE_COUNTRIES_FILE`** (PUT и merged не используются).
- **`python sync_safety_reference.py`** — PUT **`{"by_iso2": { … "safety_final_score" … } }`** из **`MERGED_OUTPUT_FILE`**, затем GET и перезапись **`REFERENCE_COUNTRIES_FILE`**. Перед запуском файл справочника должен существовать. Опция **`--dry-run`** — без HTTP.

### Анализ merged

```powershell
python analyze_safety_merged.py
```

В консоль выводится одна строка с путём к отчёту и счётчиками; полный текст — в файле (по умолчанию **`safety_merged_analysis.txt`** рядом с **`MERGED_OUTPUT_FILE`**, либо **`SAFETY_MERGED_ANALYSIS_OUT`** / **`--out`**). **`--stdout`** — продублировать отчёт в терминал.

Вход: **`--merged`** или **`MERGED_OUTPUT_FILE`**. Порог разброса: **`SAFETY_NORM_SPREAD_THRESHOLD_PCT`**. Размах — **max − min** по всем нормам источников, без отсечения выбросов.

## Источники и скрипты

| Скрипт | Источник | Выход |
|--------|----------|--------|
| `GPIparser.py` | Wikipedia — Global Peace Index | `gpi_data.json` |
| `NumbeoParser.py` | Numbeo Crime & Safety | `numbeo_data.json` |
| `WPSparser.py` | PDF Women, Peace and Security Index | `wps_data.json` |
| `GallupParser.py` | PDF Gallup Global Safety Report | `gallup_data.json` |

Вспомогательные модули: **`country_reference.py`** (справочник стран и ручные алиасы), **`safety_composite.py`** (композитный индекс).

## Запуск парсеров по отдельности

Для сопоставления названий со странами можно передать **`--reference-json`** к локальному JSON или задать **`API_COUNTRIES_URL`** в `.env` (тогда справочник запрашивается по сети — см. `country_reference.load_reference`).

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
| `--reference-json` | Путь к JSON справочника (перекрывает **`REFERENCE_COUNTRIES_FILE`**) |
| `--merged-out` | Итоговый файл (по умолчанию `safety_merged.json`) |
| `--manual-map` | Путь к `manual_country_map.json` |
| `--safety-config` | JSON весов и границ нормализации (см. env ниже) |
| `--safety-manual-scores` | JSON ручных поправок к индексу |
| `--keep-intermediate` | Не удалять после merge промежуточные `*_data.json` парсеров |

## Переменные окружения (`.env`)

Основные (подробности в **`.env.example`**):

- **`REFERENCE_COUNTRIES_FILE`** — локальный JSON справочника; **обязателен** для **`run_safety_pipeline.py`** (файл должен существовать).
- **`MERGED_OUTPUT_FILE`** — путь к итоговому merge (и вход для **`sync_safety_reference.py`** / **`analyze_safety_merged.py`**).
- **`GALLUP_DATA_FILE`**, **`WOMEN_PEACE_SECURITY_INDEX_FILE`** — пути к PDF.
- **`API_COUNTRIES_URL`** — для **одиночных** парсеров без **`--reference-json`** (загрузка справочника с API).
- **`WIKIPEDIA_USER_AGENT`** — для **`GPIparser.py`**: полная строка User-Agent с контактом (URL репозитория или email) по [политике Wikimedia](https://meta.wikimedia.org/wiki/User-Agent_policy); иначе часто отвечают **403** / rate limit.
- **`REFERENCE_COUNTRIES_DOWNLOAD_URL`** — полный URL GET: полный режим **`sync_safety_reference.py`** (после PUT) или только **`sync_safety_reference.py --download-reference-only`**.
- **`SAFETY_FINAL_SCORES_PUT_URL`**, **`SAFETY_FINAL_SCORES_X_API_KEY`** — PUT итоговых баллов для **`sync_safety_reference.py`** (минимальный **`by_iso2`**).
- **`SAFETY_MERGED_ANALYSIS_OUT`** — путь к файлу отчёта **`analyze_safety_merged.py`** (иначе **`<имя merged без расширения>_analysis.txt`** рядом с merged).
- **`SAFETY_NORM_SPREAD_THRESHOLD_PCT`** — порог для **`analyze_safety_merged.py`** (доля шкалы 0–100, по умолчанию **`0.1`**).
- **`SAFETY_INDEX_CONFIG_FILE`**, **`SAFETY_MANUAL_SCORES_FILE`**, **`MANUAL_COUNTRY_MAP_FILE`** — опциональные пути к JSON конфигурации и ручным данным.

## `manual_country_map.json`

Файл **не обязателен**: если его нет или путь не задан, парсеры и merge используют только справочник из **`--reference-json`** / **`REFERENCE_COUNTRIES_FILE`**. Путь по умолчанию — **`manual_country_map.json`** в корне репозитория (или **`MANUAL_COUNTRY_MAP_FILE`** / **`--manual-map`**).

Назначение — помочь сопоставить названия из источников с **ISO2** и задать **отображаемое имя** страны в итоговых данных.

Структура:

| Поле | Тип | Описание |
|------|-----|----------|
| **`aliases_to_iso2`** | объект | Ключ — вариант названия страны (после нормализации: регистр, пробелы, Unicode NFKC). Значение — код **ISO2** из двух латинских букв. Используется при сопоставлении строк из PDF и сайтов со справочником. |
| **`iso2_to_name`** | объект | Ключ — **ISO2**, значение — строка для поля **`name`** в merge (если нужно переопределить или дополнить имя из справочника). |

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

В **`safety_merged.json`** для затронутых стран появляются поля **`safety_manual_applied`**, при смешивании — **`safety_manual_weight`**; итоговый балл — **`safety_final_score`** (см. `safety_composite.py`). В **`meta.safety_index`** указывается путь к файлу и **`manual_overrides_count`**.

## Формат `safety_merged.json` (кратко)

- **`meta`** — время генерации и сведения об источниках.
- **`countries`** — список записей по странам с плоскими полями метрик (`gpi_score`, `numbeo_safety_index`, `gallup_law_and_order_index`, и т.д.), нормализованными **`safety_norm_*`**, медианой по ним **`safety_norm_median`**, композитом и **`safety_final_score`** (см. `safety_composite.py`).
- **`by_iso2`** — словарь `ISO2 → запись`.
- **`unmatched`** — строки, для которых не удалось надёжно вывести `iso2`.

## Ограничения

- Вёрстка Wikipedia и Numbeo может измениться — HTML-парсеры перестанут извлекать таблицы.
- PDF зависят от года издания и вёрстки страниц.
- Справочник стран и ручной маппинг критичны для качества сопоставления названий.

## Зависимости

См. **`requirements.txt`**: `requests`, `beautifulsoup4`, `pdfplumber`, `python-dotenv` и транзитивные пакеты.

## Дополнительные заметки

В каталоге **`memory-bank/`** лежат внутренние заметки по проекту и пайплайну (в т.ч. **`memory-bank/pipeline.md`**); для пользователей репозитория достаточно этого README и `.env.example`.
