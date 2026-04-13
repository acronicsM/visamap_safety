"""
Режимы:
  • По умолчанию: проверка справочника → PUT минимального by_iso2 → GET нового справочника.
  • --download-reference-only: только GET по REFERENCE_COUNTRIES_DOWNLOAD_URL в REFERENCE_COUNTRIES_FILE
    (первый запуск, без merged и без PUT).

Переменные окружения (.env):
  REFERENCE_COUNTRIES_FILE — путь к файлу справочника (для полного режима должен существовать до PUT).
  MERGED_OUTPUT_FILE — источник safety_merged.json (по умолчанию safety_merged.json).
  SAFETY_FINAL_SCORES_PUT_URL — полный URL для PUT JSON {"by_iso2": {...}}.
  SAFETY_FINAL_SCORES_X_API_KEY — заголовок X-Api-Key.
  REFERENCE_COUNTRIES_DOWNLOAD_URL — полный URL GET для загрузки справочника.
  SAFETY_PUT_SCORE_SOURCE — какое поле merged класть в safety_final_score при PUT:
    final (по умолчанию) — safety_final_score;
    composite / mean — safety_composite_score (взвешенное среднее по нормам);
    median — safety_norm_median.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent


def _reference_path() -> Path:
    env_path = os.environ.get("REFERENCE_COUNTRIES_FILE")
    if env_path:
        return Path(env_path)
    return ROOT / "reference_countries.json"


def _merged_path() -> Path:
    env_path = os.environ.get("MERGED_OUTPUT_FILE")
    if env_path:
        return Path(env_path)
    return ROOT / "safety_merged.json"


def _put_score_source() -> tuple[str, str]:
    """Возвращает (режим, ключ в строке merged)."""
    raw = (os.environ.get("SAFETY_PUT_SCORE_SOURCE") or "final").strip().lower()
    if raw in ("median", "norm_median", "safety_norm_median"):
        return "median", "safety_norm_median"
    if raw in ("composite", "mean", "weighted", "safety_composite_score"):
        return "composite", "safety_composite_score"
    return "final", "safety_final_score"


def _score_for_put(row: dict, field_key: str):
    if not isinstance(row, dict):
        return None
    return row.get(field_key)


def _put_minimal_scores(payload: dict) -> None:
    url = (os.environ.get("SAFETY_FINAL_SCORES_PUT_URL") or "").strip()
    api_key = (os.environ.get("SAFETY_FINAL_SCORES_X_API_KEY") or "").strip()
    if not url:
        print("Задайте SAFETY_FINAL_SCORES_PUT_URL в .env", file=sys.stderr)
        sys.exit(1)
    if not api_key:
        print("Задайте SAFETY_FINAL_SCORES_X_API_KEY в .env", file=sys.stderr)
        sys.exit(1)
    headers = {
        "Content-Type": "application/json",
        "X-Api-Key": api_key,
    }
    try:
        r = requests.put(url, json=payload, headers=headers, timeout=120)
    except requests.RequestException as e:
        print(f"Ошибка запроса PUT: {e}", file=sys.stderr)
        sys.exit(1)
    if r.status_code >= 400:
        body_preview = (r.text or "")[:2000]
        print(
            f"PUT завершился с кодом {r.status_code}. Тело ответа:\n{body_preview}",
            file=sys.stderr,
        )
        sys.exit(1)
    preview = (r.text or "").strip()
    if preview:
        short = preview if len(preview) <= 500 else preview[:500] + "…"
        print(f"✓ Отправлено PUT {url} → HTTP {r.status_code}\n  Ответ: {short}")
    else:
        print(f"✓ Отправлено PUT {url} → HTTP {r.status_code}")


def _download_reference(out_path: Path) -> None:
    url = (os.environ.get("REFERENCE_COUNTRIES_DOWNLOAD_URL") or "").strip()
    if not url:
        print("Задайте REFERENCE_COUNTRIES_DOWNLOAD_URL в .env", file=sys.stderr)
        sys.exit(1)
    try:
        r = requests.get(url, timeout=120)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"Ошибка запроса GET справочника: {e}", file=sys.stderr)
        sys.exit(1)
    try:
        data = r.json()
    except json.JSONDecodeError as e:
        print(f"Ответ не JSON: {e}", file=sys.stderr)
        sys.exit(1)
    if not isinstance(data, dict):
        print("Ожидался JSON-объект { iso2: { ... } }", file=sys.stderr)
        sys.exit(1)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✓ Справочник сохранён: {out_path.resolve()}")


def main() -> None:
    ap = argparse.ArgumentParser(description="PUT safety scores + GET reference, или только загрузка справочника")
    ap.add_argument(
        "--download-reference-only",
        action="store_true",
        help="Только скачать справочник: GET REFERENCE_COUNTRIES_DOWNLOAD_URL → REFERENCE_COUNTRIES_FILE (без PUT и без merged)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Без HTTP: в полном режиме проверить reference/merged; с --download-reference-only — только проверить env",
    )
    args = ap.parse_args()

    load_dotenv(ROOT / ".env")

    ref_path = _reference_path()
    merged_path = _merged_path()

    if args.download_reference_only:
        url = (os.environ.get("REFERENCE_COUNTRIES_DOWNLOAD_URL") or "").strip()
        if args.dry_run:
            print(
                f"dry-run: --download-reference-only -> {ref_path.resolve()}\n"
                f"  REFERENCE_COUNTRIES_DOWNLOAD_URL: {'задан' if url else 'НЕ задан'}"
            )
            return
        _download_reference(ref_path)
        return

    if not ref_path.is_file():
        print(
            f"Файл справочника стран не найден: {ref_path.resolve()}\n"
            "Нужен существующий REFERENCE_COUNTRIES_FILE перед отправкой.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not merged_path.is_file():
        print(
            f"Файл merged не найден: {merged_path.resolve()}",
            file=sys.stderr,
        )
        sys.exit(1)

    with open(merged_path, encoding="utf-8") as f:
        merged = json.load(f)

    by_full = merged.get("by_iso2")
    if not isinstance(by_full, dict):
        print("В merged нет объекта by_iso2", file=sys.stderr)
        sys.exit(1)

    src_label, field_key = _put_score_source()
    payload = {
        "by_iso2": {
            iso: {"safety_final_score": _score_for_put(row, field_key)}
            for iso, row in sorted(by_full.items())
        },
    }

    if args.dry_run:
        n = len(payload["by_iso2"])
        print(f"dry-run: reference OK {ref_path.resolve()}")
        print(f"dry-run: merged OK {merged_path.resolve()}, записей by_iso2: {n}")
        print(
            f"dry-run: SAFETY_PUT_SCORE_SOURCE={src_label!r} → JSON safety_final_score из merged[{field_key!r}]"
        )
        return

    print(
        f"PUT: SAFETY_PUT_SCORE_SOURCE={src_label!r} → поле JSON safety_final_score из merged[{field_key!r}]"
    )
    _put_minimal_scores(payload)
    _download_reference(ref_path)


if __name__ == "__main__":
    main()
