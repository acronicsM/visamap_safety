"""
Единый запуск: четыре парсера, объединённый JSON и единый индекс безопасности.

Требует локальный файл справочника стран: REFERENCE_COUNTRIES_FILE в .env (по умолчанию
reference_countries.json в корне репо) или явный --reference-json.

PDF-пути: GALLUP_DATA_FILE, WOMEN_PEACE_SECURITY_INDEX_FILE.

Итог всегда полный merged (meta, countries, by_iso2, unmatched) в MERGED_OUTPUT_FILE.
Числовые метрики из парсеров, равные 0, в merged не попадают (как отсутствующие).
После успешного merge по умолчанию удаляются промежуточные gallup/gpi/numbeo/wps *_data.json
в корне репо; справочник REFERENCE_COUNTRIES_FILE не трогаем. Отмена удаления: --keep-intermediate.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from country_reference import (
    effective_manual_map_path,
    load_manual_map,
    display_name_for_iso2,
    load_reference_from_path,
)
from safety_composite import enrich_merged_with_safety_index

ROOT = Path(__file__).resolve().parent

SOURCE_OUTPUTS = {
    "gallup": "gallup_data.json",
    "gpi": "gpi_data.json",
    "numbeo": "numbeo_data.json",
    "wps": "wps_data.json",
}


def _unlink_if_exists(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError as e:
        print(f"Предупреждение: не удалось удалить {path}: {e}", file=sys.stderr)


FLAT_KEYS = {
    "gallup": {
        "law_and_order_index": "gallup_law_and_order_index",
        "safe_walking_night_pct": "gallup_safe_walking_night_pct",
    },
    "gpi": {"score": "gpi_score"},
    "numbeo": {
        "crime_index": "numbeo_crime_index",
        "safety_index": "numbeo_safety_index",
    },
    "wps": {"wps_score": "wps_score"},
}


def _numeric_zero_as_missing(val: object) -> bool:
    """Нуль из парсеров не переносим в merged — дальше метрика считается отсутствующей."""
    if isinstance(val, bool):
        return False
    if isinstance(val, (int, float)):
        return float(val) == 0.0
    return False


def run_parser(
    script: str,
    args: list[str],
    *,
    cwd: Path,
) -> None:
    cmd = [sys.executable, str(cwd / script), *args]
    print("Запуск:", " ".join(cmd))
    r = subprocess.run(cmd, cwd=str(cwd))
    if r.returncode != 0:
        raise SystemExit(f"Ошибка выполнения {script} (код {r.returncode})")


def load_source_json(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def merge_loaded(
    loaded: dict[str, dict],
    ref: dict,
    manual_path: Path | None,
) -> dict:
    _, iso2_manual = load_manual_map(manual_path)

    all_iso2: set[str] = set()
    for sid, data in loaded.items():
        for c in data.get("countries") or []:
            iso = c.get("iso2")
            if iso and isinstance(iso, str) and len(iso.strip()) == 2:
                all_iso2.add(iso.strip().upper())

    by_iso2: dict[str, dict] = {}
    for iso in sorted(all_iso2):
        name = display_name_for_iso2(iso, ref, iso2_manual)
        row: dict = {"iso2": iso}
        if name:
            row["name"] = name
        by_iso2[iso] = row

    first_country_name: dict[str, str] = {}
    for sid, data in loaded.items():
        mapping = FLAT_KEYS.get(sid, {})
        for c in data.get("countries") or []:
            iso = c.get("iso2")
            if not iso or not isinstance(iso, str) or len(iso.strip()) != 2:
                continue
            iso_u = iso.strip().upper()
            if iso_u not in by_iso2:
                continue
            co = c.get("country")
            if isinstance(co, str) and co.strip() and iso_u not in first_country_name:
                first_country_name[iso_u] = co.strip()
            for raw_key, out_key in mapping.items():
                if raw_key not in c:
                    continue
                val = c[raw_key]
                if val is None or _numeric_zero_as_missing(val):
                    continue
                by_iso2[iso_u][out_key] = val

    for iso, row in by_iso2.items():
        if "name" not in row or not row["name"]:
            row["name"] = first_country_name.get(iso) or row.get("name") or ""

    unmatched: list[dict] = []
    for sid, data in loaded.items():
        for c in data.get("countries") or []:
            iso = c.get("iso2")
            if iso and isinstance(iso, str) and len(iso.strip()) == 2:
                continue
            metrics = {
                k: v
                for k, v in c.items()
                if k not in ("country", "iso2")
            }
            unmatched.append({
                "source": sid,
                "country": c.get("country"),
                "metrics": metrics,
            })

    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": {},
    }
    for sid, data in loaded.items():
        entry = {k: data[k] for k in ("source", "url", "provider", "year", "note") if k in data}
        entry["file"] = SOURCE_OUTPUTS.get(sid, "")
        meta["sources"][sid] = entry

    countries_list = [by_iso2[k] for k in sorted(by_iso2.keys())]
    return {
        "meta": meta,
        "countries": countries_list,
        "by_iso2": by_iso2,
        "unmatched": unmatched,
    }


def _resolve_reference_path(args_reference_json: str | None) -> Path:
    if args_reference_json:
        return Path(args_reference_json)
    env_path = os.environ.get("REFERENCE_COUNTRIES_FILE")
    if env_path:
        return Path(env_path)
    return ROOT / "reference_countries.json"


def main() -> None:
    parser = argparse.ArgumentParser(description="Пайплайн парсеров безопасности + merge")
    parser.add_argument(
        "--reference-json",
        default=None,
        help="JSON справочника стран (перекрывает REFERENCE_COUNTRIES_FILE; по умолчанию env или reference_countries.json)",
    )
    parser.add_argument(
        "--merged-out",
        default=None,
        help="Итоговый объединённый JSON (по умолчанию safety_merged.json)",
    )
    parser.add_argument(
        "--manual-map",
        default=None,
        help="manual_country_map.json (по умолчанию env или файл рядом с country_reference)",
    )
    parser.add_argument(
        "--safety-config",
        default=None,
        help="JSON весов и границ (по умолчанию env SAFETY_INDEX_CONFIG_FILE или safety_index_config.json)",
    )
    parser.add_argument(
        "--safety-manual-scores",
        default=None,
        help="JSON ручных оценок (по умолчанию env SAFETY_MANUAL_SCORES_FILE или safety_manual_scores.json)",
    )
    parser.add_argument(
        "--keep-intermediate",
        action="store_true",
        help="Не удалять после merge промежуточные *_data.json парсеров",
    )
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")

    ref_path = _resolve_reference_path(args.reference_json)
    if not ref_path.is_file():
        print(
            f"Файл справочника стран не найден: {ref_path.resolve()}\n"
            "Задайте REFERENCE_COUNTRIES_FILE в .env или существующий --reference-json.",
            file=sys.stderr,
        )
        sys.exit(1)

    ref_path_arg = str(ref_path.resolve())
    ref = load_reference_from_path(ref_path_arg)

    merged_out = Path(args.merged_out or os.environ.get("MERGED_OUTPUT_FILE") or ROOT / "safety_merged.json")

    manual_explicit = args.manual_map or os.environ.get("MANUAL_COUNTRY_MAP_FILE")
    manual_path = effective_manual_map_path(manual_explicit)

    ref_args = ["--reference-json", ref_path_arg]
    manual_args: list[str] = []
    if manual_path:
        manual_args = ["--manual-map", str(manual_path)]

    gallup_pdf = os.environ.get("GALLUP_DATA_FILE")
    wps_pdf = os.environ.get("WOMEN_PEACE_SECURITY_INDEX_FILE")
    if not gallup_pdf or not Path(gallup_pdf).is_file():
        gp = Path(gallup_pdf) if gallup_pdf else None
        resolved = gp.resolve() if gp else "(переменная не задана)"
        print(
            "Файл Gallup PDF не найден. Проверьте GALLUP_DATA_FILE в .env и cwd.\n"
            f"  Ожидался путь: {resolved}",
            file=sys.stderr,
        )
        sys.exit(1)
    if not wps_pdf or not Path(wps_pdf).is_file():
        wp = Path(wps_pdf) if wps_pdf else None
        resolved = wp.resolve() if wp else "(переменная не задана)"
        print(
            "Файл WPS PDF не найден. Проверьте WOMEN_PEACE_SECURITY_INDEX_FILE в .env и cwd.\n"
            f"  Ожидался путь: {resolved}",
            file=sys.stderr,
        )
        sys.exit(1)

    run_parser("GallupParser.py", [gallup_pdf, *ref_args, *manual_args], cwd=ROOT)
    run_parser("WPSparser.py", [wps_pdf, *ref_args, *manual_args], cwd=ROOT)
    run_parser("GPIparser.py", [*ref_args, *manual_args], cwd=ROOT)
    run_parser("NumbeoParser.py", [*ref_args, *manual_args], cwd=ROOT)

    loaded = {
        sid: load_source_json(ROOT / fname)
        for sid, fname in SOURCE_OUTPUTS.items()
    }
    merged = merge_loaded(loaded, ref, manual_path)

    safety_cfg_arg = args.safety_config or os.environ.get("SAFETY_INDEX_CONFIG_FILE")
    safety_manual_arg = args.safety_manual_scores or os.environ.get("SAFETY_MANUAL_SCORES_FILE")
    safety_config_path = Path(safety_cfg_arg) if safety_cfg_arg else ROOT / "safety_index_config.json"
    safety_manual_path = Path(safety_manual_arg) if safety_manual_arg else ROOT / "safety_manual_scores.json"
    enrich_merged_with_safety_index(
        merged,
        config_path=safety_config_path,
        manual_path=safety_manual_path,
    )

    with open(merged_out, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    print(f"✓ Объединённый файл: {merged_out}")

    print(f"  Стран с iso2: {len(merged['by_iso2'])}, unmatched: {len(merged['unmatched'])}")

    if not args.keep_intermediate:
        for fname in SOURCE_OUTPUTS.values():
            _unlink_if_exists(ROOT / fname)
        print("  Удалены промежуточные JSON:", ", ".join(SOURCE_OUTPUTS.values()))


if __name__ == "__main__":
    main()
