"""
Единый запуск: кэш справочника стран, четыре парсера, объединённый JSON и единый индекс безопасности.

Требует: API_COUNTRIES_URL в .env (или переменных окружения), либо готовый --reference-json.
PDF-пути: GALLUP_DATA_FILE, WOMEN_PEACE_SECURITY_INDEX_FILE.

После успешного merge по умолчанию удаляются промежуточные gallup/gpi/numbeo/wps *_data.json
и кэш справочника внутри корня репо (см. SAFETY_PIPELINE_DELETE_INTERMEDIATE и --keep-intermediate).
Внешний --reference-json вне репозитория не трогаем.
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
    COUNTRIES_NAMES_PATH,
    effective_manual_map_path,
    fetch_reference,
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

def _is_under_directory(root: Path, path: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _unlink_if_exists(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError as e:
        print(f"Предупреждение: не удалось удалить {path}: {e}", file=sys.stderr)


def _env_delete_intermediate_enabled() -> bool:
    """SAFETY_PIPELINE_DELETE_INTERMEDIATE: 1/true (по умолчанию) — удалять; 0/false — оставить."""
    raw = os.environ.get("SAFETY_PIPELINE_DELETE_INTERMEDIATE", "1").strip().lower()
    if raw in ("0", "false", "no", "off", "n"):
        return False
    return True


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


def save_reference(base_url: str, out_path: Path) -> None:
    ref = fetch_reference(base_url)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(ref, f, indent=2, ensure_ascii=False)


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
                if val is not None:
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Пайплайн парсеров безопасности + merge")
    parser.add_argument(
        "--reference-json",
        default=None,
        help=f"Готовый кэш API ({COUNTRIES_NAMES_PATH}); иначе запрос по API_COUNTRIES_URL",
    )
    parser.add_argument(
        "--reference-out",
        default=None,
        help="Куда сохранить кэш при успешном fetch (по умолчанию reference_countries.json в корне репо)",
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
        "--skip-fetch",
        action="store_true",
        help="Не вызывать API: использовать только существующий --reference-json",
    )
    parser.add_argument(
        "--keep-intermediate",
        action="store_true",
        help="Не удалять после merge промежуточные *_data.json и кэш справочника в корне репо",
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
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")

    ref_out = Path(args.reference_out or os.environ.get("REFERENCE_COUNTRIES_FILE") or ROOT / "reference_countries.json")
    merged_out = Path(args.merged_out or os.environ.get("MERGED_OUTPUT_FILE") or ROOT / "safety_merged.json")

    manual_explicit = args.manual_map or os.environ.get("MANUAL_COUNTRY_MAP_FILE")
    manual_path = effective_manual_map_path(manual_explicit)

    ref: dict
    ref_path_arg: str

    if args.skip_fetch:
        if not args.reference_json:
            print("При --skip-fetch нужен существующий --reference-json", file=sys.stderr)
            sys.exit(1)
        ref_path_arg = args.reference_json
        ref = load_reference_from_path(ref_path_arg)
    elif args.reference_json and Path(args.reference_json).is_file():
        ref_path_arg = args.reference_json
        ref = load_reference_from_path(ref_path_arg)
    else:
        base = os.environ.get("API_COUNTRIES_URL")
        if not base:
            print("Задайте API_COUNTRIES_URL или существующий --reference-json", file=sys.stderr)
            sys.exit(1)
        print(f"Загрузка справочника: {base.rstrip('/')}{COUNTRIES_NAMES_PATH}")
        save_reference(base, ref_out)
        ref_path_arg = str(ref_out)
        ref = load_reference_from_path(ref_path_arg)

    ref_args = ["--reference-json", ref_path_arg]
    manual_args: list[str] = []
    if manual_path:
        manual_args = ["--manual-map", str(manual_path)]

    gallup_pdf = os.environ.get("GALLUP_DATA_FILE")
    wps_pdf = os.environ.get("WOMEN_PEACE_SECURITY_INDEX_FILE")
    if not gallup_pdf or not Path(gallup_pdf).is_file():
        print("Нужен существующий файл GALLUP_DATA_FILE в .env", file=sys.stderr)
        sys.exit(1)
    if not wps_pdf or not Path(wps_pdf).is_file():
        print("Нужен существующий файл WOMEN_PEACE_SECURITY_INDEX_FILE в .env", file=sys.stderr)
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
        json.dump(merged, f, indent=2, ensure_ascii=False)

    print(f"✓ Объединённый файл: {merged_out}")
    print(f"  Стран с iso2: {len(merged['by_iso2'])}, unmatched: {len(merged['unmatched'])}")

    delete_intermediate = _env_delete_intermediate_enabled() and not args.keep_intermediate
    if delete_intermediate:
        for fname in SOURCE_OUTPUTS.values():
            _unlink_if_exists(ROOT / fname)
        print("  Удалены промежуточные JSON:", ", ".join(SOURCE_OUTPUTS.values()))

        ref_cache_path = Path(ref_path_arg).resolve()
        if ref_cache_path.is_file() and _is_under_directory(ROOT, ref_cache_path):
            _unlink_if_exists(ref_cache_path)
            print(f"  Удалён кэш справочника: {ref_cache_path}")
    else:
        print(
            "  Промежуточные файлы не удалены "
            "(--keep-intermediate или SAFETY_PIPELINE_DELETE_INTERMEDIATE=0/false/no)"
        )


if __name__ == "__main__":
    main()
