"""

Краткий отчёт по safety_merged.json: unmatched и страны с большим разбросом нормализованных индексов.

Итог по умолчанию пишется в UTF-8 файл; в консоль — одна строка-резюме.



Переменные окружения:

  MERGED_OUTPUT_FILE — путь к JSON (по умолчанию safety_merged.json), перекрывается --merged.

  SAFETY_MERGED_ANALYSIS_OUT — путь к файлу отчёта, перекрывается --out.

  SAFETY_NORM_SPREAD_THRESHOLD_PCT — доля шкалы 0–100 (по умолчанию 0.1 → 10 пунктов).

  SAFETY_NORM_SPREAD_MODE — как считать разброс по safety_norm_* (кроме safety_norm_median):

    minmax (по умолчанию) — max − min по нормам источников;

    mean — max |x − mean(x)| по тем же значениям;

    median — max |x − median(x)| по тем же значениям.

"""



from __future__ import annotations



import argparse

import io

import json

import os

import statistics

import sys

from pathlib import Path

from typing import TextIO



from dotenv import load_dotenv



from safety_composite import NORM_KEYS



ROOT = Path(__file__).resolve().parent



CANONICAL_NORM_FIELDS: frozenset[str] = frozenset(NORM_KEYS.values())





def _merged_path_arg(cli_path: str | None) -> Path:

    if cli_path:

        return Path(cli_path)

    env_path = os.environ.get("MERGED_OUTPUT_FILE")

    if env_path:

        return Path(env_path)

    return ROOT / "safety_merged.json"





def _default_report_path(merged_path: Path) -> Path:

    return merged_path.with_name(f"{merged_path.stem}_analysis.txt")





def _report_out_path(merged_path: Path, cli_out: str | None) -> Path:

    if cli_out:

        return Path(cli_out)

    env_path = (os.environ.get("SAFETY_MERGED_ANALYSIS_OUT") or "").strip()

    if env_path:

        return Path(env_path)

    return _default_report_path(merged_path)





def _threshold_pct() -> float:

    raw = os.environ.get("SAFETY_NORM_SPREAD_THRESHOLD_PCT", "0.1").strip()

    try:

        v = float(raw)

    except ValueError:

        return 0.1

    return max(0.0, min(1.0, v))





def _spread_mode() -> str:

    raw = (os.environ.get("SAFETY_NORM_SPREAD_MODE") or "minmax").strip().lower()

    if raw in ("mean", "avg", "average"):

        return "mean"

    if raw in ("median", "med"):

        return "median"

    return "minmax"





def _norm_spread_from_vals(vals: list[float], mode: str) -> float:

    if len(vals) < 2:

        return 0.0

    if mode == "mean":

        m = float(statistics.mean(vals))

        return max(abs(v - m) for v in vals)

    if mode == "median":

        med = float(statistics.median(vals))

        return max(abs(v - med) for v in vals)

    return max(vals) - min(vals)





def _norm_field_names() -> frozenset[str]:

    return CANONICAL_NORM_FIELDS





def _collect_norm_values(row: dict) -> dict[str, float]:

    """Только нормы источников (без safety_norm_median)."""

    out: dict[str, float] = {}

    for k in CANONICAL_NORM_FIELDS:

        v = row.get(k)

        if v is None or isinstance(v, bool):

            continue

        if isinstance(v, (int, float)):

            out[k] = float(v)

    return out





def _write_unmatched(data: dict, out: TextIO) -> int:

    unmatched = data.get("unmatched")

    if not isinstance(unmatched, list):

        print("unmatched: отсутствует или не массив", file=out)

        return 0

    n = len(unmatched)

    print(f"unmatched: всего {n}", file=out)

    for i, item in enumerate(unmatched, 1):

        if not isinstance(item, dict):

            print(f"  [{i}] (некорректная запись)", file=out)

            continue

        src = item.get("source", "?")

        country = item.get("country", "?")

        cstr = country if isinstance(country, str) else repr(country)

        if len(cstr) > 80:

            cstr = cstr[:77] + "…"

        print(f"  [{i}] {src} | {cstr}", file=out)

    return n





# iso, norms, lo, hi, spread

FlaggedRow = tuple[str, dict[str, float], float, float, float]





def _build_flagged_list(by_iso2: dict, min_points: float, spread_mode: str) -> list[FlaggedRow]:

    flagged: list[FlaggedRow] = []

    for iso_raw, row in sorted(by_iso2.items(), key=lambda x: str(x[0])):

        if not isinstance(row, dict):

            continue

        norms = _collect_norm_values(row)

        if len(norms) < 2:

            continue

        vals = list(norms.values())

        lo, hi = min(vals), max(vals)

        spread = _norm_spread_from_vals(vals, spread_mode)

        if spread >= min_points:

            flagged.append((str(iso_raw).upper(), norms, lo, hi, spread))



    flagged.sort(key=lambda t: (-t[4], t[0]))

    return flagged





def _print_flagged_report(flagged: list[FlaggedRow], out: TextIO) -> None:

    if not flagged:

        print("Стран с размахом >= порога: 0", file=out)

        return



    print(f"Стран с размахом >= порога: {len(flagged)}", file=out)



    key_w = max(len(k) for _, norms, _, _, _ in flagged for k in norms)

    key_w = max(key_w, 7)



    print(file=out)

    print(f"        {'field':<{key_w}}   value", file=out)

    print(f"        {'-' * key_w}   ----------", file=out)



    for iso, norms, lo, hi, spread in flagged:

        print(file=out)

        vals = list(norms.values())

        mean_v = float(statistics.mean(vals))

        median_v = float(statistics.median(vals))

        minmax_v = hi - lo

        print(

            f"  {iso}   min={lo:.4g}   max={hi:.4g}   minmax={minmax_v:.4g}   "

            f"mean={mean_v:.4g}   median={median_v:.4g}   spread_metric={spread:.4g}",

            file=out,

        )

        for k in sorted(norms.keys()):

            print(f"        {k:<{key_w}}   {norms[k]:>10.4f}", file=out)





def _write_norm_spread(data: dict, threshold_pct: float, spread_mode: str, out: TextIO) -> int:

    by_iso2 = data.get("by_iso2")

    if not isinstance(by_iso2, dict):

        print("by_iso2: отсутствует или не объект", file=out)

        return 0



    min_points = threshold_pct * 100.0

    known = _norm_field_names()

    mode_desc = {

        "minmax": "размах max−min по нормам источников (safety_norm_*), без safety_norm_median",

        "mean": "макс. отклонение от среднего арифметического по тем же нормам",

        "median": "макс. отклонение от медианы по тем же нормам",

    }.get(spread_mode, spread_mode)



    print(

        f"Нормализованные индексы (safety_norm_*): порог >= {min_points:.2f} "

        f"({threshold_pct:.0%} шкалы 0–100), страны с >= 2 нормами; "

        f"SAFETY_NORM_SPREAD_MODE={spread_mode!r}: {mode_desc}.",

        file=out,

    )

    print(

        "В merged: safety_norm_median — медиана по тем же нормам (для справки, в размах не участвует).",

        file=out,

    )

    print(f"Ожидаемые ключи (из конфига): {', '.join(sorted(known))}", file=out)



    flagged = _build_flagged_list(by_iso2, min_points, spread_mode)

    _print_flagged_report(flagged, out)

    return len(flagged)





def main() -> None:

    ap = argparse.ArgumentParser(description="Анализ safety_merged.json")

    ap.add_argument(

        "--merged",

        default=None,

        help="Путь к merged JSON (иначе MERGED_OUTPUT_FILE или safety_merged.json)",

    )

    ap.add_argument(

        "--out",

        "-o",

        default=None,

        help="Файл отчёта UTF-8 (иначе SAFETY_MERGED_ANALYSIS_OUT или <stem merged>_analysis.txt)",

    )

    ap.add_argument(

        "--stdout",

        action="store_true",

        help="Дублировать полный отчёт в stdout (по умолчанию только файл + краткое резюме)",

    )

    args = ap.parse_args()



    load_dotenv(ROOT / ".env")



    path = _merged_path_arg(args.merged)

    if not path.is_file():

        print(f"Файл не найден: {path.resolve()}", file=sys.stderr)

        sys.exit(1)



    with open(path, encoding="utf-8") as f:

        data = json.load(f)



    thr = _threshold_pct()

    spread_mode = _spread_mode()

    report_path = _report_out_path(path, args.out)

    report_path.parent.mkdir(parents=True, exist_ok=True)



    buf = io.StringIO()

    print(f"Источник merged: {path.resolve()}", file=buf)

    print(file=buf)

    n_unmatched = _write_unmatched(data, buf)

    print(file=buf)

    n_flagged = _write_norm_spread(data, thr, spread_mode, buf)

    text = buf.getvalue()



    with open(report_path, "w", encoding="utf-8", newline="\n") as rf:

        rf.write(text)



    if args.stdout:

        sys.stdout.write(text)



    print(

        f"Report written: {report_path.resolve()} "

        f"(unmatched={n_unmatched}, norm_spread_flagged={n_flagged})"

    )





if __name__ == "__main__":

    main()


