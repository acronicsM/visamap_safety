"""
Единый индекс безопасности 0–100 (выше = безопаснее): нормализация метрик из merge,
взвешенное среднее с перенормировкой при пропусках, смешивание с ручными оценками.
"""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Поля строки merged JSON (run_safety_pipeline.merge_loaded)
ROW_GALLUP_LO = "gallup_law_and_order_index"
ROW_GALLUP_NIGHT = "gallup_safe_walking_night_pct"
ROW_GPI = "gpi_score"
ROW_NUMBEO = "numbeo_safety_index"
ROW_WPS = "wps_score"

NORM_KEYS = {
    "gallup_lo": "safety_norm_gallup_lo",
    "gallup_night": "safety_norm_gallup_night",
    "gpi": "safety_norm_gpi",
    "numbeo": "safety_norm_numbeo",
    "wps": "safety_norm_wps",
}

# Медиана по фактически присутствующим нормализованным метрикам источников (не композит).
SAFETY_NORM_MEDIAN_FIELD = "safety_norm_median"

METRIC_ORDER = (
    ("gallup_lo", ROW_GALLUP_LO),
    ("gallup_night", ROW_GALLUP_NIGHT),
    ("gpi", ROW_GPI),
    ("numbeo", ROW_NUMBEO),
    ("wps", ROW_WPS),
)

DEFAULT_WEIGHTS: dict[str, float] = {
    "gallup_lo": 0.2,
    "gallup_night": 0.2,
    "gpi": 0.2,
    "numbeo": 0.2,
    "wps": 0.2,
}


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def _num(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    return None


def normalize_gallup_lo(raw: float, lo: float, hi: float) -> float:
    if hi <= lo:
        raise ValueError("gallup_lo_max must be > gallup_lo_min")
    return _clamp01((raw - lo) / (hi - lo)) * 100.0


def normalize_gallup_night(raw: float) -> float:
    return max(0.0, min(100.0, raw))


def normalize_gpi(score: float, lo: float, hi: float) -> float:
    if hi <= lo:
        raise ValueError("gpi_hi must be > gpi_lo")
    return _clamp01((hi - score) / (hi - lo)) * 100.0


def normalize_numbeo(raw: float) -> float:
    return max(0.0, min(100.0, raw))


def normalize_wps(raw: float) -> float:
    return _clamp01(raw) * 100.0


@dataclass(frozen=True)
class SafetyBounds:
    gpi_lo: float
    gpi_hi: float
    gallup_lo_min: float
    gallup_lo_max: float


def default_bounds() -> SafetyBounds:
    return SafetyBounds(
        gpi_lo=1.0,
        gpi_hi=4.0,
        gallup_lo_min=49.0,
        gallup_lo_max=100.0,
    )


def load_safety_config(path: Path | None) -> tuple[dict[str, float], SafetyBounds, int]:
    """Веса (сумма нормируется к 1) и границы. Возвращает (weights, bounds, schema_version)."""
    bounds = default_bounds()
    weights = dict(DEFAULT_WEIGHTS)
    schema_version = 1

    if path is None or not path.is_file():
        return _normalize_weights(weights), bounds, schema_version

    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    schema_version = int(raw.get("schema_version", 1))
    if "gpi_lo" in raw and "gpi_hi" in raw:
        bounds = SafetyBounds(
            gpi_lo=float(raw["gpi_lo"]),
            gpi_hi=float(raw["gpi_hi"]),
            gallup_lo_min=float(raw.get("gallup_lo_min", bounds.gallup_lo_min)),
            gallup_lo_max=float(raw.get("gallup_lo_max", bounds.gallup_lo_max)),
        )
    w = raw.get("weights")
    if isinstance(w, dict):
        for k in DEFAULT_WEIGHTS:
            if k in w and w[k] is not None:
                weights[k] = float(w[k])

    return _normalize_weights(weights), bounds, schema_version


def _normalize_weights(w: dict[str, float]) -> dict[str, float]:
    s = sum(max(0.0, v) for v in w.values())
    if s <= 0:
        return dict(DEFAULT_WEIGHTS)
    return {k: max(0.0, w[k]) / s for k in DEFAULT_WEIGHTS}


@dataclass(frozen=True)
class ManualEntry:
    score: float
    weight: float
    note: str | None


def load_manual_scores(path: Path | None) -> dict[str, ManualEntry]:
    if path is None or not path.is_file():
        return {}

    with open(path, encoding="utf-8") as f:
        doc = json.load(f)

    entries = doc.get("entries")
    if not isinstance(entries, list):
        return {}

    out: dict[str, ManualEntry] = {}
    for item in entries:
        if not isinstance(item, dict):
            continue
        iso = item.get("iso2")
        if not iso or not isinstance(iso, str) or len(iso.strip()) != 2:
            continue
        iso_u = iso.strip().upper()
        try:
            score = float(item["score"])
        except (KeyError, TypeError, ValueError):
            continue
        score = max(0.0, min(100.0, score))
        try:
            mw = float(item.get("weight", 1.0))
        except (TypeError, ValueError):
            mw = 1.0
        if mw <= 0 or mw > 1.0:
            continue
        note = item.get("note")
        note_s = note.strip() if isinstance(note, str) else None
        out[iso_u] = ManualEntry(score=score, weight=mw, note=note_s or None)
    return out


def _normalize_one(
    metric_key: str,
    row: dict[str, Any],
    bounds: SafetyBounds,
) -> float | None:
    if metric_key == "gallup_lo":
        v = _num(row.get(ROW_GALLUP_LO))
        if v is None:
            return None
        return normalize_gallup_lo(v, bounds.gallup_lo_min, bounds.gallup_lo_max)
    if metric_key == "gallup_night":
        v = _num(row.get(ROW_GALLUP_NIGHT))
        if v is None:
            return None
        return normalize_gallup_night(v)
    if metric_key == "gpi":
        v = _num(row.get(ROW_GPI))
        if v is None:
            return None
        return normalize_gpi(v, bounds.gpi_lo, bounds.gpi_hi)
    if metric_key == "numbeo":
        v = _num(row.get(ROW_NUMBEO))
        if v is None:
            return None
        return normalize_numbeo(v)
    if metric_key == "wps":
        v = _num(row.get(ROW_WPS))
        if v is None:
            return None
        return normalize_wps(v)
    return None


def compute_row_scores(
    row: dict[str, Any],
    weights: dict[str, float],
    bounds: SafetyBounds,
) -> tuple[float | None, dict[str, float], dict[str, float]]:
    """
    composite, norms (output field name -> value), weights_used (metric_key -> renormalized weight).
    """
    active: list[tuple[str, float, float]] = []
    norms_out: dict[str, float] = {}

    for metric_key, _ in METRIC_ORDER:
        n = _normalize_one(metric_key, row, bounds)
        if n is None:
            continue
        w = weights.get(metric_key, 0.0)
        if w <= 0:
            continue
        active.append((metric_key, w, n))
        norms_out[NORM_KEYS[metric_key]] = round(n, 4)

    if not active:
        return None, norms_out, {}

    total_w = sum(t[1] for t in active)
    if total_w <= 0:
        return None, norms_out, {}

    composite = sum(w * n for _, w, n in active) / total_w
    weights_used = {mk: round(w / total_w, 6) for mk, w, _ in active}
    return round(composite, 4), norms_out, weights_used


def apply_manual(
    composite: float | None,
    iso2: str,
    manual: dict[str, ManualEntry],
) -> tuple[float | None, bool, ManualEntry | None]:
    entry = manual.get(iso2)
    if entry is None:
        return composite, False, None
    if composite is None:
        return round(entry.score, 4), True, entry
    final = entry.weight * entry.score + (1.0 - entry.weight) * composite
    return round(final, 4), True, entry


def enrich_merged_with_safety_index(
    merged: dict[str, Any],
    *,
    config_path: Path | None,
    manual_path: Path | None,
) -> None:
    """Дополняет merged['by_iso2'] / merged['countries'] и merged['meta']['safety_index']."""
    weights, bounds, schema_ver = load_safety_config(config_path)
    manual = load_manual_scores(manual_path)

    by_iso2: dict[str, dict] = merged.get("by_iso2") or {}
    config_file_str = str(config_path.resolve()) if config_path else None
    manual_file_str = str(manual_path.resolve()) if manual_path else None

    for iso, row in by_iso2.items():
        for k in list(NORM_KEYS.values()) + [
            SAFETY_NORM_MEDIAN_FIELD,
            "safety_composite_score",
            "safety_final_score",
            "safety_weights_used",
            "safety_manual_applied",
            "safety_manual_weight",
        ]:
            row.pop(k, None)

        composite, norms, w_used = compute_row_scores(row, weights, bounds)
        for nk, nv in norms.items():
            row[nk] = nv
        norm_vals = list(norms.values())
        if norm_vals:
            row[SAFETY_NORM_MEDIAN_FIELD] = round(float(statistics.median(norm_vals)), 4)
        if composite is not None:
            row["safety_composite_score"] = composite
        if w_used:
            row["safety_weights_used"] = w_used

        final, applied, ment = apply_manual(composite, iso, manual)
        if final is not None:
            row["safety_final_score"] = final
        row["safety_manual_applied"] = applied
        if applied and ment is not None:
            row["safety_manual_weight"] = ment.weight

    meta = merged.setdefault("meta", {})
    meta["safety_index"] = {
        "schema_version": schema_ver,
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "config_file": config_file_str,
        "manual_scores_file": manual_file_str,
        "weights": weights,
        "bounds": {
            "gpi_lo": bounds.gpi_lo,
            "gpi_hi": bounds.gpi_hi,
            "gallup_lo_min": bounds.gallup_lo_min,
            "gallup_lo_max": bounds.gallup_lo_max,
        },
        "manual_overrides_count": len(manual),
    }
