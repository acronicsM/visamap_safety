"""
Справочник стран с API и ручные алиасы для сопоставления ISO 3166-1 alpha-2.
"""

from __future__ import annotations

import json
import os
import re
import unicodedata
from pathlib import Path
from typing import Any

import requests

COUNTRIES_NAMES_PATH = "/countries/names"


def default_manual_map_path() -> Path:
    return Path(__file__).resolve().parent / "manual_country_map.json"


def resolve_manual_map_path(explicit: str | None) -> Path | None:
    if explicit:
        p = Path(explicit)
        return p if p.is_file() else None
    p = default_manual_map_path()
    return p if p.is_file() else None


def normalize_name(s: str) -> str:
    if not s or not isinstance(s, str):
        return ""
    t = unicodedata.normalize("NFKC", s)
    t = t.strip().lower()
    t = re.sub(r"\s+", " ", t)
    return t


def _strip_parenthetical(s: str) -> str:
    return re.sub(r"\s*\([^)]*\)", "", s).strip()


def load_manual_map(path: Path | str | None) -> tuple[dict[str, str], dict[str, str]]:
    """Возвращает (aliases_normalized -> iso2, iso2 -> display name)."""
    if not path:
        return {}, {}
    p = Path(path)
    if not p.is_file():
        return {}, {}
    with open(p, encoding="utf-8") as f:
        data = json.load(f)
    raw_aliases = data.get("aliases_to_iso2") or {}
    aliases: dict[str, str] = {}
    for k, v in raw_aliases.items():
        if not k or not v:
            continue
        iso = str(v).strip().upper()
        if len(iso) != 2:
            continue
        nk = normalize_name(str(k))
        if nk:
            aliases[nk] = iso
    raw_names = data.get("iso2_to_name") or {}
    iso2_to_name: dict[str, str] = {}
    for k, v in raw_names.items():
        if not k or not v:
            continue
        iso = str(k).strip().upper()
        if len(iso) != 2:
            continue
        iso2_to_name[iso] = str(v).strip()
    return aliases, iso2_to_name


def _record_name_strings(record: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for key in ("name_en", "name_ru", "name_native"):
        v = record.get(key)
        if isinstance(v, str) and v.strip():
            out.append(v.strip())
    trans = record.get("name_translations")
    if isinstance(trans, dict):
        for v in trans.values():
            if isinstance(v, str) and v.strip():
                out.append(v.strip())
    return out


def _add_name_variants(s: str) -> list[str]:
    s = s.strip()
    if not s:
        return []
    variants = {s}
    stripped = _strip_parenthetical(s)
    if stripped and stripped != s:
        variants.add(stripped)
    return list(variants)


def build_name_index(ref: dict[str, Any]) -> dict[str, str]:
    """
    normalized_name -> iso2. При конфликте (два разных iso2 на одно имя) ключ удаляется.
    """
    index: dict[str, str] = {}

    def add(norm: str, iso: str) -> None:
        if not norm:
            return
        if norm in index:
            if index[norm] != iso:
                del index[norm]
        else:
            index[norm] = iso

    for iso2_key, record in ref.items():
        if not isinstance(record, dict):
            continue
        iso = str(iso2_key).strip().upper()
        if len(iso) != 2:
            continue
        for raw in _record_name_strings(record):
            for variant in _add_name_variants(raw):
                add(normalize_name(variant), iso)
    return index


def fetch_reference(base_url: str, timeout: float = 30.0) -> dict[str, Any]:
    url = base_url.rstrip("/") + COUNTRIES_NAMES_PATH
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        raise ValueError("Ожидался JSON-объект { iso2: { names... } }")
    return data


def load_reference_from_path(path: str | Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Файл справочника: ожидался JSON-объект")
    return data


def load_reference(reference_json: str | None, api_base_url: str | None) -> dict[str, Any] | None:
    if reference_json:
        return load_reference_from_path(reference_json)
    if api_base_url:
        return fetch_reference(api_base_url)
    return None


def resolve_iso2(
    country_name: str,
    name_index: dict[str, str],
    aliases: dict[str, str],
) -> str | None:
    n = normalize_name(country_name)
    if n in aliases:
        return aliases[n]
    if n in name_index:
        return name_index[n]
    stripped = _strip_parenthetical(country_name)
    n2 = normalize_name(stripped)
    if n2 and n2 != n:
        if n2 in aliases:
            return aliases[n2]
        if n2 in name_index:
            return name_index[n2]
    return None


def display_name_for_iso2(
    iso2: str,
    ref: dict[str, Any] | None,
    iso2_to_name_manual: dict[str, str],
) -> str | None:
    iso = iso2.strip().upper()
    if iso in iso2_to_name_manual:
        return iso2_to_name_manual[iso]
    if ref:
        rec = ref.get(iso)
        if rec is None:
            for k, v in ref.items():
                if str(k).strip().upper() == iso:
                    rec = v
                    break
        if isinstance(rec, dict):
            en = rec.get("name_en")
            if isinstance(en, str) and en.strip():
                return en.strip()
    return None


def effective_manual_map_path(explicit: str | Path | None) -> Path | None:
    if explicit:
        p = Path(explicit)
        if p.is_file():
            return p
    env = os.environ.get("MANUAL_COUNTRY_MAP_FILE")
    if env:
        pe = Path(env)
        if pe.is_file():
            return pe
    return resolve_manual_map_path(None)


def enrich_country_entries(
    entries: list[dict[str, Any]],
    *,
    ref: dict[str, Any] | None,
    manual_map_path: Path | str | None = None,
    country_key: str = "country",
) -> None:
    """
    Добавляет в каждую запись поле iso2: str | None (JSON null при сериализации).
    """
    path = effective_manual_map_path(manual_map_path)
    aliases, _ = load_manual_map(path)
    name_index = build_name_index(ref) if ref else {}
    for entry in entries:
        name = entry.get(country_key)
        if not isinstance(name, str):
            entry["iso2"] = None
            continue
        if not ref and not aliases:
            entry["iso2"] = None
            continue
        iso = resolve_iso2(name, name_index, aliases)
        entry["iso2"] = iso
