#!/usr/bin/env python3
"""
Materials Federation Sync — 直接调用各数据库 REST API
无重型依赖，只需 requests + pyyaml
"""

import os
import sys
import json
import time
import yaml
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from typing import Optional

import requests

# ── 日志 ──────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sync")

# ── 路径 ──────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
MATERIALS_DIR = ROOT / "materials"
SYNC_LIST_PATH = ROOT / "sync_list.json"

HEADERS = {"User-Agent": "materials-federation/1.0 (https://github.com/shushuzn/materials-federation)"}

# ── 数字属性（用于差异检测）───────────────────────────────
NUMERIC_PROPS = [
    "band_gap", "formation_energy", "formation_energy_per_atom",
    "bulk_modulus", "shear_modulus", "youngs_modulus", "poissons_ratio",
    "density", "total_magnetization", "energy_per_atom",
]

# ── Dataclass ──────────────────────────────────────────
@dataclass
class DbEntry:
    source: str
    material_id: str = ""
    formula: str = ""
    spacegroup: str = ""
    band_gap: Optional[float] = None
    formation_energy: Optional[float] = None
    formation_energy_per_atom: Optional[float] = None
    bulk_modulus: Optional[float] = None
    shear_modulus: Optional[float] = None
    youngs_modulus: Optional[float] = None
    poissons_ratio: Optional[float] = None
    density: Optional[float] = None
    total_magnetization: Optional[float] = None
    energy_per_atom: Optional[float] = None
    structure_type: str = ""
    # 原始 JSON 整存，保留其他字段
    raw: dict = field(default_factory=dict)
    last_updated: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d.pop("source")
        # 去除 None 值
        return {k: v for k, v in d.items() if v is not None and v != ""}


# ── API 实现 ──────────────────────────────────────────

def mp_fetch(material_id: str, api_key: str = None) -> Optional[DbEntry]:
    """Materials Project REST API v2"""
    api_key = api_key or os.environ.get("MP_API_KEY", "")
    if not api_key:
        log.warning("[MP] No API key, skipping")
        return None

    url = f"https://materialsproject.org/rest/v2/materials/{material_id}/vasp"
    try:
        r = requests.get(url, headers={"X-API-Key": api_key, **HEADERS}, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        d = r.json()

        # Materials Project 返回结构: {"response": [...]}
        resp = d.get("response", [{}])[0]
        return DbEntry(
            source="materials_project",
            material_id=resp.get("material_id", material_id),
            formula=resp.get("pretty_formula", ""),
            spacegroup=resp.get("spacegroup", {}).get("symbol", ""),
            band_gap=resp.get("band_gap"),
            formation_energy=resp.get("formation_energy"),
            formation_energy_per_atom=resp.get("formation_energy_per_atom"),
            structure_type=resp.get("structure", ""),
            raw=resp,
            last_updated=time.strftime("%Y-%m-%d"),
        )
    except requests.exceptions.HTTPError as e:
        log.error(f"[MP] HTTP error for {material_id}: {e}")
    except Exception as e:
        log.error(f"[MP] Failed {material_id}: {e}")
    return None


def jarvis_fetch(jarvis_id: str) -> Optional[DbEntry]:
    """JARVIS NIST API"""
    # JARVIS API endpoint
    url = f"https://jarvis.nist.gov/api/jarvis/v1/material/{jarvis_id}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        d = r.json()

        resp = d.get("response", d)
        return DbEntry(
            source="jarvis",
            material_id=jarvis_id,
            formula=resp.get("formula", ""),
            spacegroup=resp.get("spacegroup", ""),
            band_gap=_float(resp.get("bandgap")),
            formation_energy=_float(resp.get("formation_energy")),
            bulk_modulus=_float(resp.get("bulk_modulus")),
            shear_modulus=_float(resp.get("shear_modulus")),
            structure_type=resp.get("structure_type", ""),
            raw=resp,
            last_updated=time.strftime("%Y-%m-%d"),
        )
    except Exception as e:
        log.error(f"[JARVIS] Failed {jarvis_id}: {e}")
    return None


def oqmd_fetch(oqmd_id: int) -> Optional[DbEntry]:
    """OQMD API"""
    url = f"https://oqmd.org/oqmdapi/v1/calculations/{oqmd_id}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        d = r.json()

        resp = d.get("response", d)
        return DbEntry(
            source="oqmd",
            material_id=str(oqmd_id),
            formula=resp.get("composition", ""),
            band_gap=_float(resp.get("band_gap")),
            formation_energy=_float(resp.get("formation_energy")),
            raw=resp,
            last_updated=time.strftime("%Y-%m-%d"),
        )
    except Exception as e:
        log.error(f"[OQMD] Failed {oqmd_id}: {e}")
    return None


def aflow_fetch(aflow_id: str) -> Optional[DbEntry]:
    """AFLOW API"""
    url = f"https://aflow.org/API/relaxed/?format=json&aflowlib_entry={aflow_id}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 404 or r.status_code == 400:
            return None
        r.raise_for_status()
        d = r.json()

        entries = d.get("aflowlib_entries", {})
        resp = list(entries.values())[0] if entries else {}

        return DbEntry(
            source="aflow",
            material_id=aflow_id,
            formula=resp.get("chemical_formula", ""),
            spacegroup=resp.get("spacegroup", ""),
            band_gap=_float(resp.get("band_gap_gllbsec", resp.get("band_gap_hse", resp.get("band_gap_gllb")))),
            formation_energy=_float(resp.get("formation_enthalpy", resp.get("formation_energy"))),
            bulk_modulus=_float(resp.get("elastic_modulus_bulk", resp.get("bulk_modulus"))),
            shear_modulus=_float(resp.get("elastic_modulus_shear")),
            raw=resp,
            last_updated=time.strftime("%Y-%m-%d"),
        )
    except Exception as e:
        log.error(f"[AFLOW] Failed {aflow_id}: {e}")
    return None


def nomad_fetch(nomad_id: str) -> Optional[DbEntry]:
    """NOMAD via OPTIMADE API"""
    # NOMAD supports OPTIMADE
    url = f"https://nomad-lab.eu/prod/rae/backed/api/v1/optimade/structures/{nomad_id}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        d = r.json()

        attrs = d.get("attributes", {})
        return DbEntry(
            source="nomad",
            material_id=nomad_id,
            formula=attrs.get("chemical_formula", ""),
            spacegroup=attrs.get("spacegroup", ""),
            band_gap=_float(attrs.get("band_gap")),
            formation_energy=_float(attrs.get("formation_energy")),
            raw=attrs,
            last_updated=time.strftime("%Y-%m-%d"),
        )
    except Exception as e:
        log.error(f"[NOMAD] Failed {nomad_id}: {e}")
    return None


# ── 工具函数 ──────────────────────────────────────────

def _float(v) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def load_yaml(formula: str) -> dict:
    path = MATERIALS_DIR / f"{formula}.yaml"
    if path.exists():
        with open(path) as f:
            return yaml.safe_load(f) or {}
    return {}


def save_yaml(formula: str, data: dict):
    MATERIALS_DIR.mkdir(parents=True, exist_ok=True)
    path = MATERIALS_DIR / f"{formula}.yaml"
    with open(path, "w") as f:
        yaml.dump(data, f, allow_unicode=True, sort_keys=False, width=200)


def rate_limit(seconds: float = 1.0):
    time.sleep(seconds)


def load_sync_list() -> list:
    if SYNC_LIST_PATH.exists():
        with open(SYNC_LIST_PATH) as f:
            return json.load(f)
    return []


# ── 差异检测 ──────────────────────────────────────────

def compute_discrepancies(databases: dict) -> dict:
    """检测同材料跨库数值差异"""
    disc = {}

    for prop in NUMERIC_PROPS:
        values = {}
        for src, data in databases.items():
            v = data.get(prop)
            if v is not None:
                values[src] = v

        if len(values) >= 2:
            vals = list(values.values())
            diff = max(vals) - min(vals)
            # 只记录有实质差异的（>1% 相对差异）
            if diff > 0 and diff / max(abs(v) for v in vals) > 0.01:
                disc[prop] = {
                    "max_diff": round(diff, 6),
                    "relative_diff_pct": round(diff / max(abs(v) for v in vals) * 100, 2),
                    "values": {k: round(v, 6) for k, v in values.items()},
                    "sources": list(values.keys()),
                }

    return disc


def compute_coverage(databases: dict) -> dict:
    """计算各库属性覆盖率"""
    all_props = set()
    for data in databases.values():
        all_props.update(data.keys())

    common = set()
    for data in databases.values():
        if not common:
            common = set(data.keys())
        else:
            common &= set(data.keys())

    unique = {}
    for src, data in databases.items():
        unique[src] = list(set(data.keys()) - common)

    return {
        "common_properties": sorted(common),
        "properties_unique_to_source": unique,
    }


# ── 主流程 ────────────────────────────────────────────

def sync_one(item: dict) -> dict:
    formula = item["formula"]
    log.info(f"Syncing {formula}")
    existing = load_yaml(formula)

    databases = existing.get("databases", {})
    updated = False

    # 并行获取各库数据
    futures = {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        # MP
        if "mp_id" in item and "materials_project" not in databases:
            f = executor.submit(mp_fetch, item["mp_id"])
            futures["materials_project"] = f

        # JARVIS
        if "jarvis_id" in item and "jarvis" not in databases:
            f = executor.submit(jarvis_fetch, item["jarvis_id"])
            futures["jarvis"] = f

        # OQMD
        if "oqmd_id" in item and "oqmd" not in databases:
            f = executor.submit(oqmd_fetch, item["oqmd_id"])
            futures["oqmd"] = f

        # AFLOW
        if "aflow_id" in item and "aflow" not in databases:
            f = executor.submit(aflow_fetch, item["aflow_id"])
            futures["aflow"] = f

        # NOMAD
        if "nomad_id" in item and "nomad" not in databases:
            f = executor.submit(nomad_fetch, item["nomad_id"])
            futures["nomad"] = f

    for src, fut in futures.items():
        rate_limit(1.0)
        result = fut.result()
        if result:
            databases[src] = result.to_dict()
            updated = True
            log.info(f"  [{src}] fetched {formula}")

    # 计算差异和覆盖率
    discrepancies = compute_discrepancies(databases)
    coverage = compute_coverage(databases)

    data = {
        "material_id": formula,
        "formula": formula,
        "databases": databases,
        "cross_db_discrepancies": discrepancies,
        "coverage": coverage,
        "last_updated": time.strftime("%Y-%m-%d"),
    }

    save_yaml(formula, data)

    return {
        "formula": formula,
        "updated": updated,
        "sources": list(databases.keys()),
    }


def main():
    sync_list = load_sync_list()
    if not sync_list:
        log.error("sync_list.json is empty. Add materials to sync.")
        sys.exit(1)

    log.info(f"Starting sync for {len(sync_list)} materials")

    results = []
    for item in sync_list:
        try:
            result = sync_one(item)
            results.append(result)
            if result["updated"]:
                log.info(f"  ✓ {result['formula']} updated ({', '.join(result['sources'])})")
            else:
                log.info(f"  - {result['formula']} no update")
        except Exception as e:
            log.error(f"Failed {item.get('formula', '?')}: {e}")

    updated_count = sum(1 for r in results if r["updated"])
    log.info(f"Done. {updated_count}/{len(results)} materials updated.")


if __name__ == "__main__":
    main()
