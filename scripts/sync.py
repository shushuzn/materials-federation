#!/usr/bin/env python3
"""
Materials Federation Sync — jarvis-tools + OPTIMADE 标准
主要数据源:
  - JARVIS: jarvis-tools (figshare 本地数据集, 75k+ 材料)
  - MP/OQMD/AFLOW/NOMAD: OPTIMADE / REST API
无重型依赖(pymatgen/mp-api)，只需 requests + pyyaml + jarvis-tools
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

# ── License header 模板 ────────────────────────────────
LICENSE_HEADER = """# ─────────────────────────────────────────────────────────────────────────────
# {formula}
# Synced at: {synced_at}
# License: CC BY 4.0 (Materials Project), NIST Terms (JARVIS),
#          OQMD License (OQMD), AFLOW License (AFLOW),
#          CC BY 4.0 / Database Right (NOMAD)
# Disclaimer: Data sourced from third-party databases. All data remains the
#   intellectual property of respective database providers under their
#   applicable licenses. This aggregation is for research purposes only.
# Sources: {sources}
# ─────────────────────────────────────────────────────────────────────────────
""".strip()

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
    raw: dict = field(default_factory=dict)
    last_updated: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d.pop("source")
        return {k: v for k, v in d.items() if v is not None and v != ""}


# ── JARVIS 数据源 (jarvis-tools figshare) ────────────────────────────────
# 全局缓存，避免每次运行都下载数据集
_JARVIS_CACHE = {}  # formula -> jarvis entry dict


def _ensure_jarvis_cache():
    """懒加载 JARVIS 3D DFT 数据集到内存缓存"""
    if _JARVIS_CACHE:
        return  # 已有缓存
    try:
        from jarvis.db.figshare import data
        log.info("[JARVIS] Loading 3D DFT dataset from figshare (first run may take ~1 min)...")
        dft3d = data("dft_3d")
        for entry in dft3d:
            formula = entry.get("formula", "")
            if formula and formula not in _JARVIS_CACHE:
                _JARVIS_CACHE[formula] = entry
        log.info(f"[JARVIS] Cached {len(_JARVIS_CACHE)} unique formulas")
    except Exception as e:
        log.error(f"[JARVIS] Failed to load dataset: {e}")


def jarvis_fetch_by_formula(formula: str) -> Optional[DbEntry]:
    """用 formula 在本地 JARVIS 数据集中查找第一条记录"""
    _ensure_jarvis_cache()
    cache_size = len(_JARVIS_CACHE)
    entry = _JARVIS_CACHE.get(formula)
    if not entry:
        log.warning(f"[JARVIS] No entry for formula={formula!r} in cache of {cache_size} formulas. Sample keys: {list(_JARVIS_CACHE.keys())[:3]}")
        return None

    # 字段映射: JARVIS -> DbEntry
    return DbEntry(
        source="jarvis",
        material_id=entry.get("jid", ""),
        formula=entry.get("formula", ""),
        spacegroup=str(entry.get("spg_symbol", "")),
        band_gap=_float(entry.get("optb88vdw_bandgap")),
        formation_energy_per_atom=_float(entry.get("formation_energy_peratom")),
        density=_float(entry.get("density")),
        poissons_ratio=_float(entry.get("poisson")),
        bulk_modulus=_float(entry.get("bulk_modulus_kv")),
        shear_modulus=_float(entry.get("shear_modulus_gv")),
        raw=entry,
        last_updated=time.strftime("%Y-%m-%d"),
    )


# ── OPTIMADE Fetchers ─────────────────────────────────

def optimade_fetch(provider_url: str, db_source: str, material_id: str = None,
                   formula: str = None, page_limit: int = 10) -> list:
    """通用 OPTIMADE 查询，返回匹配条目列表"""
    try:
        filter_parts = []
        if material_id:
            filter_parts.append(f"id='{material_id}'")
        if formula:
            filter_parts.append(f"chemical_formula='{formula}'")
        if not filter_parts:
            return []

        filter_str = filter_parts[0]
        url = f"{provider_url}/v1/structures?filter={filter_str}&page_limit={page_limit}"
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code >= 400:
            log.debug(f"[{db_source.upper()}] OPTIMADE {r.status_code} for {filter_str}")
            return []
        r.raise_for_status()
        d = r.json()
        data = d.get("data", [])
        return data
    except Exception as e:
        log.debug(f"[{db_source.upper()}] optimade_fetch error: {e}")
    return []


def optimade_to_dbentry(data_items: list, db_source: str) -> Optional[DbEntry]:
    """从 OPTIMADE 结构数据构造 DbEntry"""
    if not data_items:
        return None

    item = data_items[0]
    attrs = item.get("attributes", {})
    raw = attrs.copy()

    band_gap = _float(attrs.get("band_gap"))
    if isinstance(attrs.get("band_gap"), dict):
        band_gap = _float(attrs.get("band_gap", {}).get("value"))

    formation_energy = _float(attrs.get("formation_energy"))
    if isinstance(attrs.get("formation_energy"), dict):
        formation_energy = _float(attrs.get("formation_energy", {}).get("value"))

    return DbEntry(
        source=db_source,
        material_id=item.get("id", ""),
        formula=attrs.get("chemical_formula", ""),
        spacegroup=str(attrs.get("spacegroup", "")),
        band_gap=band_gap,
        formation_energy=formation_energy,
        formation_energy_per_atom=_float(attrs.get("formation_energy_per_atom")),
        density=_float(attrs.get("density")),
        structure_type=attrs.get("structure_type", ""),
        raw=raw,
        last_updated=time.strftime("%Y-%m-%d"),
    )


# ── 各库专用 Fetcher ──────────────────────────────────

def mp_fetch(material_id: str = None, formula: str = None, api_key: str = None) -> Optional[DbEntry]:
    """Materials Project — OPTIMADE endpoint"""
    data = optimade_fetch(
        "https://optimade.materialsproject.org",
        "materials_project",
        material_id=material_id,
        formula=formula
    )
    if data:
        return optimade_to_dbentry(data, "materials_project")

    api_key = api_key or os.environ.get("MP_API_KEY", "")
    if not api_key:
        log.debug("[MP] No API key, skipping")
        return None

    # Try REST API (supports formula lookup via /MaterialsSnapshot)
    url = f"https://materialsproject.org/rest/v2/materials/{material_id or formula}/vasp"
    try:
        headers = {"X-API-Key": api_key, **HEADERS}
        if material_id:
            headers["User-Agent"] = HEADERS["User-Agent"]
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 404:
            log.debug(f"[MP] REST 404 for {material_id or formula}")
            return None
        r.raise_for_status()
        d = r.json()
        resp = d.get("response", [{}])[0]
        return DbEntry(
            source="materials_project",
            material_id=resp.get("material_id", material_id or formula),
            formula=resp.get("pretty_formula", ""),
            spacegroup=resp.get("spacegroup", {}).get("symbol", ""),
            band_gap=resp.get("band_gap"),
            formation_energy=resp.get("formation_energy"),
            formation_energy_per_atom=resp.get("formation_energy_per_atom"),
            structure_type=resp.get("structure", ""),
            raw=resp,
            last_updated=time.strftime("%Y-%m-%d"),
        )
    except Exception as e:
        log.debug(f"[MP] REST error: {e}")
    return None


def oqmd_fetch(oqmd_id: int = None, formula: str = None) -> Optional[DbEntry]:
    """OQMD — REST API"""
    if not oqmd_id:
        return None
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
        log.debug(f"[OQMD] Failed {oqmd_id}: {e}")
    return None


def aflow_fetch(aflow_id: str = None, formula: str = None) -> Optional[DbEntry]:
    """AFLOW — REST API"""
    if not aflow_id:
        return None
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
            bulk_modulus=_float(resp.get("elastic_modulus_bulk")),
            shear_modulus=_float(resp.get("elastic_modulus_shear")),
            raw=resp,
            last_updated=time.strftime("%Y-%m-%d"),
        )
    except Exception as e:
        log.debug(f"[AFLOW] Failed {aflow_id}: {e}")
    return None


def nomad_fetch(nomad_id: str = None, formula: str = None) -> Optional[DbEntry]:
    """NOMAD — OPTIMADE endpoint"""
    data = optimade_fetch(
        "https://nomad-lab.eu/prod/rae/backed/api/v1/optimade",
        "nomad",
        material_id=nomad_id,
        formula=formula
    )
    if data:
        return optimade_to_dbentry(data, "nomad")
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


def save_yaml(formula: str, data: dict, sources: list = None):
    """保存 YAML，带 license header"""
    MATERIALS_DIR.mkdir(parents=True, exist_ok=True)
    path = MATERIALS_DIR / f"{formula}.yaml"

    synced_at = time.strftime("%Y-%m-%dT%H:%M:%SZ")
    sources_str = ", ".join(sources) if sources else ", ".join(data.get("databases", {}).keys())

    header = LICENSE_HEADER.format(
        formula=data.get("formula", formula),
        synced_at=synced_at,
        sources=sources_str,
    )

    with open(path, "w") as f:
        f.write(header + "\n\n")
        save_data = {k: v for k, v in data.items() if k != "material_id"}
        yaml.dump(save_data, f, allow_unicode=True, sort_keys=False, width=200)


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

    futures = {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        # MP — OPTIMADE优先（formula查询），备选REST（material_id或formula）
        if "materials_project" not in databases:
            f = executor.submit(mp_fetch, material_id=item.get("mp_id"), formula=item["formula"])
            futures["materials_project"] = f

        # JARVIS — 用 jarvis-tools 本地数据集，按 formula 查
        if "jarvis" not in databases:
            f = executor.submit(jarvis_fetch_by_formula, formula)
            futures["jarvis"] = f

        # OQMD
        if "oqmd_id" in item and "oqmd" not in databases:
            f = executor.submit(oqmd_fetch, oqmd_id=item["oqmd_id"])
            futures["oqmd"] = f

        # AFLOW
        if "aflow_id" in item and "aflow" not in databases:
            f = executor.submit(aflow_fetch, aflow_id=item["aflow_id"])
            futures["aflow"] = f

        # NOMAD
        if "nomad_id" in item and "nomad" not in databases:
            f = executor.submit(nomad_fetch, nomad_id=item["nomad_id"])
            futures["nomad"] = f
        elif "formula" in item and "nomad" not in databases:
            f = executor.submit(nomad_fetch, formula=item["formula"])
            futures["nomad"] = f

    for src, fut in futures.items():
        rate_limit(0.5)
        result = fut.result()
        if result:
            # formula 验证（防止查错材料）
            fetched_formula = result.formula
            if fetched_formula and fetched_formula != formula:
                log.warning(f"[{src}] fetched '{fetched_formula}' != expected '{formula}', skipping")
                continue
            databases[src] = result.to_dict()
            updated = True
            log.info(f"  [{src}] {result.material_id or formula} ← {result.formula}")

    # 计算差异和覆盖率
    discrepancies = compute_discrepancies(databases)
    coverage = compute_coverage(databases)

    data = {
        "formula": formula,
        "databases": databases,
        "cross_db_discrepancies": discrepancies,
        "coverage": coverage,
        "last_updated": time.strftime("%Y-%m-%d"),
    }

    save_yaml(formula, data, sources=list(databases.keys()))

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
            import traceback
            traceback.print_exc()

    updated_count = sum(1 for r in results if r["updated"])
    log.info(f"Done. {updated_count}/{len(results)} materials updated.")


if __name__ == "__main__":
    main()
