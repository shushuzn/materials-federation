#!/usr/bin/env python3
"""
Materials Federation Sync — jarvis-tools + HuggingFace MP + OPTIMADE
主要数据源:
  - JARVIS: jarvis-tools (figshare 本地数据集, 75k+ 材料)
  - MP: HuggingFace colabfit/Materials_Project (parquet, 含 electronic_band_gap)
  - MP/OQMD/AFLOW/NOMAD: OPTIMADE / REST API
无重型依赖(pymatgen/mp-api)，只需 requests + pyyaml + jarvis-tools + pyarrow
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
_JARVIS_CACHE = {}  # formula -> jarvis entry dict


def _ensure_jarvis_cache():
    """懒加载 JARVIS 3D DFT 数据集到内存缓存"""
    if _JARVIS_CACHE:
        return
    try:
        from jarvis.db.figshare import data
        log.info("[JARVIS] Loading 3D DFT dataset from figshare (first run ~1 min)...")
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
    entry = _JARVIS_CACHE.get(formula)
    if not entry:
        return None
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


# ── HuggingFace MP 数据源 (colabfit/Materials_Project parquet) ─────────────
# 全局缓存: formula -> DbEntry
_MP_HF_CACHE = {}  # formula -> DbEntry
_MP_HF_SHARD_INDEX = {}  # formula -> shard index (for fast lookup next time)
_MP_HF_SHARD_COUNT = 64  # total number of co/*.parquet shards


def _mp_hf_shard_url(shard_idx: int) -> str:
    return f"https://huggingface.co/datasets/colabfit/Materials_Project/resolve/main/co/co_{shard_idx}.parquet"


def _mp_hf_download_shard(shard_idx: int, cache_dir: Path) -> Optional[Path]:
    """下载单个 parquet 分片到缓存目录"""
    url = _mp_hf_shard_url(shard_idx)
    dest = cache_dir / f"co_{shard_idx}.parquet"
    if dest.exists():
        return dest
    try:
        log.debug(f"[MP-HF] Downloading shard {shard_idx}/{_MP_HF_SHARD_COUNT}...")
        r = requests.get(url, headers=HEADERS, timeout=60)
        r.raise_for_status()
        dest.write_bytes(r.content)
        return dest
    except Exception as e:
        log.debug(f"[MP-HF] Failed to download shard {shard_idx}: {e}")
        return None


def _mp_hf_search_in_shard(shard_path: Path, formula: str) -> Optional[DbEntry]:
    """在单个分片中搜索 formula"""
    try:
        import pyarrow.parquet as pq
        # 只读取需要的列（避免加载全量数据）
        table = pq.read_table(
            str(shard_path),
            columns=["chemical_formula_reduced", "electronic_band_gap",
                     "chemical_formula_hill", "formation_energy"]
        )
        df = table.to_pandas()
        rows = df[df["chemical_formula_reduced"] == formula]
        if rows.empty:
            return None
        # 取第一条（可按 band_gap 非空优先）
        row = rows.sort_values("electronic_band_gap", ascending=False, na_position="last").iloc[0]
        return DbEntry(
            source="materials_project",
            material_id=str(row.get("property_id", formula)),
            formula=str(row["chemical_formula_reduced"]),
            band_gap=_float(row["electronic_band_gap"]) if row["electronic_band_gap"] is not None and not (isinstance(row["electronic_band_gap"], float) and str(row["electronic_band_gap"]) == "nan") else None,
            formation_energy=_float(row["formation_energy"]) if row["formation_energy"] is not None and not (isinstance(row["formation_energy"], float) and str(row["formation_energy"]) == "nan") else None,
            raw=row.to_dict(),
            last_updated=time.strftime("%Y-%m-%d"),
        )
    except Exception as e:
        log.debug(f"[MP-HF] Error reading shard {shard_path}: {e}")
        return None


def _mp_hf_load_full_index(cache_dir: Path) -> dict:
    """加载已缓存的分片索引"""
    index_file = cache_dir / "shard_index.json"
    if index_file.exists():
        try:
            with open(index_file) as f:
                return json.load(f)
        except:
            pass
    return {}


def _mp_hf_save_index(cache_dir: Path, index: dict):
    """保存分片索引"""
    cache_dir.mkdir(parents=True, exist_ok=True)
    with open(cache_dir / "shard_index.json", "w") as f:
        json.dump(index, f)


def mp_hf_fetch(formula: str) -> Optional[DbEntry]:
    """
    从 HuggingFace colabfit/Materials_Project parquet 分片查找 MP 数据。
    最多扫描 2 个分片/材料。分片索引缓存避免重复下载。
    """
    global _MP_HF_CACHE, _MP_HF_SHARD_INDEX

    if formula in _MP_HF_CACHE:
        return _MP_HF_CACHE[formula]

    cache_dir = Path("/tmp/mp_parquet_cache")
    shard_index = _mp_hf_load_full_index(cache_dir)

    # 确定扫描顺序：优先命中的分片，然后从后往前
    if formula in shard_index:
        shards_to_scan = [shard_index[formula]]
    else:
        shards_to_scan = list(range(_MP_HF_SHARD_COUNT - 1, _MP_HF_SHARD_COUNT - 3, -1))  # 最后2个分片

    for idx in shards_to_scan:
        shard_path = _mp_hf_download_shard(idx, cache_dir)
        if not shard_path:
            continue

        result = _mp_hf_search_in_shard(shard_path, formula)
        if result:
            _MP_HF_CACHE[formula] = result
            shard_index[formula] = idx
            _mp_hf_save_index(cache_dir, shard_index)
            log.info(f"  [mp-hf] {formula} in shard {idx}")
            return result

    return None


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
    """Materials Project — OPTIMADE endpoint + REST fallback"""
    # OPTIMADE
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
        log.debug("[MP] No API key, skipping REST")
        return None

    # REST API
    query = material_id or formula
    url = f"https://materialsproject.org/rest/v2/materials/{query}/vasp"
    try:
        headers = {"X-API-Key": api_key, **HEADERS}
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 404:
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
        if v is None:
            return None
        f = float(v)
        # filter out NaN
        if f != f:  # NaN check
            return None
        return f
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

    # ── JARVIS (强制同步，jarvis-tools 本地数据集) ──────────────
    _ensure_jarvis_cache()
    if formula in _JARVIS_CACHE:
        entry = _JARVIS_CACHE[formula]
        result = DbEntry(
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
        databases["jarvis"] = result.to_dict()
        updated = True
        log.info(f"  [jarvis] {result.material_id} ← {result.formula}")

    futures = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        # MP — OPTIMADE/REST (可能 IP-blocked)
        if "materials_project" not in databases:
            f = executor.submit(mp_fetch, material_id=item.get("mp_id"), formula=item["formula"])
            futures["materials_project"] = f

        # MP HuggingFace parquet (补充 MP band_gap)
        if "materials_project" not in databases:
            f = executor.submit(mp_hf_fetch, formula)
            futures["mp_hf"] = f

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
        rate_limit(0.3)
        try:
            result = fut.result()
        except Exception as e:
            log.debug(f"[{src}] exception: {e}")
            continue
        if result:
            fetched_formula = result.formula
            if fetched_formula and fetched_formula != formula:
                log.warning(f"[{src}] fetched '{fetched_formula}' != expected '{formula}', skipping")
                continue
            databases[result.source] = result.to_dict()
            updated = True
            log.info(f"  [{result.source}] {result.material_id or formula} ← {result.formula}")

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
