#!/usr/bin/env python3
"""
Materials Federation Sync
从多个材料数据库拉取数据，统一格式写入 materials/{formula}.yaml
"""

import os
import yaml
import json
import time
import requests
from pathlib import Path
from tqdm import tqdm

# ── 配置 ──────────────────────────────────────────────
REPO_DIR = Path(__file__).parent.parent
MATERIALS_DIR = REPO_DIR / "materials"
HEADERS = {"User-Agent": "materials-federation/1.0 (contact@yourdomain.com)"}

# ── 工具函数 ──────────────────────────────────────────
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
        yaml.dump(data, f, allow_unicode=True, sort_keys=False)

def rate_limit(n: int = 3):
    """每次请求前等待，遵守各数据库限制"""
    time.sleep(n)

# ── Materials Project ─────────────────────────────────
def fetch_mp_material(mp_id: str, api_key: str = None) -> dict:
    """通过 MPRester API 获取材料数据"""
    try:
        from mp_api.client import MPRester
        api_key = api_key or os.environ.get("MP_API_KEY", "")
        if not api_key:
            return {}
        with MPRester(api_key=api_key) as mpr:
            doc = mpr.get_document_by_id(mp_id)
            return {
                "material_id": str(doc.material_id),
                "formula": doc.formula_pretty,
                "band_gap": doc.band_gap,
                "formation_energy": doc.formation_energy_per_atom,
                "structure": str(doc.structure),
            }
    except Exception as e:
        print(f"  [MP] {mp_id} failed: {e}")
        return {}

# ── JARVIS ────────────────────────────────────────────
def fetch_jarvis_material(jarvis_id: str) -> dict:
    """通过 JARVIS REST API 获取"""
    try:
        url = f"https://jarvis.nist.gov/api/jarvis/v1/material/{jarvis_id}"
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        d = r.json()
        return {
            "jarvis_id": jarvis_id,
            "formula": d.get("formula"),
            "band_gap": d.get("bandgap"),
            "formation_energy": d.get("formation_energy"),
        }
    except Exception as e:
        print(f"  [JARVIS] {jarvis_id} failed: {e}")
        return {}

# ── OQMD ──────────────────────────────────────────────
def fetch_oqmd_material(oqmd_id: int) -> dict:
    """通过 OQMD API 获取"""
    try:
        url = f"https://oqmd.org/oqmdapi/v1/calculations/{oqmd_id}"
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        d = r.json()
        return {
            "oqmd_id": oqmd_id,
            "formula": d.get("composition"),
            "band_gap": d.get("band_gap"),
            "formation_energy": d.get("formation_energy"),
        }
    except Exception as e:
        print(f"  [OQMD] {oqmd_id} failed: {e}")
        return {}

# ── 主流程 ────────────────────────────────────────────
def main():
    print("Materials Federation Sync started")

    # 示例：从文件读取待同步列表（formula -> known ids）
    # 实际由 cron 每天增量扫描
    sync_list = []
    list_path = REPO_DIR / "sync_list.json"
    if list_path.exists():
        with open(list_path) as f:
            sync_list = json.load(f)

    for item in tqdm(sync_list, desc="Syncing"):
        formula = item["formula"]
        existing = load_yaml(formula)

        databases = existing.get("databases", {})

        # 补全 MP
        if "mp_id" in item and "materials_project" not in databases:
            rate_limit(3)
            mp_data = fetch_mp_material(item["mp_id"])
            if mp_data:
                databases["materials_project"] = mp_data

        # 补全 JARVIS
        if "jarvis_id" in item and "jarvis" not in databases:
            rate_limit(3)
            jarvis_data = fetch_jarvis_material(item["jarvis_id"])
            if jarvis_data:
                databases["jarvis"] = jarvis_data

        # 补全 OQMD
        if "oqmd_id" in item and "oqmd" not in databases:
            rate_limit(3)
            oqmd_data = fetch_oqmd_material(item["oqmd_id"])
            if oqmd_data:
                databases["oqmd"] = oqmd_data

        # 计算差异
        discrepancies = compute_discrepancies(databases)

        # 合并写入
        data = {
            "material_id": formula,
            "formula": formula,
            "databases": databases,
            "cross_db_discrepancies": discrepancies,
            "last_updated": time.strftime("%Y-%m-%d"),
        }
        save_yaml(formula, data)

    print(f"Done. Synced {len(sync_list)} materials.")

def compute_discrepancies(databases: dict) -> dict:
    """检测跨库数值差异"""
    numeric_props = ["band_gap", "formation_energy", "bulk_modulus"]
    discrepancies = {}

    for prop in numeric_props:
        values = {}
        for db, data in databases.items():
            v = data.get(prop)
            if v is not None:
                values[db] = v
        if len(values) >= 2:
            vals = list(values.values())
            diff = max(vals) - min(vals)
            discrepancies[prop] = {
                "max_diff": round(diff, 4),
                "values": values,
                "sources": list(values.keys()),
            }

    return discrepancies

if __name__ == "__main__":
    main()
