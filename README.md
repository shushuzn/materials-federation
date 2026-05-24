# Materials Federation

> 跨材料数据库对齐 + 差异分析 / Cross materials database alignment and discrepancy analysis

## 目标

将 Materials Project、JARVIS、OQMD、AFLOW 等主流材料数据库**统一格式聚合**，自动检测同一材料在不同数据库中的**属性差异**，标注每个数据库的**数据覆盖率**。

## 数据格式

每个材料一个 YAML 文件：

```yaml
material_id: "BaTiO3"
formula: "BaTiO3"
databases:
  materials_project:
    material_id: "mp-2998"
    band_gap: 2.7
    formation_energy: -3.2
  jarvis:
    jarvis_id: "JVASP-1002"
    band_gap: 2.6
    formation_energy: -3.1
    bulk_modulus: 162
cross_db_discrepancies:
  band_gap:
    max_diff: 0.1
    sources: ["materials_project", "jarvis"]
coverage:
  common_properties: ["formula", "spacegroup", "formation_energy", "band_gap"]
  properties_only_in: { "jarvis": ["bulk_modulus"] }
last_updated: "2025-01-15"
```

## 当前支持的数据源

| 数据库 | 状态 | 说明 |
|--------|------|------|
| Materials Project | ✅ 规划 | 需 MP_API_KEY |
| JARVIS (NIST) | ✅ 规划 | REST API |
| OQMD | ✅ 规划 | REST API |
| AFLOW | 🔜 规划 | REST API |
| NOMAD | 🔜 规划 | OPTIMADE |

## 快速开始

```bash
# 同步数据
pip install pymatgen mp-api requests pyyaml tqdm
export MP_API_KEY=your_key_here
python scripts/sync.py
```

## 贡献

提交新材料或修正数据 → PR。
