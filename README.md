# Materials Federation

> 跨材料数据库对齐 + 差异分析 / Cross materials database alignment and discrepancy analysis

## 目标

将 Materials Project、JARVIS、OQMD、AFLOW 等主流材料数据库**统一格式聚合**，自动检测同一材料在不同数据库中的**属性差异**，标注每个数据库的**数据覆盖率**。

## 数据格式

每个材料一个 YAML 文件（含 license header）：

```yaml
# ─────────────────────────────────────────────────────────────────────────────
# BaTiO3
# Synced at: 2026-05-25T10:30:00Z
# License: CC BY 4.0 (Materials Project), NIST Terms (JARVIS),
#          OQMD License (OQMD), AFLOW License (AFLOW),
#          CC BY 4.0 / Database Right (NOMAD)
# Disclaimer: Data sourced from third-party databases. All data remains the
#   intellectual property of respective database providers under their
#   applicable licenses. This aggregation is for research purposes only.
# Sources: materials_project, jarvis, oqmd
# ─────────────────────────────────────────────────────────────────────────────

formula: BaTiO3
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
    relative_diff_pct: 3.7
    sources: ["materials_project", "jarvis"]
coverage:
  common_properties: ["formula", "spacegroup", "formation_energy", "band_gap"]
  properties_unique_to_source:
    jarvis: ["bulk_modulus"]
last_updated: "2025-01-15"
```

## 数据许可证

| 数据库 | 许可证 | 关键条款 |
|--------|--------|----------|
| Materials Project | CC BY 4.0 | 署名即可使用，禁止用 MP 数据训练商业模型 |
| JARVIS (NIST) | NIST Terms | 联邦政府数据，商业使用受限 |
| OQMD | OQMD License | 需申请，商业使用需协议 |
| AFLOW | AFLOW License | 禁止直接镜像，商业使用需协议 |
| NOMAD | CC BY 4.0 | 署名即可，数据库权属于 NOMAD 联盟 |

**免责声明**：本仓库所有数据均来自第三方数据库，版权归各数据库提供商所有。本聚合仅用于研究目的。使用前请阅读各数据库的许可条款。

## 当前支持的数据源

| 数据库 | 状态 | 说明 |
|--------|------|------|
| Materials Project | ✅ 可用 | 需 MP_API_KEY |
| JARVIS (NIST) | ✅ 可用 | REST API |
| OQMD | ✅ 可用 | REST API |
| AFLOW | ✅ 可用 | REST API |
| NOMAD | ✅ 可用 | OPTIMADE API |

## 快速开始

```bash
# 安装依赖
pip install requests pyyaml tqdm

# 设置 API keys（部分数据源需要）
export MP_API_KEY=your_key_here

# 同步数据
python scripts/sync.py
```

## 目录结构

```
materials-federation/
├── materials/          # 每材料一个 YAML，含 license header
│   └── BaTiO3.yaml
├── scripts/
│   └── sync.py         # 核心同步脚本（无重型依赖）
├── sync_list.json      # 待同步材料列表
├── .github/
│   └── workflows/
│       └── sync.yml    # 每日 cron 自动同步
├── README.md
└── LICENSE
```

## 贡献

提交新材料或修正数据 → PR。
