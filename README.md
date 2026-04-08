# Sistema Acadêmico Inteligente — Databricks Demo

End-to-end data intelligence demo for university grading analytics, built on Databricks Free Edition.

## What it deploys

| Component | Description |
|---|---|
| **Declarative Pipeline** | Medallion architecture: Bronze → Silver → Gold |
| **AI/BI Dashboard** | 3-page analytics dashboard (overview, at-risk students, departments) |
| **Genie Space** | Natural language queries over academic data (Portuguese) |
| **ML Model** | Gradient Boosting to predict at-risk students from P1 grades |
| **RAG Pipeline** | Semantic search over exam PDFs via Vector Search + LLM |

## Quick start

```bash
# Configure your workspace profile in ~/.databrickscfg
databricks bundle validate
databricks bundle deploy --auto-approve
databricks bundle run setup_job
```

## Setup job tasks

```
gerar_dados ──→ upload_exams ──→ rag_setup
     │
     └──→ run_pipeline ──→ ml_training
                    │
                    └──→ create_genie
```

## Data model

- **800 students** across 8 programs (CS, Engineering, Statistics, Math, Physics, etc.)
- **14,500 enrollment records** with realistic grade distributions
- **45 courses** with difficulty ratings (Cálculo 2 at 48% pass rate)
- **12 synthetic exam PDFs** for RAG

## Requirements

- Databricks workspace (Free Edition works)
- Databricks CLI configured with a profile
- SQL warehouse (serverless)
