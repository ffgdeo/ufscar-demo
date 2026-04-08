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

### Option A: Deploy from Databricks Workspace UI

1. **Clone the repo into your workspace**
   - In the Databricks sidebar, go to **Workspace**
   - Click your user folder → **⋮** → **Create** → **Git Folder**
   - Paste the repo URL: `https://github.com/ffgdeo/ufscar-demo.git`
   - Branch: `main`, click **Create Git Folder**

2. **Configure the bundle target**
   - Open `databricks.yml` in the cloned folder
   - Update the `profile` under `targets.dev` to match your `~/.databrickscfg` profile name
   - If your warehouse has a different name, update the `warehouse_id` lookup

3. **Deploy from the terminal**
   - Open a notebook or the Web Terminal in your workspace
   - Navigate to the cloned folder:
     ```bash
     cd /Workspace/Users/<your-email>/ufscar-demo
     ```
   - Deploy and run:
     ```bash
     databricks bundle deploy --auto-approve
     databricks bundle run setup_job
     ```

4. **Wait for the setup job to complete** (~10–15 min)
   - Track progress in **Workflows → Jobs → "[dev] UFSCar Demo — Setup"**
   - The job creates all tables, runs the pipeline, trains the ML model, sets up RAG, and creates the Genie Space

5. **Explore the results**
   - **Dashboard**: Workspace → search "Painel Acadêmico"
   - **Genie Space**: Genie → "Sistema Acadêmico Inteligente"
   - **Data**: Catalog → `workspace.sistema_academico`
   - **Pipeline**: Workflows → Delta Live Tables → "Pipeline Acadêmico"

### Option B: Deploy from your local machine

```bash
# 1. Clone the repo
git clone https://github.com/ffgdeo/ufscar-demo.git
cd ufscar-demo

# 2. Make sure your Databricks CLI is configured
#    (profile name must match targets.dev.workspace.profile in databricks.yml)
databricks auth login --host https://<your-workspace>.cloud.databricks.com

# 3. Validate, deploy, and run
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

| Task | What it does | ~Time |
|---|---|---|
| `gerar_dados` | Creates schema, generates 800 students + 14.5K enrollments | ~1 min |
| `upload_exams` | Uploads 12 exam PDFs to UC Volume | ~30s |
| `run_pipeline` | Runs Declarative Pipeline (Bronze → Silver → Gold) | ~2 min |
| `ml_training` | Trains at-risk prediction model, saves predictions | ~1 min |
| `rag_setup` | Parses PDFs, creates Vector Search endpoint + index | ~10 min |
| `create_genie` | Creates Genie Space with instructions + SQL examples | ~10s |

## Data model

- **800 students** across 8 programs (CS, Engineering, Statistics, Math, Physics, etc.)
- **14,500 enrollment records** with realistic grade distributions
- **45 courses** with difficulty ratings (Cálculo 2 at 48% pass rate)
- **12 synthetic exam PDFs** for RAG

## Cleanup

```bash
databricks bundle destroy --auto-approve
```

This removes all deployed resources (jobs, pipeline, dashboard) but does **not** drop the schema or data tables. To fully clean up:

```sql
DROP SCHEMA workspace.sistema_academico CASCADE;
```

## Requirements

- Databricks workspace (Free Edition works)
- Databricks CLI v0.200+ configured with a profile
- SQL warehouse (serverless)
