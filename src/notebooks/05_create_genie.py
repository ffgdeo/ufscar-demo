# Databricks notebook source

# MAGIC %md
# MAGIC # Criar Genie Space

# COMMAND ----------

import requests, json

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

SPACE_NAME = "Sistema Acadêmico Inteligente"
TABLES = [
    "workspace.sistema_academico.gold_desempenho_aluno",
    "workspace.sistema_academico.gold_desempenho_disciplina",
    "workspace.sistema_academico.gold_alunos_em_risco",
    "workspace.sistema_academico.matriculas",
    "workspace.sistema_academico.disciplinas",
    "workspace.sistema_academico.alunos",
]
SAMPLE_QUESTIONS = [
    "Quais disciplinas têm a maior taxa de reprovação?",
    "Quantos alunos estão em risco alto no semestre atual?",
    "Qual a média de notas de Cálculo 2 por semestre?",
    "Quais cursos têm o melhor CRA médio?",
    "Mostre a evolução do desempenho do aluno 42",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for existing space

# COMMAND ----------

existing_id = None

# Try listing via data-rooms API (the one that actually works for Genie)
for api_path in ["/api/2.0/data-rooms", "/api/2.0/genie/spaces"]:
    r = requests.get(f"https://{host}{api_path}", headers=headers)
    if r.status_code == 200:
        data = r.json()
        spaces = data.get("spaces", data.get("data_rooms", data.get("genie_spaces", [])))
        for s in spaces:
            title = s.get("title", s.get("display_name", ""))
            if title == SPACE_NAME:
                existing_id = s.get("space_id", s.get("id", ""))
                print(f"Found existing Genie Space: {existing_id}")
                break
    if existing_id:
        break

if existing_id:
    print(f"Genie Space already exists: {existing_id}")
    dbutils.notebook.exit(json.dumps({"space_id": existing_id, "status": "exists"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get warehouse ID

# COMMAND ----------

r = requests.get(f"https://{host}/api/2.0/sql/warehouses", headers=headers)
warehouses = r.json().get("warehouses", [])
wh_id = warehouses[0]["id"] if warehouses else None
assert wh_id, "No SQL warehouse found!"
print(f"Using warehouse: {wh_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Genie Space

# COMMAND ----------

# The Genie creation API uses the data-rooms endpoint
payload = {
    "display_name": SPACE_NAME,
    "description": "Explore dados acadêmicos da universidade usando linguagem natural. Pergunte sobre notas, desempenho de alunos, taxas de aprovação, alunos em risco e muito mais.",
    "warehouse_id": wh_id,
    "table_identifiers": TABLES,
    "sample_questions": SAMPLE_QUESTIONS,
}

# Try multiple API paths — the correct one depends on the workspace version
space_id = None
for api_path in ["/api/2.0/data-rooms", "/api/2.0/genie/spaces"]:
    r = requests.post(f"https://{host}{api_path}", headers=headers, json=payload)
    if r.status_code == 200:
        resp = r.json()
        space_id = resp.get("space_id", resp.get("id", ""))
        if space_id:
            print(f"✅ Genie Space created via {api_path}: {space_id}")
            break

    # Some APIs use "title" instead of "display_name"
    payload_alt = dict(payload)
    payload_alt["title"] = payload_alt.pop("display_name")
    r = requests.post(f"https://{host}{api_path}", headers=headers, json=payload_alt)
    if r.status_code == 200:
        resp = r.json()
        space_id = resp.get("space_id", resp.get("id", ""))
        if space_id:
            print(f"✅ Genie Space created via {api_path} (alt): {space_id}")
            break

if not space_id:
    print(f"⚠️ Could not create Genie Space automatically.")
    print(f"Last response: {r.status_code} {r.text[:500]}")
    print(f"Create it manually from the Databricks UI: Genie → New Space")
else:
    dbutils.notebook.exit(json.dumps({"space_id": space_id, "status": "created"}))
