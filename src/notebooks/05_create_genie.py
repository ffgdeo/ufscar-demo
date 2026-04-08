# Databricks notebook source

# MAGIC %md
# MAGIC # Deploy Genie Space
# MAGIC Deploys a Genie Space from a versioned JSON definition file.
# MAGIC Pattern from: databricks-field-eng/sts-dw-repo/ai-bi-citizen/genie-cicd

# COMMAND ----------

import requests, json, os

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
base = f"https://{host}/api/2.0"

SPACE_NAME = "Sistema Acadêmico Inteligente"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load definition from versioned JSON

# COMMAND ----------

# The JSON file is in ../genie_definition/ relative to this notebook
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
base_dir = "/".join(notebook_path.rsplit("/", 2)[:-2])  # go up from notebooks/
definition_path = f"{base_dir}/genie_definition/genie_space.json"

print(f"Loading definition from: {definition_path}")

# Read the workspace file
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
import io

export_resp = w.workspace.export(definition_path, format="AUTO")
serialized_space = export_resp.content.decode("utf-8") if isinstance(export_resp.content, bytes) else export_resp.content

# Validate it's valid JSON
parsed = json.loads(serialized_space)
n_tables = len(parsed.get("data_sources", {}).get("tables", []))
n_sql = len(parsed.get("instructions", {}).get("example_question_sqls", []))
n_questions = len(parsed.get("config", {}).get("sample_questions", []))
print(f"  Tables: {n_tables}, SQL examples: {n_sql}, Sample questions: {n_questions}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get warehouse

# COMMAND ----------

r = requests.get(f"{base}/sql/warehouses", headers=headers)
wh_id = r.json()["warehouses"][0]["id"]
print(f"Warehouse: {wh_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check existing space

# COMMAND ----------

existing_id = None
r = requests.get(f"{base}/genie/spaces", headers=headers)
if r.status_code == 200:
    for s in r.json().get("spaces", []):
        if s.get("title") == SPACE_NAME:
            existing_id = s["space_id"]
            print(f"Found existing: {existing_id}")
            break

if not existing_id:
    print("No existing space — will create new")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or Update

# COMMAND ----------

if existing_id:
    r = requests.patch(f"{base}/genie/spaces/{existing_id}", headers=headers, json={
        "title": SPACE_NAME,
        "warehouse_id": wh_id,
        "serialized_space": serialized_space,
    })
    if r.status_code == 200:
        print(f"✅ Updated: {existing_id}")
        print(f"URL: https://{host}/genie/rooms/{existing_id}")
        dbutils.notebook.exit(json.dumps({"space_id": existing_id, "status": "updated"}))
    else:
        print(f"Update failed ({r.status_code}: {r.text[:200]}), creating new...")
        existing_id = None

if not existing_id:
    r = requests.post(f"{base}/genie/spaces", headers=headers, json={
        "title": SPACE_NAME,
        "description": "Explore dados acadêmicos da universidade usando linguagem natural.",
        "warehouse_id": wh_id,
        "serialized_space": serialized_space,
    })
    if r.status_code == 200:
        space_id = r.json().get("space_id", "")
        print(f"✅ Created: {space_id}")
        print(f"URL: https://{host}/genie/rooms/{space_id}")
        dbutils.notebook.exit(json.dumps({"space_id": space_id, "status": "created"}))
    else:
        raise Exception(f"Failed to create Genie Space: {r.status_code} {r.text}")
