# Databricks notebook source

# MAGIC %md
# MAGIC # Upload de PDFs de Provas
# MAGIC Copia os PDFs de provas do bundle para o Volume do Unity Catalog.

# COMMAND ----------

import os

# Find the exam PDFs in the bundle's synced workspace files
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# notebook_path is like /Workspace/Users/.../files/src/notebooks/02_upload_exams
# We need to go up to .../files/src/data/exams
base_dir = notebook_path.rsplit("/src/notebooks", 1)[0]
exams_ws_dir = f"{base_dir}/src/data/exams"

# On serverless, workspace files are accessible at /Workspace/<path>
local_dir = f"/Workspace{exams_ws_dir}"

print(f"Looking for PDFs in: {local_dir}")
print(f"Files found: {os.listdir(local_dir)}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import pathlib

w = WorkspaceClient()
volume_target = "/Volumes/workspace/sistema_academico/staging/exams"
uploaded = 0

for fname in sorted(os.listdir(local_dir)):
    if not fname.endswith(".pdf"):
        continue
    local_path = os.path.join(local_dir, fname)
    target_path = f"{volume_target}/{fname}"

    with open(local_path, "rb") as f:
        w.files.upload(target_path, f, overwrite=True)

    size = os.path.getsize(local_path)
    print(f"  ✅ {fname} ({size} bytes)")
    uploaded += 1

print(f"\n{uploaded} PDFs uploaded to {volume_target}")
