# Databricks notebook source

# MAGIC %md
# MAGIC # Upload de PDFs de Provas
# MAGIC Faz upload dos PDFs de provas para o Volume do Unity Catalog.

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Find the exam PDFs — they're bundled alongside the notebooks
# In a DAB deployment, the source files are synced to the workspace
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
base_dir = "/".join(notebook_path.rsplit("/", 1)[:-1])

# The exams are in ../data/exams/ relative to the notebooks folder
exams_workspace_dir = base_dir.replace("/notebooks", "/data/exams")
volume_target = "/Volumes/workspace/sistema_academico/staging/exams"

print(f"Source: {exams_workspace_dir}")
print(f"Target: {volume_target}")

# COMMAND ----------

# List and upload files
from databricks.sdk.service.workspace import ExportFormat

items = w.workspace.list(exams_workspace_dir)
uploaded = 0
for item in items:
    if item.path.endswith(".pdf"):
        fname = item.path.rsplit("/", 1)[-1]
        # Export file content from workspace
        content = w.workspace.export(item.path, format=ExportFormat.AUTO)
        # Upload to volume
        target_path = f"{volume_target}/{fname}"
        w.files.upload(target_path, content.content, overwrite=True)
        print(f"  ✅ {fname}")
        uploaded += 1

print(f"\n{uploaded} PDFs uploaded to {volume_target}")
