# Databricks notebook source

# MAGIC %md
# MAGIC # Upload de PDFs de Provas
# MAGIC Copia os PDFs de provas do bundle para o Volume do Unity Catalog.

# COMMAND ----------

import os

# The exam PDFs are in ../data/exams/ relative to the notebooks folder.
# In a DAB deployment, all files are synced under the bundle's workspace path.
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
base_dir = "/".join(notebook_path.rsplit("/", 2)[:-2])  # up from src/notebooks/
exams_source = f"/Workspace{base_dir}/src/data/exams"
volume_target = "/Volumes/workspace/sistema_academico/staging/exams"

print(f"Source: {exams_source}")
print(f"Target: {volume_target}")

# COMMAND ----------

# List source files and copy to volume using dbutils.fs.cp (preserves binary content)
files = dbutils.fs.ls(f"file:{exams_source}")
uploaded = 0

for f in files:
    if f.name.endswith(".pdf"):
        src = f"file:{exams_source}/{f.name}"
        dst = f"dbfs:{volume_target}/{f.name}"
        dbutils.fs.cp(src, dst, recurse=False)
        print(f"  ✅ {f.name}")
        uploaded += 1

print(f"\n{uploaded} PDFs uploaded to {volume_target}")
