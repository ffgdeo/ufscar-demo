# Databricks notebook source

# MAGIC %md
# MAGIC # RAG — Assistente de Provas Anteriores
# MAGIC Busca semântica sobre PDFs de provas usando Vector Search + LLM.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.sistema_academico.exam_chunks AS
# MAGIC WITH parsed AS (
# MAGIC   SELECT
# MAGIC     regexp_extract(path, '([^/]+)\\.pdf$', 1) AS exam_filename,
# MAGIC     path,
# MAGIC     CAST(ai_parse_document(content, map('mode', 'TEXT')) AS STRING) AS raw_json
# MAGIC   FROM read_files('/Volumes/workspace/sistema_academico/staging/exams/', format => 'binaryFile')
# MAGIC ),
# MAGIC extracted AS (
# MAGIC   SELECT exam_filename, path,
# MAGIC     concat_ws('\n\n', transform(
# MAGIC       from_json(raw_json, 'document STRUCT<elements ARRAY<STRUCT<content STRING, type STRING>>>').document.elements,
# MAGIC       x -> x.content
# MAGIC     )) AS full_text
# MAGIC   FROM parsed
# MAGIC )
# MAGIC SELECT monotonically_increasing_id() AS chunk_id, exam_filename, path, full_text AS chunk
# MAGIC FROM extracted WHERE length(full_text) > 50;
# MAGIC
# MAGIC ALTER TABLE workspace.sistema_academico.exam_chunks SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT exam_filename, length(chunk) as chars FROM workspace.sistema_academico.exam_chunks ORDER BY exam_filename

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vector Search via REST API

# COMMAND ----------

import requests, time, json

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

VS_ENDPOINT = "exam-search-endpoint"
VS_INDEX = "workspace.sistema_academico.exam_chunks_vs_index"

# COMMAND ----------

# Create or get endpoint
r = requests.get(f"https://{host}/api/2.0/vector-search/endpoints/{VS_ENDPOINT}", headers=headers)
if r.status_code == 404:
    print(f"Creating endpoint '{VS_ENDPOINT}'...")
    r = requests.post(f"https://{host}/api/2.0/vector-search/endpoints", headers=headers,
        json={"name": VS_ENDPOINT, "endpoint_type": "STANDARD"})
    print(r.json())
else:
    print(f"Endpoint exists: {r.json().get('endpoint_status', {}).get('state')}")

# Wait for ONLINE
for i in range(30):
    r = requests.get(f"https://{host}/api/2.0/vector-search/endpoints/{VS_ENDPOINT}", headers=headers)
    state = r.json().get("endpoint_status", {}).get("state", "UNKNOWN")
    print(f"  [{i}] {state}")
    if state == "ONLINE":
        break
    time.sleep(20)

# COMMAND ----------

# Create or get index
r = requests.get(f"https://{host}/api/2.0/vector-search/indexes/{VS_INDEX}", headers=headers)
if r.status_code == 404:
    print(f"Creating index '{VS_INDEX}'...")
    r = requests.post(f"https://{host}/api/2.0/vector-search/indexes", headers=headers, json={
        "name": VS_INDEX,
        "endpoint_name": VS_ENDPOINT,
        "primary_key": "chunk_id",
        "index_type": "DELTA_SYNC",
        "delta_sync_index_spec": {
            "source_table": "workspace.sistema_academico.exam_chunks",
            "embedding_source_columns": [{"name": "chunk", "embedding_model_endpoint_name": "databricks-gte-large-en"}],
            "pipeline_type": "TRIGGERED",
        }
    })
    print(r.json())
else:
    print(f"Index exists, ready={r.json().get('status', {}).get('ready')}")
    # Trigger sync
    requests.post(f"https://{host}/api/2.0/vector-search/indexes/{VS_INDEX}/sync", headers=headers)

# Wait for index ready
for i in range(40):
    r = requests.get(f"https://{host}/api/2.0/vector-search/indexes/{VS_INDEX}", headers=headers)
    ready = r.json().get("status", {}).get("ready", False)
    msg = r.json().get("status", {}).get("message", "")
    print(f"  [{i}] ready={ready} — {msg}")
    if ready:
        break
    time.sleep(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Search + RAG

# COMMAND ----------

def vs_query(query_text, n=3):
    r = requests.post(f"https://{host}/api/2.0/vector-search/indexes/{VS_INDEX}/query",
        headers=headers, json={"columns": ["exam_filename","chunk"], "query_text": query_text, "num_results": n})
    return r.json().get("result", {}).get("data_array", [])

results = vs_query("Quais tópicos caíram na prova de Cálculo?")
for r in results:
    print(f"\n📄 {r[0]}\n   {r[1][:200]}...")

# COMMAND ----------

def ask(question, n=3):
    results = vs_query(question, n)
    ctx = "\n---\n".join([f"[{r[0]}] {r[1]}" for r in results])
    prompt = f"""Você é um assistente acadêmico da UFSCar. Responda baseado APENAS no contexto abaixo. Português.
Contexto: {ctx}
Pergunta: {question}
Resposta:""".replace("'","''")
    return spark.sql(f"SELECT ai_query('databricks-meta-llama-3-3-70b-instruct','{prompt}') AS r").collect()[0]["r"]

# COMMAND ----------

for q in ["Quais tópicos caíram na P1 de Cálculo 1 em 2024?",
          "A prova de Banco de Dados tem questões sobre normalização?",
          "Que tipo de exercício aparece na prova de IA?"]:
    print(f"❓ {q}\n💡 {ask(q)}\n")

# COMMAND ----------

print("✅ RAG pipeline complete!")
