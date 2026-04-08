# Databricks notebook source

# MAGIC %md
# MAGIC # Deploy Genie Space

# COMMAND ----------

import requests, json, uuid

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
base = f"https://{host}/api/2.0"

def uid():
    return uuid.uuid4().hex

SPACE_NAME = "Sistema Acadêmico Inteligente"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie Space Definition

# COMMAND ----------

sql_ids = sorted([uid() for _ in range(6)])
q_ids = sorted([uid() for _ in range(8)])

serialized_space = json.dumps({
    "version": 2,
    "config": {
        "sample_questions": sorted([
            {"id": q_ids[0], "question": ["Quais disciplinas têm a maior taxa de reprovação?"]},
            {"id": q_ids[1], "question": ["Quantos alunos estão em risco alto no semestre 2025/1?"]},
            {"id": q_ids[2], "question": ["Qual a média de notas de Cálculo 2 por semestre?"]},
            {"id": q_ids[3], "question": ["Quais cursos têm o melhor CRA médio?"]},
            {"id": q_ids[4], "question": ["Mostre a evolução do desempenho do aluno 42"]},
            {"id": q_ids[5], "question": ["Quais alunos de Ciência da Computação reprovaram em Cálculo 2?"]},
            {"id": q_ids[6], "question": ["Qual professor tem a melhor taxa de aprovação?"]},
            {"id": q_ids[7], "question": ["Quantos alunos estão ativos por curso?"]},
        ], key=lambda x: x["id"]),
    },
    "instructions": {
        "text_instructions": sorted([
            {"id": uid(), "content": [
                "Você é o assistente acadêmico da UFSCar (Universidade Federal de São Carlos).\n",
                "Responda SEMPRE em português brasileiro.\n",
            ]},
            {"id": uid(), "content": [
                "## Regras do sistema acadêmico\n",
                "- Escala de notas: 0 a 10\n",
                "- nota_final = média aritmética de nota_p1 e nota_p2\n",
                "- Aprovação requer nota_final >= 6.0 E frequência >= 75%\n",
                "- Situações: aprovado, reprovado_nota, reprovado_frequencia, reprovado_nota_freq, trancado\n",
                "- Semestres: 2023/1, 2023/2, 2024/1, 2024/2, 2025/1\n",
                "- CRA = Coeficiente de Rendimento Acadêmico = média acumulada das notas finais\n",
            ]},
            {"id": uid(), "content": [
                "## Joins entre tabelas\n",
                "- matriculas.aluno_id = alunos.aluno_id (matrícula → aluno)\n",
                "- matriculas.disciplina_id = disciplinas.disciplina_id (matrícula → disciplina)\n",
                "- matriculas.professor_id = professores.professor_id (matrícula → professor)\n",
                "- As tabelas gold_ já têm joins pré-feitos (aluno_nome, curso_sigla, etc.)\n",
            ]},
            {"id": uid(), "content": [
                "## Dicas importantes\n",
                "- Sempre filtre situacao != 'trancado' ao calcular taxas de aprovação/reprovação\n",
                "- 'Cálculo 2' = código MAT102 na tabela disciplinas\n",
                "- Para risco de alunos, use gold_alunos_em_risco (nivel_risco: ALTO/MÉDIO/BAIXO)\n",
                "- Para evolução de um aluno, use gold_desempenho_aluno filtrado por aluno_id\n",
                "- dificuldade em disciplinas: 0 (fácil) a 1 (muito difícil)\n",
            ]},
        ], key=lambda x: x["id"]),
        "example_question_sqls": sorted([
            {"id": sql_ids[0], "question": ["Top 10 disciplinas com maior taxa de reprovação"], "sql": [
                "SELECT d.codigo, d.nome, COUNT(*) as total, ROUND(SUM(CASE WHEN m.situacao LIKE 'reprovado%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as taxa_reprovacao FROM workspace.sistema_academico.matriculas m JOIN workspace.sistema_academico.disciplinas d ON m.disciplina_id = d.disciplina_id WHERE m.situacao != 'trancado' GROUP BY d.codigo, d.nome ORDER BY taxa_reprovacao DESC LIMIT 10"
            ]},
            {"id": sql_ids[1], "question": ["Alunos em risco alto no semestre atual"], "sql": [
                "SELECT aluno_nome, curso_sigla, media_atual, freq_atual, total_reprovacoes_historico, score_risco FROM workspace.sistema_academico.gold_alunos_em_risco WHERE nivel_risco = 'ALTO' ORDER BY score_risco DESC"
            ]},
            {"id": sql_ids[2], "question": ["Evolução de Cálculo 2 por semestre"], "sql": [
                "SELECT m.semestre, ROUND(AVG(m.nota_final), 2) as media, ROUND(AVG(m.nota_p1), 2) as media_p1, ROUND(AVG(m.nota_p2), 2) as media_p2, COUNT(*) as alunos FROM workspace.sistema_academico.matriculas m JOIN workspace.sistema_academico.disciplinas d ON m.disciplina_id = d.disciplina_id WHERE d.codigo = 'MAT102' AND m.situacao != 'trancado' GROUP BY m.semestre ORDER BY m.semestre"
            ]},
            {"id": sql_ids[3], "question": ["CRA médio por curso"], "sql": [
                "SELECT curso_sigla, ROUND(AVG(cra_acumulado), 2) as cra_medio, COUNT(DISTINCT aluno_id) as alunos FROM workspace.sistema_academico.gold_desempenho_aluno GROUP BY curso_sigla ORDER BY cra_medio DESC"
            ]},
            {"id": sql_ids[4], "question": ["Alunos que passaram na P1 mas reprovaram no final"], "sql": [
                "SELECT a.nome, d.codigo, d.nome as disciplina, m.nota_p1, m.nota_p2, m.nota_final, m.frequencia_pct, m.situacao FROM workspace.sistema_academico.matriculas m JOIN workspace.sistema_academico.alunos a ON m.aluno_id = a.aluno_id JOIN workspace.sistema_academico.disciplinas d ON m.disciplina_id = d.disciplina_id WHERE m.nota_p1 >= 6.0 AND m.situacao LIKE 'reprovado%' ORDER BY m.nota_p1 DESC LIMIT 20"
            ]},
            {"id": sql_ids[5], "question": ["Distribuição de alunos por curso no semestre atual"], "sql": [
                "SELECT curso_sigla, curso_nome, COUNT(DISTINCT aluno_id) as total FROM workspace.sistema_academico.gold_desempenho_aluno WHERE semestre = '2025/1' GROUP BY curso_sigla, curso_nome ORDER BY total DESC"
            ]},
        ], key=lambda x: x["id"]),
    },
    "data_sources": {
        "tables": sorted([
            {"identifier": "workspace.sistema_academico.matriculas", "description": [
                "Tabela fato principal: cada linha = 1 aluno × 1 disciplina × 1 semestre. "
                "nota_final = média(nota_p1, nota_p2). Aprovação requer nota_final >= 6.0 E frequencia_pct >= 75%. "
                "situacao: aprovado, reprovado_nota, reprovado_frequencia, reprovado_nota_freq, trancado. "
                "Sempre filtre situacao != 'trancado' ao calcular taxas. "
                "JOIN com alunos via aluno_id. JOIN com disciplinas via disciplina_id. JOIN com professores via professor_id."
            ]},
            {"identifier": "workspace.sistema_academico.alunos", "description": [
                "Cadastro de 800 estudantes. status: ativo, formado, trancado. ano_ingresso: 2021-2024. "
                "curso_id identifica o programa. Use gold_desempenho_aluno para curso_sigla sem JOIN."
            ]},
            {"identifier": "workspace.sistema_academico.disciplinas", "description": [
                "Catálogo de 45 disciplinas. codigo identifica (ex: MAT102 = Cálculo 2, CMP201 = Algoritmos). "
                "dificuldade: 0.0 (fácil) a 1.0 (difícil). JOIN com matriculas via disciplina_id."
            ]},
            {"identifier": "workspace.sistema_academico.gold_desempenho_aluno", "description": [
                "Performance por aluno/semestre. Já tem aluno_nome, curso_sigla (sem JOIN). "
                "cra_acumulado = média acumulada. Use para evolução de alunos e ranking de CRA."
            ]},
            {"identifier": "workspace.sistema_academico.gold_desempenho_disciplina", "description": [
                "Métricas por disciplina/semestre. Já tem disciplina_codigo, departamento_sigla. "
                "taxa_aprovacao (0-100). media_nota, media_p1, media_p2 (0-10)."
            ]},
            {"identifier": "workspace.sistema_academico.gold_alunos_em_risco", "description": [
                "Alunos em risco semestre 2025/1. nivel_risco: ALTO/MÉDIO/BAIXO. "
                "score_risco 0-100. Já tem aluno_nome, curso_sigla, media_atual, freq_atual."
            ]},
        ], key=lambda t: t["identifier"]),
    },
})

print(f"Definition ready: {len(serialized_space)} chars")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get warehouse

# COMMAND ----------

r = requests.get(f"{base}/sql/warehouses", headers=headers)
wh_id = r.json()["warehouses"][0]["id"]
print(f"Warehouse: {wh_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check existing

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
        print(f"Update failed ({r.status_code}), creating new...")
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
        raise Exception(f"Failed: {r.status_code} {r.text}")
