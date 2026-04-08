# Databricks notebook source

# MAGIC %md
# MAGIC # Criar Genie Space

# COMMAND ----------

import requests, json

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

SPACE_NAME = "Sistema Acadêmico Inteligente"

SERIALIZED_SPACE = json.dumps({
    "version": 2,
    "config": {
        "instructions": (
            "Você é o assistente acadêmico da UFSCar (Universidade Federal de São Carlos). "
            "Responda SEMPRE em português brasileiro.\n\n"
            "## Regras do sistema acadêmico\n"
            "- Escala de notas: 0 a 10\n"
            "- nota_final = média(nota_p1, nota_p2)\n"
            "- Aprovação requer nota_final >= 6.0 E frequência >= 75%\n"
            "- Situações possíveis: aprovado, reprovado_nota, reprovado_frequencia, reprovado_nota_freq, trancado\n"
            "- Semestres no formato: 2023/1, 2023/2, 2024/1, 2024/2, 2025/1\n"
            "- CRA = Coeficiente de Rendimento Acadêmico (média acumulada)\n\n"
            "## Tabelas principais\n"
            "- matriculas: fato principal — cada linha é um aluno em uma disciplina em um semestre\n"
            "- alunos: cadastro de estudantes (nome, curso, ano de ingresso, status)\n"
            "- disciplinas: catálogo de disciplinas (código, nome, dificuldade 0-1)\n"
            "- gold_desempenho_aluno: visão agregada por aluno/semestre com CRA acumulado\n"
            "- gold_desempenho_disciplina: métricas por disciplina/semestre (taxa aprovação, médias)\n"
            "- gold_alunos_em_risco: alunos ativos com score de risco no semestre 2025/1\n\n"
            "## Joins comuns\n"
            "- matriculas.aluno_id = alunos.aluno_id\n"
            "- matriculas.disciplina_id = disciplinas.disciplina_id\n"
            "- alunos.curso_id pode ser ligado a cursos via gold_desempenho_aluno.curso_sigla\n\n"
            "## Dicas\n"
            "- Para taxa de reprovação, filtre situacao != 'trancado' antes de calcular\n"
            "- gold_alunos_em_risco.nivel_risco tem valores: ALTO, MÉDIO, BAIXO\n"
            "- Use disciplinas.codigo (ex: MAT102) para identificar disciplinas por código\n"
            "- 'Cálculo 2' é a disciplina com código MAT102\n"
            "- Quando perguntarem sobre 'alunos em risco', use a tabela gold_alunos_em_risco"
        ),
        "sample_questions": [
            {"question": ["Quais disciplinas têm a maior taxa de reprovação?"]},
            {"question": ["Quantos alunos estão em risco alto no semestre 2025/1?"]},
            {"question": ["Qual a média de notas de Cálculo 2 por semestre?"]},
            {"question": ["Quais cursos têm o melhor CRA médio?"]},
            {"question": ["Mostre a evolução do desempenho do aluno 42"]},
            {"question": ["Quais alunos de Ciência da Computação reprovaram em Cálculo 2?"]},
            {"question": ["Qual professor tem a melhor taxa de aprovação?"]},
            {"question": ["Quantos alunos estão ativos por curso?"]},
        ],
        "curated_questions": [
            {
                "question": "Top 10 disciplinas com maior taxa de reprovação",
                "sql": (
                    "SELECT d.codigo, d.nome, "
                    "COUNT(*) as total, "
                    "ROUND(SUM(CASE WHEN m.situacao LIKE 'reprovado%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as taxa_reprovacao "
                    "FROM workspace.sistema_academico.matriculas m "
                    "JOIN workspace.sistema_academico.disciplinas d ON m.disciplina_id = d.disciplina_id "
                    "WHERE m.situacao != 'trancado' "
                    "GROUP BY d.codigo, d.nome "
                    "ORDER BY taxa_reprovacao DESC LIMIT 10"
                )
            },
            {
                "question": "Alunos em risco alto no semestre atual com detalhes",
                "sql": (
                    "SELECT aluno_nome, curso_sigla, media_atual, freq_atual, "
                    "total_reprovacoes_historico, score_risco "
                    "FROM workspace.sistema_academico.gold_alunos_em_risco "
                    "WHERE nivel_risco = 'ALTO' ORDER BY score_risco DESC"
                )
            },
            {
                "question": "Evolução da média de notas de Cálculo 2 por semestre",
                "sql": (
                    "SELECT m.semestre, ROUND(AVG(m.nota_final), 2) as media, "
                    "ROUND(AVG(m.nota_p1), 2) as media_p1, ROUND(AVG(m.nota_p2), 2) as media_p2, "
                    "COUNT(*) as alunos "
                    "FROM workspace.sistema_academico.matriculas m "
                    "JOIN workspace.sistema_academico.disciplinas d ON m.disciplina_id = d.disciplina_id "
                    "WHERE d.codigo = 'MAT102' AND m.situacao != 'trancado' "
                    "GROUP BY m.semestre ORDER BY m.semestre"
                )
            },
            {
                "question": "CRA médio por curso",
                "sql": (
                    "SELECT curso_sigla, ROUND(AVG(cra_acumulado), 2) as cra_medio, "
                    "COUNT(DISTINCT aluno_id) as alunos "
                    "FROM workspace.sistema_academico.gold_desempenho_aluno "
                    "GROUP BY curso_sigla ORDER BY cra_medio DESC"
                )
            },
            {
                "question": "Alunos que passaram na P1 mas reprovaram no final",
                "sql": (
                    "SELECT a.nome, d.codigo, d.nome as disciplina, "
                    "m.nota_p1, m.nota_p2, m.nota_final, m.frequencia_pct, m.situacao "
                    "FROM workspace.sistema_academico.matriculas m "
                    "JOIN workspace.sistema_academico.alunos a ON m.aluno_id = a.aluno_id "
                    "JOIN workspace.sistema_academico.disciplinas d ON m.disciplina_id = d.disciplina_id "
                    "WHERE m.nota_p1 >= 6.0 AND m.situacao LIKE 'reprovado%' "
                    "ORDER BY m.nota_p1 DESC LIMIT 20"
                )
            },
            {
                "question": "Distribuição de alunos ativos por curso",
                "sql": (
                    "SELECT c.curso_sigla, c.curso_nome, COUNT(DISTINCT aluno_id) as total "
                    "FROM workspace.sistema_academico.gold_desempenho_aluno c "
                    "WHERE c.semestre = '2025/1' "
                    "GROUP BY c.curso_sigla, c.curso_nome ORDER BY total DESC"
                )
            },
        ],
    },
    "data_sources": {
        "tables": [
            {"identifier": "workspace.sistema_academico.matriculas"},
            {"identifier": "workspace.sistema_academico.alunos"},
            {"identifier": "workspace.sistema_academico.disciplinas"},
            {"identifier": "workspace.sistema_academico.gold_desempenho_aluno"},
            {"identifier": "workspace.sistema_academico.gold_desempenho_disciplina"},
            {"identifier": "workspace.sistema_academico.gold_alunos_em_risco"},
        ]
    },
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for existing space

# COMMAND ----------

existing_id = None
for api_path in ["/api/2.0/data-rooms", "/api/2.0/genie/spaces"]:
    r = requests.get(f"https://{host}{api_path}", headers=headers)
    if r.status_code == 200:
        data = r.json()
        spaces = data.get("spaces", data.get("data_rooms", data.get("genie_spaces", [])))
        for s in spaces:
            title = s.get("title", s.get("display_name", ""))
            if title == SPACE_NAME:
                existing_id = s.get("space_id", s.get("id", ""))
                break
    if existing_id:
        break

if existing_id:
    print(f"Found existing space: {existing_id} — updating...")
else:
    print("No existing space found — creating new...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get warehouse ID

# COMMAND ----------

r = requests.get(f"https://{host}/api/2.0/sql/warehouses", headers=headers)
wh_id = r.json().get("warehouses", [{}])[0].get("id")
assert wh_id, "No SQL warehouse found!"
print(f"Warehouse: {wh_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or Update Genie Space

# COMMAND ----------

# Use the public genie/spaces API with serialized_space for full config
payload = {
    "display_name": SPACE_NAME,
    "description": "Explore dados acadêmicos da universidade usando linguagem natural. Pergunte sobre notas, desempenho, taxas de aprovação, alunos em risco e mais.",
    "warehouse_id": wh_id,
    "table_identifiers": [
        "workspace.sistema_academico.matriculas",
        "workspace.sistema_academico.alunos",
        "workspace.sistema_academico.disciplinas",
        "workspace.sistema_academico.gold_desempenho_aluno",
        "workspace.sistema_academico.gold_desempenho_disciplina",
        "workspace.sistema_academico.gold_alunos_em_risco",
    ],
    "serialized_space": SERIALIZED_SPACE,
}

if existing_id:
    # Update existing
    payload["space_id"] = existing_id
    r = requests.put(f"https://{host}/api/2.0/genie/spaces/{existing_id}", headers=headers, json=payload)
    if r.status_code == 200:
        print(f"✅ Genie Space updated: {existing_id}")
        dbutils.notebook.exit(json.dumps({"space_id": existing_id, "status": "updated"}))
    else:
        print(f"Update failed ({r.status_code}), trying recreate...")
        existing_id = None

if not existing_id:
    # Create new — try both API paths and field name variants
    space_id = None
    for api_path in ["/api/2.0/genie/spaces", "/api/2.0/data-rooms"]:
        for name_field in ["display_name", "title"]:
            p = dict(payload)
            if name_field == "title":
                p["title"] = p.pop("display_name", SPACE_NAME)
            r = requests.post(f"https://{host}{api_path}", headers=headers, json=p)
            if r.status_code == 200:
                resp = r.json()
                space_id = resp.get("space_id", resp.get("id", ""))
                if space_id:
                    print(f"✅ Genie Space created via {api_path}: {space_id}")
                    dbutils.notebook.exit(json.dumps({"space_id": space_id, "status": "created"}))

    if not space_id:
        print(f"⚠️ Could not create Genie Space automatically.")
        print(f"Last response: {r.status_code} {r.text[:500]}")
        print("Create manually: Genie → New Space in the Databricks UI")
