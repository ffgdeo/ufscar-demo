# Databricks notebook source

# MAGIC %md
# MAGIC # Pipeline Academico — Spark Declarative Pipeline
# MAGIC Bronze (CSVs) → Silver → Gold

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
SCHEMA = "sistema_academico"
CSV_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/staging/csvs"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — leitura de CSVs do Volume

# COMMAND ----------

@dp.table(name="bronze_matriculas", comment="Dados brutos de matriculas lidos de CSV.")
def bronze_matriculas():
    return spark.read.format("csv").option("header", True).option("inferSchema", True).load(f"{CSV_BASE}/matriculas.csv")

@dp.table(name="bronze_alunos", comment="Cadastro de alunos lido de CSV.")
def bronze_alunos():
    return spark.read.format("csv").option("header", True).option("inferSchema", True).load(f"{CSV_BASE}/alunos.csv")

@dp.table(name="bronze_disciplinas", comment="Catalogo de disciplinas lido de CSV.")
def bronze_disciplinas():
    return spark.read.format("csv").option("header", True).option("inferSchema", True).load(f"{CSV_BASE}/disciplinas.csv")

@dp.table(name="bronze_departamentos", comment="Departamentos academicos lidos de CSV.")
def bronze_departamentos():
    return spark.read.format("csv").option("header", True).option("inferSchema", True).load(f"{CSV_BASE}/departamentos.csv")

@dp.table(name="bronze_cursos", comment="Cursos de graduacao lidos de CSV.")
def bronze_cursos():
    return spark.read.format("csv").option("header", True).option("inferSchema", True).load(f"{CSV_BASE}/cursos.csv")

@dp.table(name="bronze_professores", comment="Corpo docente lido de CSV.")
def bronze_professores():
    return spark.read.format("csv").option("header", True).option("inferSchema", True).load(f"{CSV_BASE}/professores.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

@dp.table(name="silver_matriculas", comment="Matriculas enriquecidas com nomes de alunos, disciplinas, cursos e departamentos.")
@dp.expect("nota_p1_valida", "nota_p1 IS NULL OR (nota_p1 >= 0 AND nota_p1 <= 10)")
@dp.expect("nota_p2_valida", "nota_p2 IS NULL OR (nota_p2 >= 0 AND nota_p2 <= 10)")
@dp.expect("frequencia_valida", "frequencia_pct IS NULL OR (frequencia_pct >= 0 AND frequencia_pct <= 100)")
@dp.expect_or_drop("situacao_conhecida", "situacao IN ('aprovado','reprovado_nota','reprovado_frequencia','reprovado_nota_freq','trancado')")
def silver_matriculas():
    m = spark.read.table("bronze_matriculas")
    a = spark.read.table("bronze_alunos")
    d = spark.read.table("bronze_disciplinas")
    c = spark.read.table("bronze_cursos")
    dep = spark.read.table("bronze_departamentos")
    p = spark.read.table("bronze_professores")
    return (
        m.join(a, "aluno_id").join(d, "disciplina_id")
        .join(c, a["curso_id"] == c["curso_id"])
        .join(dep, d["departamento_id"] == dep["departamento_id"])
        .join(p, m["professor_id"] == p["professor_id"])
        .select(
            m["matricula_id"], m["semestre"],
            a["aluno_id"], a["nome"].alias("aluno_nome"),
            c["sigla"].alias("curso_sigla"), c["nome"].alias("curso_nome"), a["ano_ingresso"],
            d["disciplina_id"], d["codigo"].alias("disciplina_codigo"),
            d["nome"].alias("disciplina_nome"), dep["sigla"].alias("departamento_sigla"),
            d["dificuldade"], d["creditos"],
            p["nome"].alias("professor_nome"),
            m["nota_p1"], m["nota_p2"], m["nota_final"], m["frequencia_pct"], m["situacao"],
            F.when(F.col("situacao") == "aprovado", True).otherwise(False).alias("aprovado"),
            F.when(F.col("situacao").startswith("reprovado"), True).otherwise(False).alias("reprovado"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

@dp.table(name="gold_desempenho_aluno", comment="Performance por aluno/semestre com CRA acumulado.")
def gold_desempenho_aluno():
    w = Window.partitionBy("aluno_id").orderBy("semestre").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return (
        spark.read.table("silver_matriculas").filter(F.col("situacao") != "trancado")
        .groupBy("aluno_id","aluno_nome","curso_sigla","curso_nome","ano_ingresso","semestre")
        .agg(
            F.count("*").alias("disciplinas_cursadas"),
            F.sum(F.when(F.col("aprovado"),1).otherwise(0)).alias("disciplinas_aprovadas"),
            F.sum(F.when(F.col("reprovado"),1).otherwise(0)).alias("disciplinas_reprovadas"),
            F.round(F.avg("nota_final"),2).alias("media_semestre"),
            F.round(F.avg("frequencia_pct"),1).alias("freq_media"),
        )
        .withColumn("cra_acumulado", F.round(F.avg("media_semestre").over(w), 2))
    )

@dp.table(name="gold_desempenho_disciplina", comment="Metricas por disciplina e semestre.")
def gold_desempenho_disciplina():
    return (
        spark.read.table("silver_matriculas").filter(F.col("situacao") != "trancado")
        .groupBy("disciplina_id","disciplina_codigo","disciplina_nome","departamento_sigla","dificuldade","creditos","semestre")
        .agg(
            F.count("*").alias("total_alunos"),
            F.sum(F.when(F.col("aprovado"),1).otherwise(0)).alias("aprovados"),
            F.round(F.sum(F.when(F.col("aprovado"),1).otherwise(0))*100.0/F.count("*"),1).alias("taxa_aprovacao"),
            F.round(F.avg("nota_final"),2).alias("media_nota"),
            F.round(F.avg("nota_p1"),2).alias("media_p1"),
            F.round(F.avg("nota_p2"),2).alias("media_p2"),
            F.round(F.avg("frequencia_pct"),1).alias("freq_media"),
        )
    )

@dp.table(name="gold_alunos_em_risco", comment="Alunos ativos em risco no semestre 2026/1. Nivel: ALTO (>=40), MEDIO (>=20), BAIXO (<20).")
def gold_alunos_em_risco():
    us = (
        spark.read.table("silver_matriculas")
        .filter((F.col("semestre")=="2026/1") & (F.col("situacao")!="trancado"))
        .groupBy("aluno_id").agg(
            F.round(F.avg("nota_final"),2).alias("media_atual"),
            F.round(F.avg("frequencia_pct"),1).alias("freq_atual"),
            F.count("*").alias("disciplinas_atual"),
            F.sum(F.when(F.col("nota_p1")<5,1).otherwise(0)).alias("p1_abaixo_5"),
            F.sum(F.when(F.col("frequencia_pct")<75,1).otherwise(0)).alias("disciplinas_freq_baixa"),
        )
    )
    hist = (
        spark.read.table("silver_matriculas")
        .filter((F.col("semestre")!="2026/1") & (F.col("situacao")!="trancado"))
        .groupBy("aluno_id").agg(
            F.round(F.avg("nota_final"),2).alias("cra_historico"),
            F.sum(F.when(F.col("reprovado"),1).otherwise(0)).alias("total_reprovacoes"),
        )
    )
    a = spark.read.table("bronze_alunos").filter(F.col("status")=="ativo")
    c = spark.table(f"{CATALOG}.{SCHEMA}.cursos")
    joined = us.join(a,"aluno_id").join(c,a["curso_id"]==c["curso_id"]).join(hist,"aluno_id","left")
    return (
        joined.select(
            "aluno_id", a["nome"].alias("aluno_nome"), c["sigla"].alias("curso_sigla"),
            c["nome"].alias("curso_nome"), "ano_ingresso", "media_atual", "freq_atual",
            "disciplinas_atual", "p1_abaixo_5", "disciplinas_freq_baixa",
            F.coalesce("cra_historico","media_atual").alias("cra_historico"),
            F.coalesce("total_reprovacoes",F.lit(0)).alias("total_reprovacoes_historico"),
            F.round(F.greatest(F.lit(0),F.least(F.lit(100),
                F.when(F.col("media_atual")<5,30).when(F.col("media_atual")<6,15).otherwise(0) +
                F.when(F.col("freq_atual")<70,25).when(F.col("freq_atual")<75,15).otherwise(0) +
                F.col("p1_abaixo_5")*8 + F.col("disciplinas_freq_baixa")*8 +
                F.when(F.coalesce("total_reprovacoes",F.lit(0))>5,15).when(F.coalesce("total_reprovacoes",F.lit(0))>2,8).otherwise(0)
            )),0).alias("score_risco"),
        )
        .withColumn("nivel_risco",
            F.when(F.col("score_risco")>=40,"ALTO").when(F.col("score_risco")>=20,"MEDIO").otherwise("BAIXO"))
    )
