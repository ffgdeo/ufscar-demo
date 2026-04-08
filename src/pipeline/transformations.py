# Databricks notebook source

# MAGIC %md
# MAGIC # Pipeline Acadêmico — Spark Declarative Pipeline
# MAGIC Bronze → Silver → Gold

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
SCHEMA = "sistema_academico"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

@dlt.table(name="bronze_matriculas", comment="Dados brutos de matrículas.")
def bronze_matriculas():
    return spark.table(f"{CATALOG}.{SCHEMA}.matriculas")

@dlt.table(name="bronze_alunos", comment="Cadastro de alunos.")
def bronze_alunos():
    return spark.table(f"{CATALOG}.{SCHEMA}.alunos")

@dlt.table(name="bronze_disciplinas", comment="Catálogo de disciplinas.")
def bronze_disciplinas():
    return spark.table(f"{CATALOG}.{SCHEMA}.disciplinas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

@dlt.table(name="silver_matriculas", comment="Matrículas enriquecidas com nomes de alunos, disciplinas, cursos e departamentos.")
@dlt.expect("nota_p1_valida", "nota_p1 IS NULL OR (nota_p1 >= 0 AND nota_p1 <= 10)")
@dlt.expect("nota_p2_valida", "nota_p2 IS NULL OR (nota_p2 >= 0 AND nota_p2 <= 10)")
@dlt.expect("frequencia_valida", "frequencia_pct IS NULL OR (frequencia_pct >= 0 AND frequencia_pct <= 100)")
@dlt.expect_or_drop("situacao_conhecida", "situacao IN ('aprovado','reprovado_nota','reprovado_frequencia','reprovado_nota_freq','trancado')")
def silver_matriculas():
    m = dlt.read("bronze_matriculas")
    a = dlt.read("bronze_alunos")
    d = dlt.read("bronze_disciplinas")
    c = spark.table(f"{CATALOG}.{SCHEMA}.cursos")
    dep = spark.table(f"{CATALOG}.{SCHEMA}.departamentos")
    p = spark.table(f"{CATALOG}.{SCHEMA}.professores")
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

@dlt.table(name="gold_desempenho_aluno", comment="Performance por aluno/semestre com CRA acumulado.")
def gold_desempenho_aluno():
    w = Window.partitionBy("aluno_id").orderBy("semestre").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return (
        dlt.read("silver_matriculas").filter(F.col("situacao") != "trancado")
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

@dlt.table(name="gold_desempenho_disciplina", comment="Métricas por disciplina e semestre.")
def gold_desempenho_disciplina():
    return (
        dlt.read("silver_matriculas").filter(F.col("situacao") != "trancado")
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

@dlt.table(name="gold_alunos_em_risco", comment="Alunos ativos em risco no semestre 2025/1. Nível: ALTO (>=40), MÉDIO (>=20), BAIXO (<20).")
def gold_alunos_em_risco():
    us = (
        dlt.read("silver_matriculas")
        .filter((F.col("semestre")=="2025/1") & (F.col("situacao")!="trancado"))
        .groupBy("aluno_id").agg(
            F.round(F.avg("nota_final"),2).alias("media_atual"),
            F.round(F.avg("frequencia_pct"),1).alias("freq_atual"),
            F.count("*").alias("disciplinas_atual"),
            F.sum(F.when(F.col("nota_p1")<5,1).otherwise(0)).alias("p1_abaixo_5"),
            F.sum(F.when(F.col("frequencia_pct")<75,1).otherwise(0)).alias("disciplinas_freq_baixa"),
        )
    )
    hist = (
        dlt.read("silver_matriculas")
        .filter((F.col("semestre")!="2025/1") & (F.col("situacao")!="trancado"))
        .groupBy("aluno_id").agg(
            F.round(F.avg("nota_final"),2).alias("cra_historico"),
            F.sum(F.when(F.col("reprovado"),1).otherwise(0)).alias("total_reprovacoes"),
        )
    )
    a = dlt.read("bronze_alunos").filter(F.col("status")=="ativo")
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
            F.when(F.col("score_risco")>=40,"ALTO").when(F.col("score_risco")>=20,"MÉDIO").otherwise("BAIXO"))
    )
