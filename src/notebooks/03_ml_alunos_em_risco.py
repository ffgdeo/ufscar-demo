# Databricks notebook source

# MAGIC %md
# MAGIC # Modelo de Predicao — Alunos em Risco
# MAGIC Treina um modelo para prever reprovacao usando Feature Engineering + GradientBoosting.

# COMMAND ----------

# MAGIC %pip install scikit-learn databricks-feature-engineering -q

# COMMAND ----------

import mlflow, mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import classification_report, roc_auc_score
import pandas as pd

try:
    mlflow.set_registry_uri("databricks-uc")
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering — criar tabela de features por aluno

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()

# COMMAND ----------

# Computar features agregadas por aluno a partir do historico completo
features_df = spark.sql("""
SELECT
  aluno_id,
  ROUND(AVG(nota_final), 2) AS cra_acumulado,
  COUNT(*) AS total_disciplinas_cursadas,
  SUM(CASE WHEN situacao LIKE 'reprovado%' THEN 1 ELSE 0 END) AS total_reprovacoes,
  ROUND(SUM(CASE WHEN situacao = 'aprovado' THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 3) AS taxa_aprovacao_pessoal,
  COUNT(DISTINCT semestre) AS semestres_cursados
FROM workspace.sistema_academico.matriculas
WHERE situacao != 'trancado'
GROUP BY aluno_id
""")

# Criar ou atualizar a feature table
spark.sql("DROP TABLE IF EXISTS workspace.sistema_academico.student_features")
fe.create_table(
    name="workspace.sistema_academico.student_features",
    primary_keys=["aluno_id"],
    df=features_df,
    description="Features agregadas por aluno: CRA, total disciplinas, reprovacoes, taxa aprovacao, semestres cursados."
)

print(f"Feature table criada com {features_df.count()} alunos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build training dataset usando Feature Lookups

# COMMAND ----------

# Base: cada matricula individual com dados da disciplina (nao trancados, com nota)
raw_df = spark.sql("""
SELECT
  m.matricula_id,
  m.aluno_id,
  m.nota_p1,
  m.frequencia_pct,
  d.dificuldade,
  d.creditos,
  CASE WHEN m.situacao LIKE 'reprovado%' THEN 1 ELSE 0 END AS reprovado
FROM workspace.sistema_academico.matriculas m
JOIN workspace.sistema_academico.disciplinas d ON m.disciplina_id = d.disciplina_id
WHERE m.situacao != 'trancado' AND m.nota_p1 IS NOT NULL
""")

# Definir feature lookups
feature_lookups = [
    FeatureLookup(
        table_name="workspace.sistema_academico.student_features",
        feature_names=["cra_acumulado", "total_disciplinas_cursadas", "total_reprovacoes", "taxa_aprovacao_pessoal", "semestres_cursados"],
        lookup_key=["aluno_id"],
    ),
]

# Criar training set com feature lookups
training_set = fe.create_training_set(
    df=raw_df,
    feature_lookups=feature_lookups,
    label="reprovado",
    exclude_columns=["matricula_id", "aluno_id"],
)

df = training_set.load_df().toPandas()

# Convert Decimal columns to float (avoids JSON serialization errors in MLflow)
for col in df.select_dtypes(include=["object"]).columns:
    try:
        df[col] = df[col].astype(float)
    except (ValueError, TypeError):
        pass
for col in df.columns:
    if df[col].dtype == object or str(df[col].dtype) == "object":
        continue
    df[col] = df[col].astype(float) if "decimal" in str(df[col].dtype).lower() else df[col]

# Force all numeric columns to native Python types
numeric_cols = df.select_dtypes(include=["number"]).columns
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

print(f"Dataset: {len(df)} rows, {df['reprovado'].mean():.1%} fail rate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train model

# COMMAND ----------

features = ["nota_p1","frequencia_pct","dificuldade","creditos","cra_acumulado","total_disciplinas_cursadas","total_reprovacoes","taxa_aprovacao_pessoal","semestres_cursados"]
X_train, X_test, y_train, y_test = train_test_split(df[features], df["reprovado"], test_size=0.2, random_state=42, stratify=df["reprovado"])

experiment_path = "/Users/ffgdeo@gmail.com/ml_alunos_em_risco"
mlflow.set_experiment(experiment_path)

with mlflow.start_run(run_name="gradient_boosting_fe_v1") as run:
    model = GradientBoostingClassifier(n_estimators=100, max_depth=4, learning_rate=0.1, random_state=42)
    model.fit(X_train, y_train)
    y_prob = model.predict_proba(X_test)[:,1]
    auc = roc_auc_score(y_test, y_prob)
    report = classification_report(y_test, model.predict(X_test), output_dict=True)

    mlflow.log_params({"model_type":"GradientBoosting","n_estimators":100,"max_depth":4})
    mlflow.log_metrics({"auc_roc":auc,"accuracy":report["accuracy"],"recall_reprovado":report["1"]["recall"]})

    # Log model with MLflow (no feature lookup metadata — avoids online store requirement for serving)
    mlflow.sklearn.log_model(
        model,
        artifact_path="model",
        input_example=X_test.head(1),
    )

    try:
        mlflow.register_model(f"runs:/{run.info.run_id}/model","workspace.sistema_academico.modelo_risco_academico")
        print("Model registered in UC")
    except Exception as e:
        print(f"UC registry not available: {e}")

    print(f"AUC={auc:.3f}, Accuracy={report['accuracy']:.3f}, Recall={report['1']['recall']:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score current semester (2026/1)

# COMMAND ----------

df_atual = spark.sql("""
WITH hist AS (
  SELECT aluno_id, AVG(nota_final) AS cra_acumulado,
    COUNT(*) AS total_disciplinas_cursadas,
    SUM(CASE WHEN situacao LIKE 'reprovado%' THEN 1 ELSE 0 END) AS total_reprovacoes,
    ROUND(SUM(CASE WHEN situacao = 'aprovado' THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 3) AS taxa_aprovacao_pessoal,
    COUNT(DISTINCT semestre) AS semestres_cursados
  FROM workspace.sistema_academico.matriculas
  WHERE semestre < '2026/1' AND situacao != 'trancado' GROUP BY aluno_id
)
SELECT m.aluno_id, a.nome AS aluno_nome, c.sigla AS curso_sigla,
  d.codigo AS disciplina_codigo, d.nome AS disciplina_nome,
  m.nota_p1, m.frequencia_pct, d.dificuldade, d.creditos,
  COALESCE(h.cra_acumulado, 6.5) AS cra_acumulado,
  COALESCE(h.total_disciplinas_cursadas, 0) AS total_disciplinas_cursadas,
  COALESCE(h.total_reprovacoes, 0) AS total_reprovacoes,
  COALESCE(h.taxa_aprovacao_pessoal, 0.5) AS taxa_aprovacao_pessoal,
  COALESCE(h.semestres_cursados, 0) AS semestres_cursados
FROM workspace.sistema_academico.matriculas m
JOIN workspace.sistema_academico.alunos a ON m.aluno_id = a.aluno_id
JOIN workspace.sistema_academico.cursos c ON a.curso_id = c.curso_id
JOIN workspace.sistema_academico.disciplinas d ON m.disciplina_id = d.disciplina_id
LEFT JOIN hist h ON m.aluno_id = h.aluno_id
WHERE m.semestre = '2026/1' AND m.situacao != 'trancado' AND m.nota_p1 IS NOT NULL
""").toPandas()

df_atual["prob_reprovacao"] = model.predict_proba(df_atual[features])[:,1]
df_atual["risco"] = df_atual["prob_reprovacao"].apply(lambda p: "ALTO" if p>.7 else ("MEDIO" if p>.4 else "BAIXO"))

for col in ["aluno_id"]: df_atual[col] = df_atual[col].astype(int)
for col in ["nota_p1","frequencia_pct","cra_acumulado","prob_reprovacao"]: df_atual[col] = df_atual[col].astype(float)

spark.createDataFrame(df_atual[["aluno_id","aluno_nome","curso_sigla","disciplina_codigo",
    "disciplina_nome","nota_p1","frequencia_pct","cra_acumulado","prob_reprovacao","risco"]]) \
    .write.mode("overwrite").saveAsTable("workspace.sistema_academico.gold_previsao_risco_2026_1")

spark.sql("COMMENT ON TABLE workspace.sistema_academico.gold_previsao_risco_2026_1 IS 'Previsoes ML de risco de reprovacao 2026/1.'")
print(f"{len(df_atual)} predictions saved. {(df_atual['risco']=='ALTO').sum()} high risk.")
