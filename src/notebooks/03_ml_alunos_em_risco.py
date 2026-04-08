# Databricks notebook source

# MAGIC %md
# MAGIC # Modelo de Predição — Alunos em Risco
# MAGIC Treina um modelo para prever reprovação baseado em nota P1 + frequência.

# COMMAND ----------

# MAGIC %pip install scikit-learn -q

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
# MAGIC ## Build training dataset

# COMMAND ----------

df = spark.sql("""
WITH hist AS (
  SELECT m.aluno_id, m.semestre,
    AVG(m2.nota_final) AS cra_anterior,
    SUM(CASE WHEN m2.situacao LIKE 'reprovado%' THEN 1 ELSE 0 END) AS reprovacoes_anteriores,
    COUNT(m2.matricula_id) AS disciplinas_anteriores
  FROM workspace.sistema_academico.matriculas m
  LEFT JOIN workspace.sistema_academico.matriculas m2
    ON m.aluno_id = m2.aluno_id AND m2.semestre < m.semestre AND m2.situacao != 'trancado'
  WHERE m.situacao != 'trancado'
  GROUP BY m.aluno_id, m.semestre
)
SELECT m.nota_p1, m.frequencia_pct, d.dificuldade, d.creditos,
  COALESCE(h.cra_anterior, 6.5) AS cra_anterior,
  COALESCE(h.reprovacoes_anteriores, 0) AS reprovacoes_anteriores,
  COALESCE(h.disciplinas_anteriores, 0) AS disciplinas_anteriores,
  CASE WHEN m.situacao LIKE 'reprovado%' THEN 1 ELSE 0 END AS reprovado
FROM workspace.sistema_academico.matriculas m
JOIN workspace.sistema_academico.disciplinas d ON m.disciplina_id = d.disciplina_id
LEFT JOIN hist h ON m.aluno_id = h.aluno_id AND m.semestre = h.semestre
WHERE m.situacao != 'trancado' AND m.nota_p1 IS NOT NULL
""").toPandas()

print(f"Dataset: {len(df)} rows, {df['reprovado'].mean():.1%} fail rate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train model

# COMMAND ----------

features = ["nota_p1","frequencia_pct","dificuldade","creditos","cra_anterior","reprovacoes_anteriores","disciplinas_anteriores"]
X_train, X_test, y_train, y_test = train_test_split(df[features], df["reprovado"], test_size=0.2, random_state=42, stratify=df["reprovado"])

experiment_path = "/Users/ffgdeo@gmail.com/ml_alunos_em_risco"
mlflow.set_experiment(experiment_path)

with mlflow.start_run(run_name="gradient_boosting_v1") as run:
    model = GradientBoostingClassifier(n_estimators=100, max_depth=4, learning_rate=0.1, random_state=42)
    model.fit(X_train, y_train)
    y_prob = model.predict_proba(X_test)[:,1]
    auc = roc_auc_score(y_test, y_prob)
    report = classification_report(y_test, model.predict(X_test), output_dict=True)

    mlflow.log_params({"model_type":"GradientBoosting","n_estimators":100,"max_depth":4})
    mlflow.log_metrics({"auc_roc":auc,"accuracy":report["accuracy"],"recall_reprovado":report["1"]["recall"]})
    mlflow.sklearn.log_model(model, artifact_path="model", input_example=X_test.head(1))

    try:
        mlflow.register_model(f"runs:/{run.info.run_id}/model","workspace.sistema_academico.modelo_risco_academico")
        print("✅ Model registered in UC")
    except Exception as e:
        print(f"⚠️ UC registry not available: {e}")

    print(f"✅ AUC={auc:.3f}, Accuracy={report['accuracy']:.3f}, Recall={report['1']['recall']:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score current semester

# COMMAND ----------

df_atual = spark.sql("""
WITH hist AS (
  SELECT aluno_id, AVG(nota_final) AS cra_anterior,
    SUM(CASE WHEN situacao LIKE 'reprovado%' THEN 1 ELSE 0 END) AS reprovacoes_anteriores,
    COUNT(*) AS disciplinas_anteriores
  FROM workspace.sistema_academico.matriculas
  WHERE semestre < '2025/1' AND situacao != 'trancado' GROUP BY aluno_id
)
SELECT m.aluno_id, a.nome AS aluno_nome, c.sigla AS curso_sigla,
  d.codigo AS disciplina_codigo, d.nome AS disciplina_nome,
  m.nota_p1, m.frequencia_pct, d.dificuldade, d.creditos,
  COALESCE(h.cra_anterior, 6.5) AS cra_anterior,
  COALESCE(h.reprovacoes_anteriores, 0) AS reprovacoes_anteriores,
  COALESCE(h.disciplinas_anteriores, 0) AS disciplinas_anteriores
FROM workspace.sistema_academico.matriculas m
JOIN workspace.sistema_academico.alunos a ON m.aluno_id = a.aluno_id
JOIN workspace.sistema_academico.cursos c ON a.curso_id = c.curso_id
JOIN workspace.sistema_academico.disciplinas d ON m.disciplina_id = d.disciplina_id
LEFT JOIN hist h ON m.aluno_id = h.aluno_id
WHERE m.semestre = '2025/1' AND m.situacao != 'trancado' AND m.nota_p1 IS NOT NULL
""").toPandas()

df_atual["prob_reprovacao"] = model.predict_proba(df_atual[features])[:,1]
df_atual["risco"] = df_atual["prob_reprovacao"].apply(lambda p: "ALTO" if p>.7 else ("MEDIO" if p>.4 else "BAIXO"))

for col in ["aluno_id"]: df_atual[col] = df_atual[col].astype(int)
for col in ["nota_p1","frequencia_pct","cra_anterior","prob_reprovacao"]: df_atual[col] = df_atual[col].astype(float)

spark.createDataFrame(df_atual[["aluno_id","aluno_nome","curso_sigla","disciplina_codigo",
    "disciplina_nome","nota_p1","frequencia_pct","cra_anterior","prob_reprovacao","risco"]]) \
    .write.mode("overwrite").saveAsTable("workspace.sistema_academico.gold_previsao_risco_2025_1")

spark.sql("COMMENT ON TABLE workspace.sistema_academico.gold_previsao_risco_2025_1 IS 'Previsões ML de risco de reprovação 2025/1.'")
print(f"✅ {len(df_atual)} predictions saved. {(df_atual['risco']=='ALTO').sum()} high risk.")
