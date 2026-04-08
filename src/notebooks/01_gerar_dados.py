# Databricks notebook source

# MAGIC %md
# MAGIC # Geração de Dados Sintéticos
# MAGIC Cria dados realistas de uma universidade brasileira.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "sistema_academico"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabelas de referência

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE departamentos (departamento_id INT, sigla STRING, nome STRING);
# MAGIC INSERT INTO departamentos VALUES
# MAGIC (1,'DC','Departamento de Computação'),(2,'DM','Departamento de Matemática'),
# MAGIC (3,'DF','Departamento de Física'),(4,'DEst','Departamento de Estatística'),
# MAGIC (5,'DEP','Departamento de Engenharia de Produção'),(6,'DL','Departamento de Letras');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cursos (curso_id INT, sigla STRING, nome STRING, departamento_id INT, duracao_semestres INT);
# MAGIC INSERT INTO cursos VALUES
# MAGIC (1,'BCC','Ciência da Computação',1,8),(2,'EC','Engenharia de Computação',1,10),
# MAGIC (3,'BEs','Bacharelado em Estatística',4,8),(4,'BM','Bacharelado em Matemática',2,8),
# MAGIC (5,'BF','Bacharelado em Física',3,8),(6,'EP','Engenharia de Produção',5,10),
# MAGIC (7,'BL','Bacharelado em Linguística',6,8),(8,'BCD','Ciência de Dados',4,8);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE professores (professor_id INT, nome STRING, departamento_id INT, titulacao STRING);
# MAGIC INSERT INTO professores VALUES
# MAGIC (1,'Prof. Ana Paula Souza',1,'Doutora'),(2,'Prof. Carlos Eduardo Lima',1,'Doutor'),
# MAGIC (3,'Prof. Fernanda Oliveira',1,'Doutora'),(4,'Prof. Ricardo Santos',1,'Doutor'),
# MAGIC (5,'Prof. José Roberto Silva',2,'Doutor'),(6,'Prof. Maria Helena Costa',2,'Doutora'),
# MAGIC (7,'Prof. Paulo Henrique Rocha',2,'Doutor'),(8,'Prof. Marcos Antônio Pereira',3,'Doutor'),
# MAGIC (9,'Prof. Juliana Ferreira',3,'Doutora'),(10,'Prof. Eduardo Martins',3,'Doutor'),
# MAGIC (11,'Prof. Beatriz Almeida',4,'Doutora'),(12,'Prof. Thiago Nascimento',4,'Doutor'),
# MAGIC (13,'Prof. Luciana Mendes',4,'Doutora'),(14,'Prof. Roberto Carvalho',5,'Doutor'),
# MAGIC (15,'Prof. Patrícia Barbosa',5,'Doutora'),(16,'Prof. Gustavo Teixeira',5,'Doutor'),
# MAGIC (17,'Prof. Camila Ribeiro',6,'Doutora'),(18,'Prof. Daniel Freitas',6,'Doutor'),
# MAGIC (19,'Prof. Vinícius Moreira',1,'Mestre'),(20,'Prof. Isabela Nunes',4,'Doutora');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Disciplinas e Grade Curricular

# COMMAND ----------

import random
from pyspark.sql.types import *

random.seed(42)

disciplinas = [
    (1,"CMP101","Introdução à Programação",1,4,1,0.25),
    (2,"CMP102","Lógica de Programação",1,4,1,0.30),
    (3,"CMP201","Algoritmos e Estruturas de Dados",1,6,2,0.55),
    (4,"CMP202","Programação Orientada a Objetos",1,4,3,0.40),
    (5,"CMP301","Banco de Dados",1,4,4,0.45),
    (6,"CMP302","Engenharia de Software",1,4,5,0.35),
    (7,"CMP303","Sistemas Operacionais",1,4,4,0.60),
    (8,"CMP304","Redes de Computadores",1,4,5,0.45),
    (9,"CMP401","Inteligência Artificial",1,4,6,0.55),
    (10,"CMP402","Aprendizado de Máquina",1,4,7,0.60),
    (11,"CMP403","Compiladores",1,4,6,0.65),
    (12,"CMP404","Computação Gráfica",1,4,7,0.40),
    (13,"CMP405","Projeto de Sistemas",1,4,8,0.30),
    (14,"MAT101","Cálculo 1",2,6,1,0.65),
    (15,"MAT102","Cálculo 2",2,6,2,0.80),
    (16,"MAT103","Geometria Analítica",2,4,1,0.45),
    (17,"MAT201","Álgebra Linear",2,4,2,0.70),
    (18,"MAT202","Matemática Discreta",2,4,1,0.40),
    (19,"MAT301","Cálculo Numérico",2,4,4,0.50),
    (20,"MAT302","Equações Diferenciais",2,4,3,0.65),
    (21,"MAT401","Análise Real",2,4,5,0.75),
    (22,"FIS101","Física 1 — Mecânica",3,4,1,0.60),
    (23,"FIS102","Física 2 — Termodinâmica e Ondas",3,4,2,0.55),
    (24,"FIS201","Física 3 — Eletromagnetismo",3,4,3,0.60),
    (25,"FIS111","Física Experimental 1",3,2,1,0.20),
    (26,"FIS112","Física Experimental 2",3,2,2,0.20),
    (27,"FIS301","Mecânica Quântica",3,4,5,0.80),
    (28,"EST101","Probabilidade e Estatística",4,4,3,0.50),
    (29,"EST201","Inferência Estatística",4,4,4,0.60),
    (30,"EST301","Estatística Computacional",4,4,5,0.45),
    (31,"EST302","Modelos de Regressão",4,4,5,0.50),
    (32,"EST401","Análise Multivariada",4,4,6,0.55),
    (33,"EST402","Séries Temporais",4,4,7,0.50),
    (34,"EPR201","Desenho Técnico",5,2,2,0.25),
    (35,"EPR301","Pesquisa Operacional",5,4,5,0.50),
    (36,"EPR302","Gestão da Qualidade",5,4,6,0.30),
    (37,"EPR303","Gestão de Projetos",5,4,6,0.30),
    (38,"EPR401","Logística e Cadeia de Suprimentos",5,4,7,0.40),
    (39,"EPR402","Simulação de Sistemas",5,4,7,0.45),
    (40,"LET101","Linguística Geral",6,4,1,0.25),
    (41,"LET201","Sociolinguística",6,4,3,0.30),
    (42,"LET301","Processamento de Linguagem Natural",6,4,5,0.50),
    (43,"GER101","Metodologia Científica",6,2,1,0.15),
    (44,"GER102","Comunicação e Expressão",6,2,1,0.10),
    (45,"GER201","Ética e Cidadania",6,2,3,0.10),
]

df_disc = spark.createDataFrame(disciplinas,
    ["disciplina_id","codigo","nome","departamento_id","creditos","semestre_recomendado","dificuldade"])
df_disc.write.mode("overwrite").saveAsTable("disciplinas")

curriculo = {
    1:[1,2,3,4,5,6,7,8,9,10,11,13,14,15,16,17,18,19,22,23,25,26,28,29,43,44,45],
    2:[1,2,3,4,5,7,8,9,12,13,14,15,16,17,18,19,20,22,23,24,25,26,28,34,43,44,45],
    3:[1,14,15,16,17,19,22,25,28,29,30,31,32,33,43,44,45],
    4:[14,15,16,17,18,19,20,21,22,23,25,28,29,43,44,45],
    5:[14,15,16,17,20,22,23,24,25,26,27,28,43,44,45],
    6:[1,14,15,16,17,19,22,23,25,26,28,29,34,35,36,37,38,39,43,44,45],
    7:[40,41,42,28,43,44,45],
    8:[1,3,5,9,10,14,15,16,17,18,19,28,29,30,31,32,33,42,43,44,45],
}
gc_rows = [(c,d) for c,ds in curriculo.items() for d in ds]
spark.createDataFrame(gc_rows, ["curso_id","disciplina_id"]).write.mode("overwrite").saveAsTable("grade_curricular")
print(f"Disciplinas: {len(disciplinas)}, Grade curricular: {len(gc_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alunos e Matrículas

# COMMAND ----------

disc_map = {d[0]:{"dept":d[3],"sem":d[5],"dif":d[6]} for d in disciplinas}
prof_by_dept = {1:[1,2,3,4,19],2:[5,6,7],3:[8,9,10],4:[11,12,13,20],5:[14,15,16],6:[17,18]}

nomes_m = ["João","Pedro","Lucas","Gabriel","Matheus","Rafael","Bruno","Gustavo","Felipe","Leonardo",
    "André","Carlos","Daniel","Eduardo","Fernando","Henrique","Igor","José","Marcos","Nicolas",
    "Paulo","Ricardo","Thiago","Vinícius","Diego","Caio","Arthur","Bernardo","Guilherme","Samuel"]
nomes_f = ["Ana","Beatriz","Camila","Daniela","Fernanda","Gabriela","Helena","Isabella","Juliana",
    "Larissa","Mariana","Natália","Patrícia","Rafaela","Sabrina","Tatiana","Vanessa","Amanda",
    "Bruna","Carolina","Débora","Eduarda","Flávia","Giovanna","Heloísa","Ingrid","Jéssica",
    "Letícia","Manuela","Nicole"]
sobrenomes = ["Silva","Santos","Oliveira","Souza","Rodrigues","Ferreira","Almeida","Pereira","Lima",
    "Gomes","Costa","Ribeiro","Martins","Carvalho","Araújo","Melo","Barbosa","Cardoso",
    "Nascimento","Moreira","Nunes","Vieira","Monteiro","Mendes","Correia","Pinto","Rocha",
    "Machado","Freitas","Teixeira"]

def nome(): return f"{random.choice(nomes_m if random.random()<.5 else nomes_f)} {random.choice(sobrenomes)} {random.choice(sobrenomes)}"

curso_pool = [c for c,w in {1:120,2:110,3:80,4:70,5:80,6:120,7:60,8:100}.items() for _ in range(w)]
SEMESTRES = ["2023/1","2023/2","2024/1","2024/2","2025/1"]

alunos, matriculas = [], []
mid = 0
for i in range(1,801):
    ano = random.choices([2021,2022,2023,2024], weights=[15,25,30,30])[0]
    cid = random.choice(curso_pool)
    hab = max(2.0, min(10.0, random.gauss(6.5,1.2)))
    if ano==2021: st=random.choices(["formado","ativo","trancado"],[50,40,10])[0]
    elif ano==2022: st=random.choices(["ativo","formado","trancado"],[65,20,15])[0]
    elif ano==2023: st=random.choices(["ativo","trancado"],[90,10])[0]
    else: st=random.choices(["ativo","trancado"],[95,5])[0]
    burnout = random.random() < 0.12
    alunos.append((i, nome(), cid, ano, st))

    disc_ids = sorted(curriculo.get(cid,[]), key=lambda d: disc_map[d]["sem"])
    sems = SEMESTRES[{2021:0,2022:0,2023:0,2024:2}.get(ano,0):]
    if st=="trancado": sems=sems[:max(1,len(sems)-random.randint(1,3))]
    queue = list(disc_ids)
    for si, sem in enumerate(sems):
        if not queue: break
        nc = min(len(queue), random.randint(4,7))
        bf = si*0.4 if burnout else 0.0
        for did in queue[:nc]:
            mid += 1
            dif = disc_map[did]["dif"]
            pid = random.choice(prof_by_dept.get(disc_map[did]["dept"],[1]))
            if random.random() < 0.03:
                matriculas.append((mid,i,did,pid,sem,None,None,None,None,"trancado"))
                continue
            p1 = round(max(0,min(10,random.gauss(hab+0.8-dif*1.6-bf, 1.0))),1)
            p2 = round(max(0,min(10,random.gauss(hab+0.8-dif*1.6-bf-0.25, 1.0))),1)
            nf = round((p1+p2)/2, 1)
            freq = round(max(30,min(100,random.gauss(72+hab*2.5-bf*10, 7))),1)
            if nf>=6 and freq>=75: sit="aprovado"
            elif freq<75 and nf<6: sit="reprovado_nota_freq"
            elif freq<75: sit="reprovado_frequencia"
            else: sit="reprovado_nota"
            matriculas.append((mid,i,did,pid,sem,p1,p2,nf,freq,sit))
        queue = queue[nc:]

spark.createDataFrame(alunos, ["aluno_id","nome","curso_id","ano_ingresso","status"]) \
    .write.mode("overwrite").saveAsTable("alunos")

schema_m = StructType([StructField("matricula_id",IntegerType()),StructField("aluno_id",IntegerType()),
    StructField("disciplina_id",IntegerType()),StructField("professor_id",IntegerType()),
    StructField("semestre",StringType()),StructField("nota_p1",FloatType()),
    StructField("nota_p2",FloatType()),StructField("nota_final",FloatType()),
    StructField("frequencia_pct",FloatType()),StructField("situacao",StringType())])
spark.createDataFrame(matriculas, schema=schema_m).write.mode("overwrite").saveAsTable("matriculas")

print(f"Alunos: {len(alunos)}, Matrículas: {len(matriculas)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table comments (para Genie)

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE departamentos IS 'Departamentos acadêmicos da universidade (ex: Computação, Matemática, Física)';
# MAGIC COMMENT ON TABLE cursos IS 'Programas de graduação. Cada curso pertence a um departamento.';
# MAGIC COMMENT ON TABLE disciplinas IS 'Catálogo de disciplinas com código, nome, créditos, semestre recomendado e fator de dificuldade (0-1).';
# MAGIC COMMENT ON TABLE professores IS 'Corpo docente com nome, departamento e titulação.';
# MAGIC COMMENT ON TABLE alunos IS 'Estudantes matriculados. Status: ativo, formado ou trancado.';
# MAGIC COMMENT ON TABLE matriculas IS 'Registro de matrículas com notas P1, P2, nota final (média), frequência e situação. Escala 0-10, aprovação requer nota >= 6.0 e frequência >= 75%.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume para PDFs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS staging COMMENT 'Staging area para uploads de dados';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'departamentos' as t, COUNT(*) as n FROM departamentos
# MAGIC UNION ALL SELECT 'cursos', COUNT(*) FROM cursos
# MAGIC UNION ALL SELECT 'disciplinas', COUNT(*) FROM disciplinas
# MAGIC UNION ALL SELECT 'professores', COUNT(*) FROM professores
# MAGIC UNION ALL SELECT 'alunos', COUNT(*) FROM alunos
# MAGIC UNION ALL SELECT 'matriculas', COUNT(*) FROM matriculas

# COMMAND ----------

print("✅ Dados gerados com sucesso!")
