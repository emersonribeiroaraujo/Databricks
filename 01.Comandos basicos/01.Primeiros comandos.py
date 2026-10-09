# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ##### DBFS e dbutils Documentação 
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-utils

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Arquitetura azure
# MAGIC
# MAGIC https://learn.microsoft.com/pt-br/azure/architecture/solution-ideas/articles/azure-databricks-modern-analytics-architecture
# MAGIC
# MAGIC https://learn.microsoft.com/pt-br/azure/architecture/solution-ideas/articles/ingest-etl-stream-with-adb

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ### fsutils - Funções de Manipulação de Arquivos<br>
# MAGIC cp(from: String, to: String, recurse: boolean = false): boolean -> Copia um arquivo ou diretório, possivelmente entre sistemas de arquivos<br>
# MAGIC head(file: String, maxBytes: int = 65536): String -> Retorna até os primeiros bytes 'maxBytes' do arquivo fornecido como uma String codificada em UTF-8<br>
# MAGIC ls(dir: String): Seq -> Lista o conteúdo de um diretório<br>
# MAGIC mkdirs(dir: String): boolean -> Cria o diretório fornecido se ele não existir, criando também quaisquer diretórios pais necessários<br>
# MAGIC mv(from: String, to: String, recurse: boolean = false): boolean -> Move um arquivo ou diretório, possivelmente entre sistemas de arquivos<br>
# MAGIC put(arquivo: String, conteúdo: String, overwrite: boolean = false): boolean -> Grava a String fornecida em um arquivo, codificado em UTF-8<br>
# MAGIC rm(dir: String, recurse: boolean = false): boolean -> Remove um arquivo ou diretório

# COMMAND ----------

dbutils.fs.ls("/Volumes/curso_databricks/aula/aula_volume")

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/curso_databricks/aula/aula_volume"))

# COMMAND ----------

#aprofundar em pastas e subpastas
display(dbutils.fs.ls("/Volumes/curso_databricks/aula/aula_volume/Bike Store/"))

# COMMAND ----------

df_teste=spark.read.csv("dbfs:/Volumes/curso_databricks/aula/aula_volume/Bike Store/categories.csv", header=True, inferSchema=True)
df_teste.show()




# COMMAND ----------

df_teste2=spark.read.csv("/Volumes/curso_databricks/aula/aula_volume/Bike Store/categories.csv", header=True, inferSchema=True)
df_teste2.show()

# COMMAND ----------

df_teste3=spark.read.csv("/Volumes/curso_databricks/aula/aula_volume/Bike Store/categories.csv", header=True, inferSchema=True)
df_teste3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Visualizando os dados

# COMMAND ----------

# ver dados sem atribuir ao df
spark.read.csv("/Volumes/curso_databricks/aula/aula_volume/arquivos_csv/Clientes.csv", header=True, inferSchema=True).limit(5).display()


# COMMAND ----------

# Ler o arquivo CSV e armazenar em DF
df=spark.read.csv("/Volumes/curso_databricks/aula/aula_volume/arquivos_csv/Clientes.csv", header=True, inferSchema=True)


# COMMAND ----------

df.display()

# COMMAND ----------

# ver tipos de dados
df.printSchema()

# COMMAND ----------

df.columns

# COMMAND ----------

#vendo os dados colunas selcionadas
df.select('id', 'created_at', 'first_name', 'additionals').limit(5).display()

# COMMAND ----------

df_colunas=df.select('id', 'created_at', 'first_name', 'additionals')

# COMMAND ----------

df_colunas.display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Sobrescrevendo DF

# COMMAND ----------

df=df.select('id', 'created_at', 'first_name', 'additionals')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop volume custos
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Localizando Base de Dados Para Treino
# MAGIC
# MAGIC https://www.kaggle.com

# COMMAND ----------

# MAGIC %md
# MAGIC Obs: nao vamos usar estas bases para o curso é um Bonus para voce treinar com material Extra, ao passar do tempo essas infos podem estar indisponiveis, o que for pertinente as aulas vou deixar todo o material para Download

# COMMAND ----------

df_teste = spark.read.csv('/Volumes/cacaushow/custos/descarte/dados/dim_loja.csv', header=True, inferSchema=True)
display(df_teste)

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets'))

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/wine-quality/'))

# COMMAND ----------

df_red_teste = spark.read.csv('/databricks-datasets/wine-quality/winequality-red.csv', header=True, inferSchema=True, sep=';')
df_red_teste.display()

# COMMAND ----------

#leitura 
df_red = spark.read.csv('dbfs:/databricks-datasets/wine-quality/winequality-red.csv', header=True, inferSchema=True, sep=';')
df_white = spark.read.csv('dbfs:/databricks-datasets/wine-quality/winequality-white.csv', header=True, inferSchema=True, sep=';')

# escrita
df_red.write.mode('overwrite').csv('/Volumes/curso_databricks/aula/aula_volume/Dados Publicos Treino/winequality-red.csv', header=True)
df_white.write.mode('overwrite').csv('/Volumes/curso_databricks/aula/aula_volume/Dados Publicos Treino/winequality-white.csv', header=True)