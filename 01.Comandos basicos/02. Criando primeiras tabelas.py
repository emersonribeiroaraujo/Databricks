# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC lendo Arquivo e salvando como tabela

# COMMAND ----------

display(dbutils.fs.ls('/Volumes/curso_databricks/aula/aula_volume/Bike Store/'))

# COMMAND ----------

df_categories = spark.read.csv("/Volumes/curso_databricks/aula/aula_volume/Bike Store/categories.csv", header=True, inferSchema=True)
df_categories.show()

# COMMAND ----------

# MAGIC %md
# MAGIC tratamento de dados

# COMMAND ----------

#criar códigos fazendo estas transformações por exemplo selecionando apenas aulgumas colunas, tratando dados nulos

# COMMAND ----------

# MAGIC %md
# MAGIC Escrita 

# COMMAND ----------

df_categories.write.format('delta').mode('overwrite').saveAsTable('curso_databricks.aula.categories')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curso_databricks.aula.categories

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM curso_databricks.aula.categories

# COMMAND ----------

# MAGIC %md
# MAGIC outras formas de fazer a escrita

# COMMAND ----------

df_categories.write\
    .format('delta')\
    .mode('overwrite')\
    .saveAsTable('curso_databricks.aula.categories2')

# COMMAND ----------

(
    df_categories.write
    .format('delta')
    .mode('overwrite')
    .saveAsTable('curso_databricks.aula.categories3')

)

# COMMAND ----------

catalog='curso_databricks'
schema='aula'
table='categories4'
fulll_table_name=f'{catalog}.{schema}.{table}'


(
    df_categories.write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(f'{fulll_table_name}')

)




# COMMAND ----------

catalogo = 'curso_databricks' 
schema = 'aula'
tabela = 'Emerson'
jncao = f'{catalogo}.{schema}.{tabela}'


(
    df_categories.write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(f'{jncao}')

)

# COMMAND ----------

# MAGIC %md
# MAGIC Criar Schemas arquitetura medalion

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists curso_databricks.bronze;
# MAGIC create schema if not exists curso_databricks.silver;
# MAGIC create schema if not exists curso_databricks.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop schema if exists curso_databricks.bronze cascade;
# MAGIC drop schema if exists curso_databricks.silver cascade;
# MAGIC drop schema if exists curso_databricks.gold cascade;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists curso_databricks.aula.categories;
# MAGIC drop table if exists curso_databricks.aula.categories2;
# MAGIC drop table if exists curso_databricks.aula.categories3;
# MAGIC drop table if exists curso_databricks.aula.categories4;
# MAGIC drop table if exists curso_databricks.aula.emerson;
# MAGIC
# MAGIC