# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
"""
Catalog
│
├── Schema
│   │
│   ├── Tabela
│   └── Volume

"""

# COMMAND ----------

# MAGIC %md
# MAGIC Create catalogs:https://learn.microsoft.com/en-us/azure/databricks/catalogs/create-catalog
# MAGIC
# MAGIC create-schema:https://learn.microsoft.com/en-us/azure/databricks/schemas/create-schema
# MAGIC
# MAGIC Azure Databricks tables concepts:https://learn.microsoft.com/en-us/azure/databricks/tables/tables-concepts
# MAGIC
# MAGIC create Volumes:https://learn.microsoft.com/en-us/azure/databricks/volumes/utility-commands
# MAGIC
# MAGIC create table:https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-table-using
# MAGIC
# MAGIC
# MAGIC comandos SQL:https://docs.databricks.com/aws/en/sql/language-manual/
# MAGIC
# MAGIC

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS example;")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS example4;

# COMMAND ----------

# deletar catalog
spark.sql("drop CATALOG if exists bikestore cascade;")


# COMMAND ----------

########## criar 
#criar catalog
#criar schema
#criar tabela
#criar volume

#### deletar 

#drop table
#drop volume
#drop schema
#drop catalog

# COMMAND ----------

#criar catalog
spark.sql("CREATE CATALOG IF NOT EXISTS teste;")

# COMMAND ----------

#criar schema
spark.sql("CREATE SCHEMA IF NOT EXISTS teste.schema1;")

# COMMAND ----------

#criar tabela
spark.sql("CREATE TABLE IF NOT EXISTS teste.schema1.tabela1 (id INT, name STRING) using DELTA;")

# COMMAND ----------

 #criar volume
 spark.sql("CREATE VOLUME IF NOT EXISTS teste.schema1.volume1;")


# COMMAND ----------

#deletar tabela
spark.sql("DROP TABLE IF EXISTS teste.schema1.tabela1")




# COMMAND ----------

#delete volume
spark.sql("DROP VOLUME IF EXISTS teste.schema1.volume1")


# COMMAND ----------

#delete schema
spark.sql("DROP SCHEMA IF EXISTS teste.schema1")


# COMMAND ----------

#deletar catalog
spark.sql("DROP CATALOG IF EXISTS teste cascade")

# COMMAND ----------

# MAGIC %md
# MAGIC Criar ambiente oficial do curso

# COMMAND ----------


# criar arquitetura oficial
spark.sql("CREATE CATALOG IF NOT EXISTS curso_databricks")
spark.sql("CREATE SCHEMA IF NOT EXISTS curso_databricks.aula")
#Tabelas vamos fazer ao decorrer das aulas

spark.sql("""
CREATE VOLUME IF NOT EXISTS curso_databricks.aula.aula_volume
""")


# COMMAND ----------

