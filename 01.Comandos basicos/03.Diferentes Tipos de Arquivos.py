# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Formatos de Arquivos em Big Data
# MAGIC %md
# MAGIC # Diferentes Tipos de Arquivos no Spark
# MAGIC
# MAGIC ## Principais Formatos Usados em Big Data:
# MAGIC
# MAGIC ### 1. **Parquet** (Formato Colunar)
# MAGIC * Formato colunar otimizado para leitura
# MAGIC * Compressão eficiente
# MAGIC * Melhor performance para queries analíticas
# MAGIC * Suporta schemas complexos
# MAGIC
# MAGIC ### 2. **Delta Lake** (Formato Transacional)
# MAGIC * Formato nativo do Databricks
# MAGIC * ACID transactions
# MAGIC * Time travel (versionamento)
# MAGIC * Schema evolution
# MAGIC * Upserts e deletes eficientes
# MAGIC
# MAGIC ### 3. **JSON** (Formato Semi-Estruturado)
# MAGIC * Formato legível por humanos
# MAGIC * Flexível para dados semi-estruturados
# MAGIC * Maior tamanho de arquivo
# MAGIC * Mais lento para processar
# MAGIC
# MAGIC ### 4. **CSV** (Formato Texto)
# MAGIC * Formato simples e universal
# MAGIC * Legível por humanos
# MAGIC * Sem suporte nativo a tipos complexos
# MAGIC * Requer definição de schema
# MAGIC
# MAGIC ### 5. **ORC** (Optimized Row Columnar)
# MAGIC * Formato colunar otimizado
# MAGIC * Compressão eficiente
# MAGIC * Popular no ecossistema Hive
# MAGIC * Suporta ACID transactions
# MAGIC
# MAGIC ### 6. **Avro** (Formato de Linha)
# MAGIC * Formato binário compacto
# MAGIC * Schema incluído no arquivo
# MAGIC * Otimizado para escrita
# MAGIC * Bom para streaming e serialização

# COMMAND ----------

df_categories = spark.read.csv("/Volumes/curso_databricks/aula/aula_volume/Bike Store/categories.csv", header=True, inferSchema=True)
df_categories.show()

# COMMAND ----------

# DBTITLE 1,Seção: Salvando Dados
# MAGIC %md
# MAGIC ## PARTE 1: Salvando Dados em Diferentes Formatos
# MAGIC Vamos salvar o DataFrame em todos os formatos principais

# COMMAND ----------

# DBTITLE 1,1. Salvar em Parquet
# 1. PARQUET - Formato Colunar Otimizado
parquet_path = "/Volumes/curso_databricks/aula/aula_volume/Bike Store/formats/parquet"
df_categories.write.mode("overwrite").parquet(parquet_path)


# COMMAND ----------

# DBTITLE 1,2. Salvar em Delta
# 2. DELTA - Formato Transacional do Databricks
delta_path = "/Volumes/curso_databricks/aula/aula_volume/Bike Store/formats/delta"
df_categories.write.mode("overwrite").format("delta").save(delta_path)


# COMMAND ----------

# DBTITLE 1,3. Salvar em JSON
# 3. JSON - Formato Semi-Estruturado
json_path = "/Volumes/curso_databricks/aula/aula_volume/Bike Store/formats/json"
df_categories.write.mode("overwrite").json(json_path)

# COMMAND ----------

# DBTITLE 1,4. Salvar em CSV
# 4. CSV - Formato Texto Simples
csv_path = "/Volumes/curso_databricks/aula/aula_volume/Bike Store/formats/csv"
df_categories.write.mode("overwrite").option("header", "true").csv(csv_path)

# COMMAND ----------

# DBTITLE 1,5. Salvar em ORC
# 5. ORC - Optimized Row Columnar
orc_path = "/Volumes/curso_databricks/aula/aula_volume/Bike Store/formats/orc"
df_categories.write.mode("overwrite").orc(orc_path)

# COMMAND ----------

# DBTITLE 1,6. Salvar em Avro
# 6. AVRO - Formato de Linha Compacto
avro_path = "/Volumes/curso_databricks/aula/aula_volume/Bike Store/formats/avro"
df_categories.write.mode("overwrite").format("avro").save(avro_path)

# COMMAND ----------

# DBTITLE 1,Seção: Lendo Dados
# MAGIC %md
# MAGIC ##  PARTE 2: Lendo Dados de Diferentes Formatos
# MAGIC Vamos ler os dados salvos em cada formato e exibir os resultados

# COMMAND ----------

# DBTITLE 1,1. Ler Parquet
# 1. LER PARQUET
df_parquet = spark.read.parquet(parquet_path)
df_parquet.show()

# COMMAND ----------

# DBTITLE 1,2. Ler Delta
# 2. LER DELTA
df_delta = spark.read.format("delta").load(delta_path)
df_delta.show()

# COMMAND ----------

# DBTITLE 1,3. Ler JSON
# 3. LER JSON
df_json = spark.read.json(json_path)
df_json.show()

# COMMAND ----------

# DBTITLE 1,4. Ler CSV
# 4. LER CSV
df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
df_csv.show()

# COMMAND ----------

# DBTITLE 1,5. Ler ORC
# 5. LER ORC
df_orc = spark.read.orc(orc_path)
df_orc.show()


# COMMAND ----------

# DBTITLE 1,6. Ler Avro
# 6. LER AVRO
df_avro = spark.read.format("avro").load(avro_path)
df_avro.show()


# COMMAND ----------

# DBTITLE 1,Seção: Comparação
# MAGIC %md
# MAGIC ## PARTE 3: Comparação de Tamanhos e Performance
# MAGIC Vamos comparar o tamanho dos arquivos gerados em cada formato

# COMMAND ----------

# DBTITLE 1,Comparação de Tamanhos
import os

def get_directory_size(path):
    """Calcula o tamanho total de um diretório em bytes"""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path.replace("/Volumes/", "/Volumes/")):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            try:
                total_size += os.path.getsize(filepath)
            except:
                pass
    return total_size

def bytes_to_readable(bytes_size):
    """Converte bytes para formato legível"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"

formats_paths = {
    'Parquet': parquet_path,
    'Delta': delta_path,
    'JSON': json_path,
    'CSV': csv_path,
    'ORC': orc_path,
    'Avro': avro_path
}

print("\n" + "=" * 70)
print("COMPARAÇÃO DE TAMANHO DOS ARQUIVOS")
print("=" * 70)

for format_name, path in formats_paths.items():
    # Remove o prefixo /Volumes/ e adiciona /Volumes/ para o caminho real
    real_path = path.replace("/Volumes/", "/Volumes/")
    try:
        size = get_directory_size(real_path)
        readable_size = bytes_to_readable(size)
        print(f"{format_name:12} -> {readable_size:>12}")
    except Exception as e:
        print(f"{format_name:12} -> Erro ao calcular tamanho")

print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Melhores Práticas
# MAGIC %md
# MAGIC ##  PARTE 4: Quando Usar Cada Formato?
# MAGIC
# MAGIC ###  Recomendações de Uso:
# MAGIC
# MAGIC | Formato | Melhor Para | Evitar Quando |
# MAGIC |---------|-------------|---------------|
# MAGIC | **Parquet** | Data lakes, queries analíticas, leitura intensiva | Dados que mudam frequentemente |
# MAGIC | **Delta** | Pipelines de dados, operações Schema, data lakes transacionais  versionamento e controle CICD | Sistemas legados sem suporte |
# MAGIC | **JSON** | APIs, dados semi-estruturados, interoperabilidade | Grandes volumes de dados |
# MAGIC | **CSV** | Export/import simples, Excel, ferramentas legadas | Dados com schemas complexos |
# MAGIC | **ORC** | Ecossistema Hive, compressão máxima | Quando não usa Hive |
# MAGIC | **Avro** | Streaming, Kafka, schema evolution | Queries analíticas |
# MAGIC
# MAGIC ###  Boas Práticas:
# MAGIC
# MAGIC * **Para Data Engineering no Databricks**: Use **Delta** como padrão
# MAGIC * **Para Analytics/BI**: Use **Parquet** ou **Delta**
# MAGIC * **Para Integrações Externas**: Use **JSON** ou **CSV**
# MAGIC * **Para Streaming**: Use **Avro** ou **Delta**
# MAGIC * **Para Compressão Máxima**: Use **ORC** ou **Parquet com compressão**
# MAGIC
# MAGIC ###  Opções Avançadas:
# MAGIC
# MAGIC ```python
# MAGIC # Parquet com compressão
# MAGIC df.write.option("compression", "snappy").parquet(path)
# MAGIC
# MAGIC # Delta com otimização
# MAGIC df.write.format("delta").option("optimizeWrite", "true").save(path)
# MAGIC
# MAGIC # Particionamento
# MAGIC df.write.partitionBy("coluna").parquet(path)
# MAGIC
# MAGIC # Modo de escrita
# MAGIC df.write.mode("overwrite")  # append, overwrite, ignore, error
# MAGIC ```