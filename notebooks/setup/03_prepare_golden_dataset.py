# Databricks notebook source
# MAGIC %md
# MAGIC # 📚 Preparación del Golden Dataset - DLCO QA Agent
# MAGIC 
# MAGIC **Proyecto:** Agente Databricks Genie para DataOps Corp v1.1  
# MAGIC **Cliente:** Grupo Credicorp  
# MAGIC **Autor:** ReinaldoB  
# MAGIC **Versión:** 1.0 — Mayo 2026
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 🎯 Objetivo
# MAGIC 
# MAGIC Crear un dataset de ejemplos de código PySpark etiquetados como "buenos" y "malos" para entrenar el agente de QA.
# MAGIC 
# MAGIC **Estructura del Dataset:**
# MAGIC - `code`: Código PySpark de ejemplo
# MAGIC - `expected_findings`: Array de findings esperados (vacío = código bueno)
# MAGIC - `quality`: "high" o "low"
# MAGIC - `framework_compliant`: TRUE o FALSE
# MAGIC - `description`: Descripción del ejemplo
# MAGIC - `category`: Tipo de validación (naming, secrets, paths, etc.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Configuración Inicial

# COMMAND ----------

import pandas as pd
from datetime import datetime

# Usar el catálogo correcto
spark.sql("USE CATALOG dlco_qa_agents")

print("✅ Catálogo 'dlco_qa_agents' activado")
print("📍 Dataset se guardará en: dlco_qa_agents.silver.golden_qa_eval_v1\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🟢 PARTE 1: Ejemplos de Código BUENO (High Quality)
# MAGIC 
# MAGIC Estos son ejemplos que cumplen todas las reglas DLCO y representan mejores prácticas.

# COMMAND ----------

good_examples = []

# EJEMPLO 1: Lectura correcta de tabla UC con logging
good_examples.append({
    "code": """import logging
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

def load_customer_data() -> DataFrame:
    \"\"\"Carga datos de clientes desde bronze.\"\"\"
    try:
        logger.info("Iniciando carga de datos de clientes")
        df = spark.table("bronze_customers")
        logger.info(f"Registros cargados: {df.count()}")
        return df
    except Exception as e:
        logger.error(f"Error cargando datos: {e}")
        raise
""",
    "expected_findings": [],
    "quality": "high",
    "framework_compliant": True,
    "description": "Lectura correcta de tabla UC con logging y manejo de errores",
    "category": "best_practice"
})

# EJEMPLO 2: Write con formato Delta y partición
good_examples.append({
    "code": """def save_customer_data(df):
    \"\"\"Guarda datos procesados en silver.\"\"\"
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("country", "year") \
        .saveAsTable("silver_customers_processed")
""",
    "expected_findings": [],
    "quality": "high",
    "framework_compliant": True,
    "description": "Write correcto con Delta, particiones y nombre de tabla conforme",
    "category": "best_practice"
})

# EJEMPLO 3: Uso correcto de secrets
good_examples.append({
    "code": """def get_api_client():
    \"\"\"Crea cliente API con credenciales seguras.\"\"\"
    api_key = dbutils.secrets.get(scope="dlco-secrets", key="api-key")
    api_url = dbutils.secrets.get(scope="dlco-secrets", key="api-url")
    
    return APIClient(url=api_url, key=api_key)
""",
    "expected_findings": [],
    "quality": "high",
    "framework_compliant": True,
    "description": "Uso correcto de dbutils.secrets para credenciales",
    "category": "security"
})

# EJEMPLO 4: CSV read con schema explícito
good_examples.append({
    "code": """from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.read \
    .format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .table("bronze_customers_raw")
""",
    "expected_findings": [],
    "quality": "high",
    "framework_compliant": True,
    "description": "CSV read con schema explícito para mejor performance",
    "category": "best_practice"
})

# EJEMPLO 5: Nombres correctos en snake_case
good_examples.append({
    "code": """def process_customer_transactions(customer_df, transaction_df):
    \"\"\"Procesa transacciones de clientes.\"\"\"
    joined_df = customer_df.join(transaction_df, "customer_id")
    result_df = joined_df.groupBy("customer_id").agg(
        sum("amount").alias("total_amount")
    )
    return result_df
""",
    "expected_findings": [],
    "quality": "high",
    "framework_compliant": True,
    "description": "Nombres de funciones y variables en snake_case correcto",
    "category": "naming_convention"
})

# EJEMPLO 6: Transformación con audit trail
good_examples.append({
    "code": """import logging

logger = logging.getLogger(__name__)

def transform_silver_to_gold(df):
    \"\"\"Transforma datos de silver a gold.\"\"\"
    logger.info("Iniciando transformación silver -> gold")
    
    result = df.select("customer_id", "total_amount", "country")
    
    logger.info(f"Transformación completada. Registros: {result.count()}")
    
    # Registrar en audit trail
    spark.sql(f\"\"\"
        INSERT INTO audit.transformations VALUES (
            '{datetime.now()}', 'silver_to_gold', {result.count()}
        )
    \"\"\")
    
    return result
""",
    "expected_findings": [],
    "quality": "high",
    "framework_compliant": True,
    "description": "Transformación con logging y registro en audit trail",
    "category": "audit"
})

# EJEMPLO 7: Uso de limit en lugar de collect
good_examples.append({
    "code": """def preview_data(df, n=10):
    \"\"\"Muestra preview de los datos.\"\"\"
    sample = df.limit(n)
    return sample.toPandas()
""",
    "expected_findings": [],
    "quality": "high",
    "framework_compliant": True,
    "description": "Uso de limit() en lugar de collect() para preview",
    "category": "best_practice"
})

# EJEMPLO 8: Encriptación de columnas sensibles
good_examples.append({
    "code": """from pyspark.sql.functions import sha2, col

def encrypt_pii_columns(df):
    \"\"\"Encripta columnas con datos sensibles.\"\"\"
    return df.withColumn(
        "ssn_encrypted", 
        sha2(col("ssn"), 256)
    ).withColumn(
        "email_encrypted",
        sha2(col("email"), 256)
    ).drop("ssn", "email")
""",
    "expected_findings": [],
    "quality": "high",
    "framework_compliant": True,
    "description": "Encriptación correcta de columnas sensibles (SSN, email)",
    "category": "security"
})

print(f"✅ {len(good_examples)} ejemplos de código BUENO creados")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🔴 PARTE 2: Ejemplos de Código MALO (Low Quality)
# MAGIC 
# MAGIC Estos son ejemplos con anti-patrones que deben ser detectados por el agente.

# COMMAND ----------

bad_examples = []

# EJEMPLO 1: Uso de print() en lugar de logging
bad_examples.append({
    "code": """def load_data():
    print("Loading customer data...")
    df = spark.table("bronze_customers")
    print(f"Loaded {df.count()} records")
    return df
""",
    "expected_findings": ["DLCO-001:HIGH:Uso de print() detectado. Usar logging en su lugar."],
    "quality": "low",
    "framework_compliant": False,
    "description": "Uso de print() en producción",
    "category": "anti_pattern"
})

# EJEMPLO 2: Path hardcoded
bad_examples.append({
    "code": """def load_raw_data():
    df = spark.read.parquet("/mnt/raw/customers/data.parquet")
    return df
""",
    "expected_findings": ["DLCO-003:MEDIUM:Path hardcoded detectado. Usar tablas de Unity Catalog."],
    "quality": "low",
    "framework_compliant": False,
    "description": "Path hardcoded en lugar de tabla UC",
    "category": "anti_pattern"
})

# EJEMPLO 3: Uso de collect() en DF grande
bad_examples.append({
    "code": """def get_all_customers():
    df = spark.table("bronze_customers")
    all_data = df.collect()  # Puede causar OOM
    return all_data
""",
    "expected_findings": ["DLCO-005:HIGH:Uso de .collect() detectado. Puede causar OOM en datasets grandes."],
    "quality": "low",
    "framework_compliant": False,
    "description": "Uso peligroso de collect() sin limit",
    "category": "anti_pattern"
})

# EJEMPLO 4: CSV read sin schema
bad_examples.append({
    "code": """def load_csv_data():
    df = spark.read.csv("bronze_customers_csv", header=True)
    return df
""",
    "expected_findings": ["DLCO-007:MEDIUM:CSV read sin schema explícito. Definir schema para mejor performance."],
    "quality": "low",
    "framework_compliant": False,
    "description": "CSV read sin schema explícito",
    "category": "anti_pattern"
})

# EJEMPLO 5: Overwrite sin partitionBy
bad_examples.append({
    "code": """def save_processed_data(df):
    df.write.mode("overwrite").saveAsTable("silver_customers")
""",
    "expected_findings": ["DLCO-009:MEDIUM:Overwrite sin partitionBy puede ser peligroso."],
    "quality": "low",
    "framework_compliant": False,
    "description": "Overwrite sin particiones",
    "category": "anti_pattern"
})

# EJEMPLO 6: Nombre de tabla no conforme
bad_examples.append({
    "code": """def save_output(df):
    df.write.saveAsTable("customer_final_output")
""",
    "expected_findings": ["DLCO-011:LOW:Verificar que nombres de tabla sigan convención layer_nombre."],
    "quality": "low",
    "framework_compliant": False,
    "description": "Nombre de tabla no sigue convención bronze/silver/gold",
    "category": "naming_convention"
})

# EJEMPLO 7: Sin manejo de errores
bad_examples.append({
    "code": """def process_data():
    df = spark.read.table("bronze_customers")
    result = df.filter("age > 18")
    return result
""",
    "expected_findings": ["DLCO-013:MEDIUM:Falta manejo de errores (try-except) en operaciones críticas."],
    "quality": "low",
    "framework_compliant": False,
    "description": "Sin try-except en operaciones críticas",
    "category": "anti_pattern"
})

# EJEMPLO 8: Credencial hardcoded
bad_examples.append({
    "code": """def connect_to_api():
    api_key = "sk-1234567890abcdef"
    api_url = "https://api.example.com"
    return APIClient(api_key, api_url)
""",
    "expected_findings": ["DLCO-015:CRITICAL:Posible credencial hardcoded detectada. Usar dbutils.secrets."],
    "quality": "low",
    "framework_compliant": False,
    "description": "API key hardcoded en el código",
    "category": "security"
})

# EJEMPLO 9: Write sin formato explícito
bad_examples.append({
    "code": """def save_data(df):
    df.write.saveAsTable("bronze_output")
""",
    "expected_findings": ["DLCO-017:MEDIUM:Especificar formato explícito. Se recomienda Delta Lake."],
    "quality": "low",
    "framework_compliant": False,
    "description": "Write sin especificar formato Delta",
    "category": "anti_pattern"
})

# EJEMPLO 10: Función con nombre en PascalCase (debería ser snake_case)
bad_examples.append({
    "code": """def ProcessCustomerData(df):
    return df.filter("status = 'active'")
""",
    "expected_findings": ["NAMING:MEDIUM:Función 'ProcessCustomerData' no usa snake_case."],
    "quality": "low",
    "framework_compliant": False,
    "description": "Nombre de función no usa snake_case",
    "category": "naming_convention"
})

# EJEMPLO 11: Password hardcoded
bad_examples.append({
    "code": """def get_database_connection():
    password = "MySecretPassword123"
    conn_string = f"jdbc:postgresql://host/db?password={password}"
    return conn_string
""",
    "expected_findings": ["DLCO-015:CRITICAL:Posible credencial hardcoded detectada. Usar dbutils.secrets."],
    "quality": "low",
    "framework_compliant": False,
    "description": "Password hardcoded en connection string",
    "category": "security"
})

# EJEMPLO 12: Columnas sensibles sin encriptar
bad_examples.append({
    "code": """def process_customer_pii(df):
    return df.select("customer_id", "ssn", "email", "phone")
""",
    "expected_findings": ["DAC:FAIL:Columnas sensibles (ssn, email, phone) sin encriptación"],
    "quality": "low",
    "framework_compliant": False,
    "description": "PII sin encriptación",
    "category": "security"
})

# EJEMPLO 13: Write sin audit trail
bad_examples.append({
    "code": """def save_to_gold(df):
    df.write.mode("overwrite").saveAsTable("gold_customer_summary")
""",
    "expected_findings": ["AUDIT:FAIL:Operaciones críticas (write) sin auditoría"],
    "quality": "low",
    "framework_compliant": False,
    "description": "Write sin logging ni audit trail",
    "category": "audit"
})

# EJEMPLO 14: Múltiples anti-patrones juntos
bad_examples.append({
    "code": """def load_and_process():
    print("Starting process")
    password = "admin123"
    df = spark.read.csv("/mnt/raw/data.csv")
    results = df.collect()
    df.write.saveAsTable("final_output")
""",
    "expected_findings": [
        "DLCO-001:HIGH:Uso de print() detectado. Usar logging en su lugar.",
        "DLCO-003:MEDIUM:Path hardcoded detectado. Usar tablas de Unity Catalog.",
        "DLCO-005:HIGH:Uso de .collect() detectado. Puede causar OOM en datasets grandes.",
        "DLCO-007:MEDIUM:CSV read sin schema explícito. Definir schema para mejor performance.",
        "DLCO-011:LOW:Verificar que nombres de tabla sigan convención layer_nombre.",
        "DLCO-013:MEDIUM:Falta manejo de errores (try-except) en operaciones críticas.",
        "DLCO-015:CRITICAL:Posible credencial hardcoded detectada. Usar dbutils.secrets.",
        "DLCO-017:MEDIUM:Especificar formato explícito. Se recomienda Delta Lake."
    ],
    "quality": "low",
    "framework_compliant": False,
    "description": "Múltiples anti-patrones en un solo código",
    "category": "anti_pattern"
})

print(f"✅ {len(bad_examples)} ejemplos de código MALO creados")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 💾 PARTE 3: Guardar el Golden Dataset

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

# Combinar todos los ejemplos
all_examples = good_examples + bad_examples

# Crear DataFrame
df_golden = spark.createDataFrame(all_examples)

# Agregar metadata
df_golden = df_golden.withColumn("created_at", current_timestamp())
df_golden = df_golden.withColumn("version", lit("v1.0"))
df_golden = df_golden.withColumn("source", lit("synthetic"))

# Guardar en silver
df_golden.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dlco_qa_agents.silver.golden_qa_eval_v1")

print(f"""
╔═══════════════════════════════════════════════════════════════╗
║           ✅ GOLDEN DATASET CREADO EXITOSAMENTE              ║
╚═══════════════════════════════════════════════════════════════╝

📊 Estadísticas:
   • Total de ejemplos: {len(all_examples)}
   • Ejemplos buenos (high quality): {len(good_examples)}
   • Ejemplos malos (low quality): {len(bad_examples)}

📂 Ubicación: dlco_qa_agents.silver.golden_qa_eval_v1

🎯 Próximo paso: Crear Vector Search Index
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📊 PARTE 4: Verificación y Exploración del Dataset

# COMMAND ----------

# Verificar el dataset guardado
df_saved = spark.table("dlco_qa_agents.silver.golden_qa_eval_v1")

print("📊 Dataset Overview:\n")
df_saved.printSchema()

print(f"\n📈 Total de registros: {df_saved.count()}")

# COMMAND ----------

# Distribución por calidad
print("\n📊 Distribución por Calidad:\n")
df_saved.groupBy("quality").count().show()

# COMMAND ----------

# Distribución por categoría
print("\n📊 Distribución por Categoría:\n")
df_saved.groupBy("category").count().show()

# COMMAND ----------

# Ver ejemplos de código bueno
print("\n🟢 Ejemplos de Código BUENO:\n")
df_saved.filter("quality = 'high'").select("description", "category").show(truncate=False)

# COMMAND ----------

# Ver ejemplos de código malo
print("\n🔴 Ejemplos de Código MALO:\n")
df_saved.filter("quality = 'low'").select("description", "category").show(truncate=False)

# COMMAND ----------

print("""
╔═══════════════════════════════════════════════════════════════╗
║        ✅ DÍA 3 COMPLETADO - GOLDEN DATASET LISTO            ║
╚═══════════════════════════════════════════════════════════════╝

📦 Dataset creado: dlco_qa_agents.silver.golden_qa_eval_v1

✅ Lo que tenemos:
   • 8 ejemplos de código bueno (best practices)
   • 14 ejemplos de código malo (anti-patterns)
   • Total: 22 ejemplos etiquetados

🎯 Próximo paso: DÍA 4 - Vector Search Index
   Crear índice vectorial para RAG con databricks-gte-large-en
""")