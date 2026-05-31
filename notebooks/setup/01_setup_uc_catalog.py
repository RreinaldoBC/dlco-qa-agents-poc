# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Unity Catalog - DLCO QA Agents
# MAGIC 
# MAGIC Este notebook crea la estructura del catálogo en Unity Catalog para el proyecto DLCO QA Agent.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificar y Crear Catálogo con Managed Location

# COMMAND ----------

# Obtener metastore info
metastore_info = spark.sql("DESCRIBE METASTORE").collect()
print("📊 Información del Metastore:")
for row in metastore_info:
    print(f"   {row.info_name}: {row.info_value}")

# COMMAND ----------

# Intentar crear el catálogo primero sin MANAGED LOCATION
# Si falla, lo crearemos con MANAGED LOCATION en el siguiente paso

try:
    spark.sql("""
    CREATE CATALOG IF NOT EXISTS dlco_qa_agents
    COMMENT 'Catálogo para agentes de QA de código del framework DLCO'
    """)
    print("✅ Catálogo 'dlco_qa_agents' creado sin MANAGED LOCATION")
except Exception as e:
    error_msg = str(e)
    if "storage root URL does not exist" in error_msg or "MANAGED LOCATION" in error_msg:
        print("⚠️ Se requiere MANAGED LOCATION. Creando con ubicación de almacenamiento...")
        
        # Crear con MANAGED LOCATION usando el storage del metastore
        # Obtener el storage root del metastore
        metastore_storage = None
        for row in metastore_info:
            if row.info_name == "Storage root":
                metastore_storage = row.info_value
                break
        
        if metastore_storage:
            catalog_location = f"{metastore_storage}/dlco_qa_agents"
            print(f"📍 Usando ubicación: {catalog_location}")
            
            spark.sql(f"""
            CREATE CATALOG IF NOT EXISTS dlco_qa_agents
            MANAGED LOCATION '{catalog_location}'
            COMMENT 'Catálogo para agentes de QA de código del framework DLCO'
            """)
            print("✅ Catálogo 'dlco_qa_agents' creado con MANAGED LOCATION")
        else:
            print("❌ No se pudo obtener el storage root del metastore")
            raise e
    else:
        print(f"❌ Error inesperado: {error_msg}")
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Usar el Catálogo y Crear Schemas

# COMMAND ----------

# Usar el catálogo
spark.sql("USE CATALOG dlco_qa_agents")

# Crear schemas
schemas = [
    ("bronze", "Reglas y artefactos de los agentes"),
    ("silver", "Golden datasets y evaluaciones"),
    ("gold", "Reportes y dashboards de QA"),
    ("tools", "Unity Catalog Functions"),
    ("audit", "Logs de inference y tracking"),
    ("vectors", "Vector Search indexes")
]

for schema_name, comment in schemas:
    spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {schema_name}
    COMMENT '{comment}'
    """)
    print(f"✅ Schema '{schema_name}' creado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificar Estructura

# COMMAND ----------

# Listar todos los schemas
print("\n📊 Schemas creados en dlco_qa_agents:\n")
schemas_df = spark.sql("SHOW SCHEMAS IN dlco_qa_agents")
display(schemas_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Crear Tablas Iniciales

# COMMAND ----------

# Tabla para tracking de validaciones
spark.sql("""
CREATE TABLE IF NOT EXISTS dlco_qa_agents.audit.pr_verdicts (
    pr_id STRING,
    pr_url STRING,
    pr_verdict STRING,
    findings_count INT,
    critical_count INT,
    high_count INT,
    medium_count INT,
    low_count INT,
    execution_time_seconds DOUBLE,
    agent_version STRING,
    created_at TIMESTAMP,
    created_by STRING
)
USING DELTA
COMMENT 'Registro de validaciones de PRs'
""")

print("✅ Tabla 'audit.pr_verdicts' creada")

# COMMAND ----------

# Tabla para findings individuales
spark.sql("""
CREATE TABLE IF NOT EXISTS dlco_qa_agents.audit.agent_findings (
    finding_id STRING,
    pr_id STRING,
    rule_id STRING,
    severity STRING,
    message STRING,
    file_path STRING,
    line_number INT,
    code_snippet STRING,
    agent_name STRING,
    created_at TIMESTAMP
)
USING DELTA
COMMENT 'Findings individuales detectados por los agentes'
""")

print("✅ Tabla 'audit.agent_findings' creada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumen Final

# COMMAND ----------

# Verificar que todo se creó correctamente
print("""
╔═══════════════════════════════════════════════════════╗
║   ✅ SETUP COMPLETADO EXITOSAMENTE                    ║
╚═══════════════════════════════════════════════════════╝

📦 Catálogo: dlco_qa_agents
""")

# Listar schemas
schemas_list = [row.databaseName for row in spark.sql("SHOW SCHEMAS IN dlco_qa_agents").collect()]
print("📂 Schemas:")
for schema in schemas_list:
    if schema != "information_schema":
        print(f"   • {schema}")

# Listar tablas en audit
print("\n📊 Tablas en audit:")
tables = spark.sql("SHOW TABLES IN dlco_qa_agents.audit").collect()
for table in tables:
    print(f"   • {table.tableName}")

print("""
🎯 Próximo paso: Registrar UC Functions
   Ejecutar: 02_register_uc_tools.py
""")