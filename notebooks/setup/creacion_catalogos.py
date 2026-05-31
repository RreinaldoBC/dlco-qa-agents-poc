# Databricks notebook source
# Databricks notebook
catalog_name = "dlco_qa_agents"
catalog_location = "abfss://dlco-qa-agents@deacoursestdatalake.dfs.core.windows.net/"

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {catalog_name}
MANAGED LOCATION '{catalog_location}'
COMMENT 'Catálogo para agentes de QA del framework DataOps Corp'
""")

print(f"✅ Catálogo '{catalog_name}' creado")

# Usar el catálogo
spark.sql(f"USE CATALOG {catalog_name}")

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
# Verificar que todo se creó correctamente
print("""
╔═══════════════════════════════════════════════════════╗
║   ✅ SETUP UC COMPLETADO EXITOSAMENTE                ║
╚═══════════════════════════════════════════════════════╝
""")

# Listar schemas
print("📂 Schemas en dlco_qa_agents:")
display(spark.sql("SHOW SCHEMAS IN dlco_qa_agents"))

# Listar tablas en audit
print("\n📊 Tablas en audit:")
display(spark.sql("SHOW TABLES IN dlco_qa_agents.audit"))