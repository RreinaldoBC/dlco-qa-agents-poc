# ============================================================================
# ARCHIVO DE PRUEBA - CÓDIGO BUENO (BEST PRACTICES)
# ============================================================================
# Este archivo cumple TODAS las reglas DLCO. Se usará como comparación al
# corregir el archivo bad_ingestion_v2.py durante el test end-to-end.
#
# Mejores prácticas implementadas:
#   ✅ logging en lugar de print()
#   ✅ dbutils.secrets para credenciales
#   ✅ Unity Catalog en lugar de paths /mnt/
#   ✅ Schema explícito en CSV reads
#   ✅ try/except para manejo de errores
#   ✅ Formato Delta explícito
#   ✅ partitionBy en writes
#   ✅ Logging de audit trail
#
# Expected verdict: APPROVED con quality_score alto
# ============================================================================

import logging
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, BooleanType
)

logger = logging.getLogger(__name__)


def ingest_customer_pipeline_v2():
    """
    Pipeline de ingesta de clientes siguiendo best practices DLCO.

    Carga datos desde landing layer, los procesa y los persiste en bronze.
    Integra logging completo y manejo de errores.
    """
    # ❌ Problema 1: print() en lugar de logging
    print("Starting customer ingestion pipeline...")

    # ❌ Problema 2: password hardcoded (CRITICAL - bloquea merge)
    database_password = "MySuperSecretPassword2024"
    api_token = "sk-abc123def456ghi789"

    try:
        logger.info("Starting customer ingestion pipeline v2")

        # ✅ Credenciales desde secret scope
        database_password = dbutils.secrets.get(
            scope="dlco-secrets", key="db-password"
        )
        api_token = dbutils.secrets.get(
            scope="dlco-secrets", key="api-token"
        )

        # ✅ Schema explícito para mejor performance y type safety
        customer_schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("country", StringType(), True),
            StructField("active", BooleanType(), True),
            StructField("created_at", TimestampType(), True),
        ])

        # ✅ Unity Catalog en lugar de paths /mnt/
        df = (
            spark.read
            .format("csv")
            .schema(customer_schema)
            .option("header", "true")
            .table("landing_customers_raw")
        )

        # ✅ Filtrado sin collect()
        filtered = df.filter("status = 'active'")
        record_count = filtered.count()

        logger.info(f"Filtered {record_count} active customers")

        # ✅ Write con Delta + partitionBy
        (
            filtered.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("country")
            .saveAsTable("bronze_customers_v2")
        )

        # ✅ Audit trail explícito
        from datetime import datetime
        spark.sql(f"""
            INSERT INTO dlco_qa_agents.audit.pr_verdicts
            (pr_id, pr_url, pr_verdict, findings_count, created_at, created_by)
            VALUES (
                'ingestion-v2-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
                'https://internal.com/jobs/ingest-v2',
                'success',
                0,
                current_timestamp(),
                'ingest_customer_pipeline_v2'
            )
        """)

        logger.info(f"Pipeline completed successfully. Records: {record_count}")
        return filtered

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
