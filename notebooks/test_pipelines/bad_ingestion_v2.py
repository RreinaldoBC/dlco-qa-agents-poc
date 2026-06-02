# ============================================================================
# ARCHIVO DE PRUEBA - CÓDIGO INTENCIONALMENTE MALO
# ============================================================================
# Este archivo contiene múltiples anti-patrones DLCO para probar que el agente
# DLCO QA los detecta correctamente cuando se abre el PR.
#
# Anti-patrones intencionalmente incluidos:
#   1. DLCO-001: print() en producción          (HIGH)
#   2. DLCO-003: path hardcoded /mnt/...        (MEDIUM)
#   3. DLCO-005: df.collect() sin limit         (HIGH)
#   4. DLCO-007: CSV read sin schema explícito  (MEDIUM)
#   5. DLCO-013: sin manejo de errores          (MEDIUM)
#   6. DLCO-015: password hardcoded             (CRITICAL) ← bloquea el merge
#   7. DLCO-017: write sin formato Delta        (MEDIUM)
#
# Expected verdict: REJECTED (debido al CRITICAL de password hardcoded)
# ============================================================================


def ingest_customer_pipeline_v2():
    """
    Pipeline de ingesta NUEVO desarrollado por el equipo.
    Este código tiene múltiples problemas que el agente DLCO debe detectar.
    """
    # ❌ Problema 1: print() en lugar de logging
    print("Starting customer ingestion pipeline...")

    # ❌ Problema 2: password hardcoded (CRITICAL - bloquea merge)
    database_password = "MySuperSecretPassword2024"
    api_token = "sk-abc123def456ghi789"

    # ❌ Problema 3: path hardcoded en lugar de Unity Catalog
    raw_data_path = "/mnt/landing/customers/raw_data.csv"

    # ❌ Problema 4: CSV read sin schema explícito
    df = spark.read.csv(raw_data_path, header=True)

    # ❌ Problema 5: collect() sin limit (riesgo OOM)
    all_customers = df.collect()

    # Procesamiento
    filtered = df.filter("status = 'active'")

    # ❌ Problema 6: write sin formato Delta explícito
    # ❌ Problema 7: sin partitionBy en overwrite
    filtered.write.mode("overwrite").saveAsTable("customers_output")

    # ❌ Problema 8: print en lugar de logging
    print(f"Processed {len(all_customers)} customers")

    return filtered


# Llamada principal
if __name__ == "__main__":
    result = ingest_customer_pipeline_v2()
