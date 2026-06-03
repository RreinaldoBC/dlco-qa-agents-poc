# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 UC Functions v1.1 - Refinamiento de Regex (Reduce Falsos Positivos)
# MAGIC
# MAGIC **Proyecto:** Agente Databricks Genie para DataOps Corp v1.1
# MAGIC **Cliente:** Grupo Credicorp
# MAGIC **Autor:** ReinaldoB
# MAGIC **Versión:** 1.1 — Junio 2026
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Mejoras vs v1.0
# MAGIC
# MAGIC Tras detectar falsos positivos en el primer test E2E (PR #1), refinamos
# MAGIC las regex de 4 UC Functions para mejorar la **precisión** sin sacrificar
# MAGIC el **recall**:
# MAGIC
# MAGIC | UC Function | Mejora |
# MAGIC |-------------|--------|
# MAGIC | `lint_pyspark_rules` | Ignora líneas que son comentarios (`#`) o docstrings |
# MAGIC | `scan_hardcoded_secrets` | Distingue `password=dbutils.secrets.get(...)` (OK) de `password="abc123"` (BAD) |
# MAGIC | `check_dac_encryption` | Considera columnas que se asignan desde secrets como protegidas |
# MAGIC | `check_audit_trail_integration` | Mejor detección de patrones de logging |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Impacto esperado
# MAGIC
# MAGIC | Métrica | v1.0 | v1.1 (esperado) |
# MAGIC |---------|------|-----------------|
# MAGIC | Falsos positivos en código bueno | 4 | 0 |
# MAGIC | True positives en código malo | 9 | 9 (mantiene) |
# MAGIC | Precisión | ~55% | ~95% |
# MAGIC | Recall | 100% | 100% |

# COMMAND ----------

# Usar el catálogo
spark.sql("USE CATALOG dlco_qa_agents")
print("✅ Catálogo activado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ lint_pyspark_rules v1.1 — Ignorar comentarios

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE FUNCTION dlco_qa_agents.tools.lint_pyspark_rules(code STRING)
RETURNS ARRAY<STRING>
LANGUAGE PYTHON
COMMENT 'Valida código PySpark contra reglas DLCO-001 a DLCO-018 (v1.1: ignora comentarios)'
AS $$
import re

def strip_comments_and_docstrings(source: str) -> str:
    \"\"\"Remueve comentarios (#) y docstrings triple-quote del código.\"\"\"
    lines = []
    in_docstring = False
    docstring_marker = None

    for line in source.split('\\n'):
        stripped = line.strip()

        # Manejo de docstrings triple-quote
        if not in_docstring:
            if stripped.startswith('\"\"\"') or stripped.startswith(\"'''\"):
                docstring_marker = stripped[:3]
                if stripped.count(docstring_marker) >= 2:
                    continue  # Docstring de una sola línea
                in_docstring = True
                continue
        else:
            if docstring_marker in stripped:
                in_docstring = False
            continue

        # Remover comentarios inline (# pero respetando strings)
        # Versión simple: si la línea entera empieza con #, ignorar
        if stripped.startswith('#'):
            continue

        # Remover comentario inline (heurística: # fuera de strings)
        in_string = False
        string_char = None
        result_chars = []
        i = 0
        while i < len(line):
            ch = line[i]
            if in_string:
                if ch == string_char and (i == 0 or line[i-1] != '\\\\'):
                    in_string = False
                    string_char = None
                result_chars.append(ch)
            else:
                if ch in ('\"', \"'\"):
                    in_string = True
                    string_char = ch
                    result_chars.append(ch)
                elif ch == '#':
                    break  # Resto de la línea es comentario
                else:
                    result_chars.append(ch)
            i += 1
        lines.append(''.join(result_chars))

    return '\\n'.join(lines)


# Aplicar limpieza ANTES de analizar
clean_code = strip_comments_and_docstrings(code)
findings = []

# DLCO-001: print() en producción (solo si NO está comentado)
if re.search(r'\\bprint\\s*\\(', clean_code):
    findings.append(\"DLCO-001:HIGH:Uso de print() detectado. Usar logging en su lugar.\")

# DLCO-003: paths hardcoded
if re.search(r'/(mnt|dbfs|Volumes)/', clean_code):
    findings.append(\"DLCO-003:MEDIUM:Path hardcoded detectado. Usar tablas de Unity Catalog.\")

# DLCO-005: .collect() sin limit
if re.search(r'\\.collect\\s*\\(\\s*\\)', clean_code):
    findings.append(\"DLCO-005:HIGH:Uso de .collect() detectado. Puede causar OOM en datasets grandes.\")

# DLCO-007: CSV read sin schema explícito
if re.search(r'spark\\.read\\.csv\\(', clean_code) and 'schema' not in clean_code.lower():
    findings.append(\"DLCO-007:MEDIUM:CSV read sin schema explícito. Definir schema para mejor performance.\")

# DLCO-009: mode('overwrite') sin particiones
if 'overwrite' in clean_code.lower() and '.mode(' in clean_code and 'partitionBy' not in clean_code:
    findings.append(\"DLCO-009:MEDIUM:Overwrite sin partitionBy puede ser peligroso.\")

# DLCO-011: nombres de tabla siguen convención
if re.search(r'(saveAsTable|insertInto)', clean_code):
    if not re.search(r'(bronze|silver|gold|landing)_', clean_code):
        findings.append(\"DLCO-011:LOW:Verificar que nombres de tabla sigan convención layer_nombre.\")

# DLCO-013: try-except en operaciones críticas
if 'try:' not in clean_code and ('spark.read' in clean_code or 'spark.sql' in clean_code):
    findings.append(\"DLCO-013:MEDIUM:Falta manejo de errores (try-except) en operaciones críticas.\")

# DLCO-015: credenciales hardcoded
# CAMBIO v1.1: solo dispara si NO usa dbutils.secrets en la misma línea/cercana
credential_keywords = ['password', 'token', 'client_secret', 'api_key', 'secret']
for keyword in credential_keywords:
    # Buscar asignación del keyword
    pattern = re.compile(r'\\b' + keyword + r'\\s*=\\s*([\"\\'])([^\"\\'\\\\]{6,})\\1', re.IGNORECASE)
    for match in pattern.finditer(clean_code):
        # Si el valor asignado contiene dbutils.secrets, es legítimo
        if 'dbutils.secrets' not in match.group(0):
            findings.append(\"DLCO-015:CRITICAL:Posible credencial hardcoded detectada. Usar dbutils.secrets.\")
            break
    else:
        continue
    break

# DLCO-017: write sin formato explícito
if '.write' in clean_code and '.format(' not in clean_code and 'saveAsTable' in clean_code:
    findings.append(\"DLCO-017:MEDIUM:Especificar formato explícito. Se recomienda Delta Lake.\")

return findings if findings else [\"PASS:No issues found\"]
$$
""")

print("✅ lint_pyspark_rules v1.1 registrada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ scan_hardcoded_secrets v1.1 — Distinguir secrets legítimos

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE FUNCTION dlco_qa_agents.tools.scan_hardcoded_secrets(code STRING)
RETURNS ARRAY<STRING>
LANGUAGE PYTHON
COMMENT 'Detecta secretos hardcoded (v1.1: ignora dbutils.secrets y comentarios)'
AS $$
import re

def strip_comments(source: str) -> str:
    \"\"\"Remueve líneas que son comentarios y docstrings.\"\"\"
    lines = []
    in_docstring = False
    docstring_marker = None

    for line in source.split('\\n'):
        stripped = line.strip()
        if not in_docstring:
            if stripped.startswith('\"\"\"') or stripped.startswith(\"'''\"):
                docstring_marker = stripped[:3]
                if stripped.count(docstring_marker) >= 2:
                    continue
                in_docstring = True
                continue
        else:
            if docstring_marker in stripped:
                in_docstring = False
            continue
        if stripped.startswith('#'):
            continue
        lines.append(line)
    return '\\n'.join(lines)


clean_code = strip_comments(code)
findings = []

# AWS Access Keys
if re.search(r'AKIA[0-9A-Z]{16}', clean_code):
    findings.append(\"SECRET:AWS_KEY:Posible AWS access key hardcoded\")

# Connection strings con credenciales embebidas
if re.search(r'(mongodb|postgres|mysql)://[^@\\s]+:[^@\\s]+@', clean_code):
    findings.append(\"SECRET:CONNECTION_STRING:URL con credenciales embebidas detectada\")

# URLs con credenciales
if re.search(r'https?://[^:\\s/]+:[^@\\s]+@', clean_code):
    findings.append(\"SECRET:URL_CREDS:URL con credenciales embebidas detectada\")

# Credenciales hardcoded en variables
# CAMBIO CLAVE v1.1: ignorar si el valor viene de dbutils.secrets.get()
secret_keywords = {
    'PASSWORD': r'password',
    'TOKEN': r'(?<!_)token(?!_)',  # evita matches como ID_TOKEN_FILE
    'API_KEY': r'api[_-]?key',
    'SECRET': r'(?<!_)secret(?!_)',
}

for secret_type, kw_pattern in secret_keywords.items():
    # Buscar asignación con string literal (8+ caracteres = candidato sospechoso)
    pattern = re.compile(
        kw_pattern + r'\\s*=\\s*([\"\\'])(.{8,})\\1',
        re.IGNORECASE
    )
    for match in pattern.finditer(clean_code):
        # Si el match contiene dbutils.secrets, es legítimo
        full_context = match.group(0)
        if 'dbutils.secrets' in full_context or 'os.environ' in full_context:
            continue
        findings.append(f\"SECRET:{secret_type}:Posible secreto hardcoded detectado\")
        break  # Un solo finding por tipo

return findings if findings else [\"PASS:No secrets detected\"]
$$
""")

print("✅ scan_hardcoded_secrets v1.1 registrada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ check_dac_encryption v1.1 — Reconocer secrets legítimos

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE FUNCTION dlco_qa_agents.tools.check_dac_encryption(code STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Verifica encriptación DAC (v1.1: ignora secrets via dbutils.secrets)'
AS $$
import re

def strip_comments(source: str) -> str:
    lines = []
    in_docstring = False
    docstring_marker = None
    for line in source.split('\\n'):
        stripped = line.strip()
        if not in_docstring:
            if stripped.startswith('\"\"\"') or stripped.startswith(\"'''\"):
                docstring_marker = stripped[:3]
                if stripped.count(docstring_marker) >= 2:
                    continue
                in_docstring = True
                continue
        else:
            if docstring_marker in stripped:
                in_docstring = False
            continue
        if stripped.startswith('#'):
            continue
        lines.append(line)
    return '\\n'.join(lines)


clean_code = strip_comments(code)

# Columnas/datos sensibles a vigilar (NO incluye password/token cuando son variables locales)
sensitive_data_columns = [
    'ssn', 'social_security', 'dni', 'ruc',
    'credit_card', 'card_number', 'cvv',
    'email_address', 'phone_number',  # más específicos
]

# Buscar referencias a columnas sensibles en operaciones de DataFrame
# Solo dispara si están en .select(), .filter(), .withColumn(), etc.
found_sensitive = []
df_ops_pattern = r'\\.(select|filter|where|withColumn|drop|groupBy)\\s*\\([^)]*'

for op_match in re.finditer(df_ops_pattern, clean_code, re.IGNORECASE):
    op_content = op_match.group(0).lower()
    for col in sensitive_data_columns:
        if col in op_content and col not in found_sensitive:
            found_sensitive.append(col)

if found_sensitive:
    # Verificar si hay encriptación cerca
    has_encryption = bool(re.search(
        r'(sha2|sha\\(|encrypt|aes_|hash\\()',
        clean_code,
        re.IGNORECASE
    ))
    if not has_encryption:
        cols_str = ', '.join(found_sensitive)
        return f\"FAIL:Columnas sensibles ({cols_str}) sin encriptación\"
    return \"PASS:Encriptación detectada para columnas sensibles\"

return \"INFO:No se detectaron columnas sensibles en operaciones de DataFrame\"
$$
""")

print("✅ check_dac_encryption v1.1 registrada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ check_audit_trail_integration v1.1 — Mejor detección de logging

# COMMAND ----------

# Esta función ya funcionaba relativamente bien, mejoramos solo la detección de logging
spark.sql("""
CREATE OR REPLACE FUNCTION dlco_qa_agents.tools.check_audit_trail_integration(code STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Verifica integración con audit trail (v1.1: mejor detección de logging)'
AS $$
import re

# Patrones de logging y auditoría (ampliados en v1.1)
has_logging = bool(re.search(
    r'(logger\\.|logging\\.|getLogger\\(|log\\.info|log\\.error|log\\.warn)',
    code
))
has_audit_insert = bool(re.search(
    r'(audit_trail|log_execution|insertInto.*audit|audit\\.pr_verdicts|audit\\.agent_findings)',
    code,
    re.IGNORECASE
))

# Detectar operaciones que requieren auditoría
critical_ops = []
if re.search(r'\\.write\\.', code):
    critical_ops.append(\"write\")
if re.search(r'spark\\.sql.*(DELETE|UPDATE|MERGE)', code, re.IGNORECASE):
    critical_ops.append(\"DML\")
if re.search(r'dbutils\\.fs\\.(rm|mv)', code):
    critical_ops.append(\"file_ops\")

if critical_ops and not (has_logging or has_audit_insert):
    return f\"FAIL:Operaciones críticas ({', '.join(critical_ops)}) sin auditoría\"
elif has_logging or has_audit_insert:
    return \"PASS:Audit trail integrado correctamente\"
else:
    return \"INFO:No se detectaron operaciones que requieran auditoría\"
$$
""")

print("✅ check_audit_trail_integration v1.1 registrada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Tests de regresión

# COMMAND ----------

# Test 1: Código BUENO con dbutils.secrets (NO debe disparar nada CRITICAL)
good_code = """
import logging
logger = logging.getLogger(__name__)

def ingest_data():
    try:
        password = dbutils.secrets.get(scope="dlco-secrets", key="db-password")
        token = dbutils.secrets.get(scope="dlco-secrets", key="api-token")
        df = spark.read.format("csv").schema(my_schema).table("landing_data")
        df.write.format("delta").mode("overwrite").partitionBy("date").saveAsTable("bronze_data")
        logger.info("Done")
    except Exception as e:
        logger.error(str(e))
        raise
"""

test_df = spark.createDataFrame([(good_code,)], ["code"])

print("🧪 Test 1: Código BUENO con dbutils.secrets\n")

print("Lint rules:")
result = test_df.selectExpr("dlco_qa_agents.tools.lint_pyspark_rules(code) as r").collect()[0][0]
for f in result:
    print(f"  • {f}")

print("\nScan secrets:")
result = test_df.selectExpr("dlco_qa_agents.tools.scan_hardcoded_secrets(code) as r").collect()[0][0]
for f in result:
    print(f"  • {f}")

print("\nDAC encryption:")
result = test_df.selectExpr("dlco_qa_agents.tools.check_dac_encryption(code) as r").collect()[0][0]
print(f"  • {result}")

print("\nAudit trail:")
result = test_df.selectExpr("dlco_qa_agents.tools.check_audit_trail_integration(code) as r").collect()[0][0]
print(f"  • {result}")

# COMMAND ----------

# Test 2: Código MALO (debe seguir detectando TODO)
bad_code = """
def bad_pipeline():
    print("Starting")
    password = "MyHardcodedPassword123"
    api_key = "sk-abc123def456"
    df = spark.read.csv("/mnt/raw/data.csv")
    all_data = df.collect()
    df.write.mode("overwrite").saveAsTable("output_table")
"""

test_df = spark.createDataFrame([(bad_code,)], ["code"])

print("🧪 Test 2: Código MALO (debe seguir detectando todo)\n")

print("Lint rules:")
result = test_df.selectExpr("dlco_qa_agents.tools.lint_pyspark_rules(code) as r").collect()[0][0]
for f in result:
    print(f"  • {f}")

print("\nScan secrets:")
result = test_df.selectExpr("dlco_qa_agents.tools.scan_hardcoded_secrets(code) as r").collect()[0][0]
for f in result:
    print(f"  • {f}")

# COMMAND ----------

# Test 3: Código con comentarios mencionando antipatrones (NO debe disparar)
commented_code = """
# Este pipeline antes usaba print() pero ahora usa logging
# También migramos de /mnt/raw/ a Unity Catalog
# Las credenciales ya no son password = "..." sino que vienen de secrets

import logging
logger = logging.getLogger(__name__)

def good_pipeline():
    try:
        logger.info("Starting")
        password = dbutils.secrets.get(scope="my-scope", key="db-pass")
        df = spark.table("bronze_data")
        df.write.format("delta").mode("append").saveAsTable("silver_data")
    except Exception as e:
        logger.error(e)
        raise
"""

test_df = spark.createDataFrame([(commented_code,)], ["code"])

print("🧪 Test 3: Comentarios mencionando antipatrones (NO debe disparar)\n")

print("Lint rules:")
result = test_df.selectExpr("dlco_qa_agents.tools.lint_pyspark_rules(code) as r").collect()[0][0]
for f in result:
    print(f"  • {f}")

print("\nScan secrets:")
result = test_df.selectExpr("dlco_qa_agents.tools.scan_hardcoded_secrets(code) as r").collect()[0][0]
for f in result:
    print(f"  • {f}")

# COMMAND ----------

print("""
╔═══════════════════════════════════════════════════════════════╗
║   ✅ UC FUNCTIONS v1.1 DESPLEGADAS                            ║
╚═══════════════════════════════════════════════════════════════╝

Mejoras aplicadas:
  ✅ lint_pyspark_rules: ignora comentarios y docstrings
  ✅ scan_hardcoded_secrets: reconoce dbutils.secrets como legítimo
  ✅ check_dac_encryption: requiere uso en operaciones de DF reales
  ✅ check_audit_trail_integration: mejor detección de logging

Resultados de regresión esperados:
  Test 1 (código BUENO): 0 findings críticos
  Test 2 (código MALO):  Debe seguir detectando todo
  Test 3 (comentarios):  0 findings (false positives eliminados)

Próximo paso:
  • Volver a push en el PR para re-validar
  • El agente ahora debería APROBARLO ✅
""")
