# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 CI Validation Notebook - DLCO QA Agent
# MAGIC
# MAGIC **Proyecto:** Agente Databricks Genie para DataOps Corp v1.1
# MAGIC **Cliente:** Grupo Credicorp
# MAGIC **Autor:** ReinaldoB
# MAGIC **Versión:** 1.0 — Mayo 2026
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Propósito
# MAGIC
# MAGIC Notebook invocado por **GitHub Actions** cuando se abre/actualiza un Pull Request.
# MAGIC Ejecuta el agente PySpark Validator sobre los archivos modificados en el PR y
# MAGIC retorna un veredicto (`approved` / `rejected`) con los findings detallados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Imports y configuración inicial

# COMMAND ----------

import json
import time
import uuid
from datetime import datetime

# Usar el catálogo
spark.sql("USE CATALOG dlco_qa_agents")

print("✅ Configuración inicial completa")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 Recibir parámetros del job

# COMMAND ----------

dbutils.widgets.text("pr_id", "", "PR ID")
dbutils.widgets.text("pr_url", "", "PR URL")
dbutils.widgets.text("repo_name", "", "Repo Name")
dbutils.widgets.text("branch_name", "", "Branch Name")
dbutils.widgets.text("files_to_validate", "[]", "Files to Validate (JSON)")

pr_id = dbutils.widgets.get("pr_id") or "MANUAL-RUN"
pr_url = dbutils.widgets.get("pr_url") or ""
repo_name = dbutils.widgets.get("repo_name") or ""
branch_name = dbutils.widgets.get("branch_name") or ""
files_to_validate_json = dbutils.widgets.get("files_to_validate") or "[]"

try:
    files_to_validate = json.loads(files_to_validate_json)
except json.JSONDecodeError as e:
    print(f"⚠️ Error parseando files_to_validate JSON: {e}")
    files_to_validate = []

print("=" * 70)
print("🔍 DLCO QA Agent — CI Validation")
print("=" * 70)
print(f"PR ID:           {pr_id}")
print(f"PR URL:          {pr_url}")
print(f"Repo:            {repo_name}")
print(f"Branch:          {branch_name}")
print(f"Files received:  {len(files_to_validate)}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 Clase del Agente (versión CI optimizada)

# COMMAND ----------

class CIPySparkValidatorAgent:
    """Agente de validación de código PySpark optimizado para CI/CD."""

    CATALOG = "dlco_qa_agents"

    def _escape_sql(self, text: str) -> str:
        return text.replace("\\", "\\\\").replace("'", "''")

    def _run_uc_function(self, function_name: str, code: str) -> str:
        try:
            escaped = self._escape_sql(code)
            result = spark.sql(
                f"SELECT {self.CATALOG}.tools.{function_name}('{escaped}') as result"
            ).collect()[0]["result"]
            if isinstance(result, list):
                return " | ".join(str(x) for x in result)
            return str(result) if result else ""
        except Exception as e:
            print(f"⚠️ Error en UC Function {function_name}: {e}")
            return f"ERROR:{e}"

    def run_uc_validations(self, code: str) -> dict:
        return {
            "lint_rules": self._run_uc_function("lint_pyspark_rules", code),
            "naming": self._run_uc_function("validate_naming_convention", code),
            "audit": self._run_uc_function("check_audit_trail_integration", code),
            "dac": self._run_uc_function("check_dac_encryption", code),
            "secrets": self._run_uc_function("scan_hardcoded_secrets", code),
        }

    def parse_findings(self, uc_results: dict, file_path: str) -> list:
        findings = []

        # Lint rules
        lint = uc_results.get("lint_rules", "")
        if lint and "PASS" not in lint:
            for part in lint.split("|"):
                part = part.strip()
                if "DLCO-" in part:
                    pieces = part.split(":")
                    if len(pieces) >= 3:
                        findings.append({
                            "file": file_path,
                            "rule": pieces[0].strip(),
                            "severity": pieces[1].strip(),
                            "message": ":".join(pieces[2:]).strip(),
                        })

        # Naming
        naming = uc_results.get("naming", "")
        if naming and "PASS" not in naming and "INFO" not in naming:
            for part in naming.split("|"):
                if "NAMING:" in part:
                    pieces = part.split(":")
                    if len(pieces) >= 3:
                        findings.append({
                            "file": file_path,
                            "rule": pieces[0].strip(),
                            "severity": pieces[1].strip(),
                            "message": ":".join(pieces[2:]).strip(),
                        })

        # Audit
        audit = uc_results.get("audit", "")
        if "FAIL" in audit:
            findings.append({
                "file": file_path,
                "rule": "AUDIT",
                "severity": "MEDIUM",
                "message": audit.replace("FAIL:", "").strip(),
            })

        # DAC
        dac = uc_results.get("dac", "")
        if "FAIL" in dac:
            findings.append({
                "file": file_path,
                "rule": "DAC",
                "severity": "HIGH",
                "message": dac.replace("FAIL:", "").strip(),
            })

        # Secrets
        secrets = uc_results.get("secrets", "")
        if secrets and "PASS" not in secrets:
            for part in secrets.split("|"):
                if "SECRET:" in part:
                    pieces = part.split(":")
                    if len(pieces) >= 3:
                        findings.append({
                            "file": file_path,
                            "rule": f"SECRET-{pieces[1].strip()}",
                            "severity": "CRITICAL",
                            "message": ":".join(pieces[2:]).strip(),
                        })

        return findings

    def calculate_quality_score(self, findings: list) -> int:
        score = 100
        weights = {"CRITICAL": 40, "HIGH": 20, "MEDIUM": 10, "LOW": 5}
        for f in findings:
            score -= weights.get(f.get("severity", "LOW"), 5)
        return max(0, score)

    def validate_file(self, file_path: str, content: str) -> dict:
        print(f"\n📄 Validando: {file_path}")
        start = time.time()

        if not file_path.endswith(".py"):
            print(f"   ⏭️ Skip (no es .py)")
            return {"file": file_path, "skipped": True, "findings": []}

        uc_results = self.run_uc_validations(content)
        findings = self.parse_findings(uc_results, file_path)
        score = self.calculate_quality_score(findings)

        elapsed = time.time() - start
        print(f"   ⏱️ {elapsed:.1f}s | Score: {score}/100 | Findings: {len(findings)}")

        return {
            "file": file_path,
            "skipped": False,
            "quality_score": score,
            "findings": findings,
            "elapsed_seconds": round(elapsed, 2),
        }


print("✅ Clase CIPySparkValidatorAgent definida")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Ejecutar validación

# COMMAND ----------

start_global = time.time()

agent = CIPySparkValidatorAgent()

file_results = []
all_findings = []

if not files_to_validate:
    print("⚠️ No hay archivos para validar")
else:
    for file_data in files_to_validate:
        path = file_data.get("path", "")
        content = file_data.get("content", "")
        if not content:
            continue
        result = agent.validate_file(path, content)
        file_results.append(result)
        if not result.get("skipped"):
            all_findings.extend(result.get("findings", []))

total_elapsed = time.time() - start_global

# Contar findings por severidad
severity_counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
for f in all_findings:
    sev = f.get("severity", "LOW")
    if sev in severity_counts:
        severity_counts[sev] += 1

# Calcular score global (promedio de archivos validados)
validated_files = [r for r in file_results if not r.get("skipped")]
if validated_files:
    global_score = sum(r["quality_score"] for r in validated_files) // len(validated_files)
else:
    global_score = 100

# Veredicto: rechazo automático si hay CRITICAL
verdict = "rejected" if severity_counts["CRITICAL"] > 0 else "approved"

print("\n" + "=" * 70)
print("📊 RESUMEN DE VALIDACIÓN")
print("=" * 70)
print(f"Archivos procesados:  {len(file_results)}")
print(f"Archivos validados:   {len(validated_files)}")
print(f"Quality Score global: {global_score}/100")
print(f"Total findings:       {len(all_findings)}")
print(f"  🔴 CRITICAL: {severity_counts['CRITICAL']}")
print(f"  🟠 HIGH:     {severity_counts['HIGH']}")
print(f"  🟡 MEDIUM:   {severity_counts['MEDIUM']}")
print(f"  🟢 LOW:      {severity_counts['LOW']}")
print(f"Veredicto:            {verdict.upper()}")
print(f"Tiempo total:         {total_elapsed:.1f}s")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Registrar en audit trail

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)

run_id = f"PR-{pr_id}-{uuid.uuid4().hex[:8]}"

# Registrar veredicto del PR
verdict_schema = StructType([
    StructField("pr_id", StringType(), True),
    StructField("pr_url", StringType(), True),
    StructField("pr_verdict", StringType(), True),
    StructField("findings_count", IntegerType(), True),
    StructField("critical_count", IntegerType(), True),
    StructField("high_count", IntegerType(), True),
    StructField("medium_count", IntegerType(), True),
    StructField("low_count", IntegerType(), True),
    StructField("execution_time_seconds", DoubleType(), True),
    StructField("agent_version", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("created_by", StringType(), True),
])

verdict_row = [(
    run_id, pr_url, verdict, len(all_findings),
    severity_counts["CRITICAL"], severity_counts["HIGH"],
    severity_counts["MEDIUM"], severity_counts["LOW"],
    float(total_elapsed), "v1.0",
    datetime.now(), "dlco_qa_ci_agent",
)]

df_verdict = spark.createDataFrame(verdict_row, verdict_schema)
df_verdict.write.format("delta").mode("append").saveAsTable(
    "dlco_qa_agents.audit.pr_verdicts"
)
print(f"✅ Veredicto registrado: {run_id}")

# Registrar findings individuales
if all_findings:
    findings_schema = StructType([
        StructField("finding_id", StringType(), True),
        StructField("pr_id", StringType(), True),
        StructField("rule_id", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("message", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("line_number", IntegerType(), True),
        StructField("code_snippet", StringType(), True),
        StructField("agent_name", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ])

    findings_rows = [
        (
            f"FIND-{uuid.uuid4().hex[:8]}",
            run_id,
            f.get("rule", ""),
            f.get("severity", "LOW"),
            f.get("message", ""),
            f.get("file", ""),
            None, None,
            "ci_pyspark_validator",
            datetime.now(),
        )
        for f in all_findings
    ]

    df_findings = spark.createDataFrame(findings_rows, findings_schema)
    df_findings.write.format("delta").mode("append").saveAsTable(
        "dlco_qa_agents.audit.agent_findings"
    )
    print(f"✅ {len(all_findings)} findings registrados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📤 Retornar resultado al job

# COMMAND ----------

result = {
    "verdict": verdict,
    "quality_score": global_score,
    "total_findings": len(all_findings),
    "files_validated": len(validated_files),
    "files_skipped": len(file_results) - len(validated_files),
    "severity_counts": severity_counts,
    "findings": all_findings,
    "run_id": run_id,
    "agent_version": "v1.0",
    "execution_time_seconds": round(total_elapsed, 2),
    "timestamp": datetime.now().isoformat(),
    "summary": (
        f"❌ PR RECHAZADO: {severity_counts['CRITICAL']} finding(s) CRITICAL detectado(s)"
        if verdict == "rejected"
        else f"✅ PR APROBADO con score {global_score}/100 "
             f"({len(all_findings)} findings menores)"
    ),
}

print("\n" + "=" * 70)
print("📤 RESULTADO FINAL")
print("=" * 70)
print(json.dumps(result, indent=2, ensure_ascii=False))
print("=" * 70)

# COMMAND ----------

# dbutils.notebook.exit retorna el JSON que GitHub Actions consume
dbutils.notebook.exit(json.dumps(result, ensure_ascii=False))