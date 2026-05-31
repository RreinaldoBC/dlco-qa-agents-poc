# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 CI Validation Notebook - DLCO QA Agent
# MAGIC
# MAGIC **Proyecto:** Agente Databricks Genie para DataOps Corp v1.1
# MAGIC **Cliente:** Grupo Credicorp
# MAGIC **Autor:** ReinaldoB
# MAGIC **Versión:** 0.1 (STUB) — Mayo 2026
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Propósito
# MAGIC
# MAGIC Notebook invocado por **GitHub Actions** cuando se abre/actualiza un Pull Request.
# MAGIC Ejecuta el agente PySpark Validator sobre los archivos modificados en el PR y
# MAGIC retorna un veredicto (`approved` / `rejected`) con los findings detallados.
# MAGIC
# MAGIC ## 📥 Parámetros (recibidos del job)
# MAGIC
# MAGIC | Parámetro | Descripción | Ejemplo |
# MAGIC |-----------|-------------|---------|
# MAGIC | `pr_id` | ID del PR en GitHub | `PR-123` |
# MAGIC | `pr_url` | URL completa del PR | `https://github.com/credicorp/repo/pull/123` |
# MAGIC | `repo_name` | Nombre del repositorio | `credicorp/data-pipelines` |
# MAGIC | `branch_name` | Branch del PR | `feature/new-pipeline` |
# MAGIC | `files_to_validate` | JSON con archivos a validar | `[{"path":"...", "content":"..."}]` |
# MAGIC
# MAGIC ## 📤 Output
# MAGIC
# MAGIC - Registro en `dlco_qa_agents.audit.pr_verdicts`
# MAGIC - Findings detallados en `dlco_qa_agents.audit.agent_findings`
# MAGIC - Notebook output JSON consumido por GitHub Actions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > ⚠️ **STATUS: STUB**
# MAGIC > Este notebook se completará en el **Paso 9** del plan de fase 2.
# MAGIC > La lógica de integración con GitHub Actions y el wrapper del agente
# MAGIC > se agregará junto con `src/dlco_qa/ci_validator.py`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 Recibir parámetros del job

# COMMAND ----------

# Definir widgets para recibir parámetros del job
dbutils.widgets.text("pr_id", "", "PR ID")
dbutils.widgets.text("pr_url", "", "PR URL")
dbutils.widgets.text("repo_name", "", "Repo Name")
dbutils.widgets.text("branch_name", "", "Branch Name")
dbutils.widgets.text("files_to_validate", "[]", "Files to Validate (JSON)")

# Leer parámetros
pr_id = dbutils.widgets.get("pr_id")
pr_url = dbutils.widgets.get("pr_url")
repo_name = dbutils.widgets.get("repo_name")
branch_name = dbutils.widgets.get("branch_name")
files_to_validate = dbutils.widgets.get("files_to_validate")

print("=" * 70)
print("🔍 DLCO QA Agent — CI Validation")
print("=" * 70)
print(f"PR ID:      {pr_id}")
print(f"PR URL:     {pr_url}")
print(f"Repo:       {repo_name}")
print(f"Branch:     {branch_name}")
print(f"Files JSON: {files_to_validate[:100]}...")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚧 Lógica de validación (pendiente — Paso 9)

# COMMAND ----------

import json
from datetime import datetime

# TODO (Paso 9):
#   1. Parsear files_to_validate (JSON array de {path, content})
#   2. Para cada archivo .py:
#      - Instanciar PySparValidatorAgent
#      - Llamar agent.validate_code(content)
#      - Acumular findings
#   3. Calcular veredicto global:
#      - rejected si hay algún finding CRITICAL o score < 70
#      - approved en otro caso
#   4. Insertar en audit.pr_verdicts y audit.agent_findings
#   5. Retornar JSON con resultado para GitHub Actions

# === STUB: respuesta dummy para validar el job en este paso ===
result = {
    "pr_id": pr_id,
    "verdict": "stub_pending_implementation",
    "quality_score": 0,
    "total_findings": 0,
    "findings": [],
    "agent_version": "v0.1-stub",
    "timestamp": datetime.now().isoformat(),
    "message": (
        "⚠️ Notebook stub. Lógica completa pendiente del Paso 9 "
        "(integración con GitHub Actions)."
    ),
}

print(json.dumps(result, indent=2, ensure_ascii=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📤 Retornar resultado al job

# COMMAND ----------

# dbutils.notebook.exit permite retornar un valor que GitHub Actions puede consumir
dbutils.notebook.exit(json.dumps(result, ensure_ascii=False))
