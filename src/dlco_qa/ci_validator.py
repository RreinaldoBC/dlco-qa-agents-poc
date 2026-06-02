"""
DLCO QA Agent - CI Validator
============================

Wrapper invocado por GitHub Actions para ejecutar el agente PySpark Validator
de Databricks sobre los archivos modificados en un Pull Request.

Flujo:
  1. Lee los archivos modificados (paths) del PR.
  2. Carga el contenido de cada archivo.
  3. Invoca el Databricks Job `dlco_qa_validation_job` pasando los archivos
     como parámetro JSON.
  4. Espera el resultado (polling).
  5. Parsea el JSON retornado por dbutils.notebook.exit y lo guarda en
     un archivo local para que GitHub Actions lo lea.

Uso (desde GitHub Actions):
  python src/dlco_qa/ci_validator.py \\
    --pr-id 123 \\
    --pr-url https://github.com/RreinaldoBC/dlco-qa-agents-poc/pull/123 \\
    --repo RreinaldoBC/dlco-qa-agents-poc \\
    --branch feature/new-pipeline \\
    --files notebooks/ingestion.py,notebooks/transform.py \\
    --output-file validation_result.json

Variables de entorno requeridas:
  DATABRICKS_HOST   - URL del workspace
  DATABRICKS_TOKEN  - Personal Access Token
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunResultState


# ============================================================================
# Configuración
# ============================================================================

JOB_NAME_PATTERN = "[DLCO QA] Validación de PR"
POLL_INTERVAL_SECONDS = 15
MAX_WAIT_SECONDS = 1500  # 25 min


# ============================================================================
# Funciones auxiliares
# ============================================================================


def log(msg: str, level: str = "INFO") -> None:
    """Logger simple con timestamp."""
    timestamp = time.strftime("%H:%M:%S")
    icon = {"INFO": "ℹ️", "OK": "✅", "WARN": "⚠️", "ERROR": "❌"}.get(level, "•")
    print(f"[{timestamp}] {icon} {msg}", flush=True)


def load_files_content(file_paths: list[str]) -> list[dict]:
    """
    Carga el contenido de los archivos modificados.

    Returns:
        Lista de dicts: [{"path": str, "content": str, "size": int}]
    """
    files_data = []
    for path_str in file_paths:
        path = Path(path_str.strip())
        if not path.exists():
            log(f"Archivo no encontrado, skip: {path}", level="WARN")
            continue
        try:
            content = path.read_text(encoding="utf-8")
            files_data.append({
                "path": str(path),
                "content": content,
                "size": len(content),
            })
            log(f"Cargado: {path} ({len(content)} bytes)")
        except Exception as e:
            log(f"Error leyendo {path}: {e}", level="ERROR")
    return files_data


def find_validation_job(w: WorkspaceClient) -> int:
    """
    Busca el job de validación por nombre (puede tener prefijo del bundle dev).

    Returns:
        job_id del primer match.
    """
    log(f"Buscando job que contenga: '{JOB_NAME_PATTERN}'")
    jobs = list(w.jobs.list())
    matches = [j for j in jobs if j.settings and JOB_NAME_PATTERN in (j.settings.name or "")]

    if not matches:
        raise RuntimeError(
            f"No se encontró ningún job con '{JOB_NAME_PATTERN}' en el nombre. "
            f"¿Está desplegado el bundle? Ejecuta: databricks bundle deploy --target dev"
        )

    # Si hay múltiples, preferir uno con "prod" en el nombre; si no, el primero
    prod_match = next((j for j in matches if "prod" in (j.settings.name or "").lower()), None)
    selected = prod_match or matches[0]

    log(f"Job seleccionado: {selected.settings.name} (id={selected.job_id})", level="OK")
    return selected.job_id


def trigger_validation_job(
    w: WorkspaceClient,
    job_id: int,
    pr_id: str,
    pr_url: str,
    repo: str,
    branch: str,
    files_data: list[dict],
) -> int:
    """
    Dispara el job de validación pasando los archivos a validar como parámetro.

    Returns:
        run_id del job triggereado.
    """
    log(f"Triggerando job {job_id} para PR #{pr_id}...")

    job_params = {
        "pr_id": str(pr_id),
        "pr_url": pr_url,
        "repo_name": repo,
        "branch_name": branch,
        "files_to_validate": json.dumps(files_data, ensure_ascii=False),
    }

    run = w.jobs.run_now(job_id=job_id, job_parameters=job_params)
    log(f"Run iniciado: run_id={run.run_id}", level="OK")
    return run.run_id


def wait_for_run(w: WorkspaceClient, run_id: int) -> dict:
    """
    Hace polling del estado del run hasta que termine.

    Returns:
        El resultado parseado del notebook (JSON de dbutils.notebook.exit).
    """
    log(f"Esperando resultado del run {run_id}...")
    elapsed = 0

    while elapsed < MAX_WAIT_SECONDS:
        run = w.jobs.get_run(run_id=run_id)
        state = run.state

        if state and state.life_cycle_state:
            lcs = state.life_cycle_state.value
            log(f"Estado: {lcs} (esperado: {elapsed}s)")

            if lcs in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
                result_state = state.result_state.value if state.result_state else "UNKNOWN"
                log(f"Run finalizado. Result state: {result_state}")

                if result_state != "SUCCESS":
                    msg = state.state_message or "Sin mensaje"
                    raise RuntimeError(
                        f"El job de validación falló con state={result_state}. "
                        f"Mensaje: {msg}\nRun URL: {run.run_page_url}"
                    )

                # Extraer el output del notebook
                if run.tasks:
                    task_run_id = run.tasks[0].run_id
                    task_output = w.jobs.get_run_output(run_id=task_run_id)
                    notebook_output = task_output.notebook_output
                    if notebook_output and notebook_output.result:
                        return json.loads(notebook_output.result)

                raise RuntimeError("Job completó pero no retornó output JSON")

        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS

    raise TimeoutError(f"Timeout esperando run {run_id} (>{MAX_WAIT_SECONDS}s)")


# ============================================================================
# Main
# ============================================================================


def main() -> int:
    parser = argparse.ArgumentParser(description="DLCO QA Agent - CI Validator")
    parser.add_argument("--pr-id", required=True, help="ID del Pull Request")
    parser.add_argument("--pr-url", required=True, help="URL del PR")
    parser.add_argument("--repo", required=True, help="Nombre del repositorio")
    parser.add_argument("--branch", required=True, help="Branch del PR")
    parser.add_argument("--files", required=True, help="CSV de paths a validar")
    parser.add_argument(
        "--output-file",
        default="validation_result.json",
        help="Ruta del archivo de salida JSON",
    )
    args = parser.parse_args()

    # Validar variables de entorno
    if not os.environ.get("DATABRICKS_HOST") or not os.environ.get("DATABRICKS_TOKEN"):
        log("Faltan DATABRICKS_HOST o DATABRICKS_TOKEN en el entorno", level="ERROR")
        return 1

    log("=" * 60)
    log("DLCO QA Agent - CI Validator iniciado")
    log("=" * 60)
    log(f"PR:     #{args.pr_id} ({args.pr_url})")
    log(f"Repo:   {args.repo}")
    log(f"Branch: {args.branch}")

    # Cargar archivos
    file_paths = [p for p in args.files.split(",") if p.strip()]
    log(f"Files:  {len(file_paths)} archivos a procesar")

    files_data = load_files_content(file_paths)
    if not files_data:
        log("No hay archivos válidos para validar. Marcando como skip.", level="WARN")
        result = {
            "verdict": "skipped",
            "quality_score": 100,
            "total_findings": 0,
            "findings": [],
            "message": "No hay archivos .py válidos para validar",
        }
        Path(args.output_file).write_text(json.dumps(result, indent=2), encoding="utf-8")
        return 0

    # Conectar con Databricks
    try:
        w = WorkspaceClient()
        log(f"Conectado a Databricks: {w.config.host}", level="OK")
    except Exception as e:
        log(f"Error conectando a Databricks: {e}", level="ERROR")
        return 1

    # Triggerear el job
    try:
        job_id = find_validation_job(w)
        run_id = trigger_validation_job(
            w, job_id, args.pr_id, args.pr_url, args.repo, args.branch, files_data
        )
        result = wait_for_run(w, run_id)
    except Exception as e:
        log(f"Error ejecutando agente: {e}", level="ERROR")
        # Aún así guardamos un resultado de error para que GitHub Actions pueda reportarlo
        result = {
            "verdict": "error",
            "quality_score": 0,
            "total_findings": 0,
            "findings": [],
            "error": str(e),
        }
        Path(args.output_file).write_text(json.dumps(result, indent=2), encoding="utf-8")
        return 1

    # Guardar resultado
    Path(args.output_file).write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    log(f"Resultado guardado en: {args.output_file}", level="OK")

    # Resumen
    log("=" * 60)
    log(f"Veredicto: {result.get('verdict', 'unknown').upper()}")
    log(f"Quality Score: {result.get('quality_score', 0)}/100")
    log(f"Total findings: {result.get('total_findings', 0)}")
    log("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
