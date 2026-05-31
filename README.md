# 🤖 DLCO QA Agents - PoC

> **Proyecto:** Agente Databricks Genie para validación de calidad de código  
> **Cliente:** Grupo Credicorp  
> **Framework:** DataOps Corp v1.1  
> **Autor:** ReinaldoB

---

## 📋 Descripción

Sistema de agentes inteligentes que valida automáticamente código PySpark, SQL y configuraciones YAML antes de que llegue a producción. Se integra como gate de calidad en el CI/CD del framework DataOps Corp de Credicorp.

## 🏗️ Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│                      GitHub (PR)                                │
│         ┌───────────────────────────────────────┐               │
│         │   GitHub Actions Workflow             │               │
│         └─────────────────┬─────────────────────┘               │
└───────────────────────────┼─────────────────────────────────────┘
                            │  invoca job
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Databricks Workspace                         │
│                                                                 │
│   ┌──────────────────────────────────────────────┐              │
│   │   DLCO QA Validation Job                     │              │
│   │   ┌────────────────────────────────────┐     │              │
│   │   │  PySpark Validator Agent           │     │              │
│   │   │  ┌──────────────────────────────┐  │     │              │
│   │   │  │ RAG: Vector Search Index     │  │     │              │
│   │   │  │ Tools: 6 UC Functions        │  │     │              │
│   │   │  │ Scoring: 0-100               │  │     │              │
│   │   │  └──────────────────────────────┘  │     │              │
│   │   └────────────────────────────────────┘     │              │
│   └──────────────────────────────────────────────┘              │
│                                                                 │
│   Unity Catalog: dlco_qa_agents                                 │
│   ├── tools/    (6 UC Functions)                                │
│   ├── silver/   (Golden Dataset)                                │
│   ├── vectors/  (Vector Search Index)                           │
│   └── audit/    (PR verdicts + findings)                        │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
                  ┌──────────────────────┐
                  │ Lakeview Dashboard   │
                  │ (Métricas de calidad)│
                  └──────────────────────┘
```

## 📂 Estructura del Repositorio

```
.
├── .github/workflows/      # CI/CD pipelines
├── .vscode/                # Config IDE compartida
├── notebooks/setup/        # Notebooks de configuración del agente
├── notebooks/ci/           # Notebooks invocados desde CI
├── src/dlco_qa/            # Cliente Python del agente
├── tests/                  # Tests unitarios + fixtures
├── dashboards/             # Lakeview dashboards (JSON)
├── docs/                   # Documentación técnica
├── resources/              # Configs modulares del bundle
├── databricks.yml          # Asset Bundle principal
└── pyproject.toml          # Dependencias Python
```

## 🚀 Quick Start

### Prerequisitos

- Python 3.10+
- VSCode con extensión Databricks
- Databricks CLI v0.205+
- Acceso al workspace `adb-319103249978237.17.azuredatabricks.net`

### Setup local

```bash
# 1. Clonar repo
git clone https://github.com/<org>/dlco_qa_agents_poc.git
cd dlco_qa_agents_poc

# 2. Crear venv y dependencias
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # Linux/Mac
pip install -r requirements-dev.txt

# 3. Autenticación Databricks
databricks auth login --host https://adb-319103249978237.17.azuredatabricks.net

# 4. Validar el bundle
databricks bundle validate

# 5. Deploy a dev
databricks bundle deploy --target dev
```

### Ejecutar la PoC end-to-end

```bash
# Ejecutar el job de setup completo (one-time)
databricks bundle run dlco_qa_setup_job --target dev

# Probar validación de un PR
databricks bundle run dlco_qa_validation_job --target dev \
  --params pr_id=TEST-001,pr_url=https://github.com/test/test/pull/1
```

## 🔄 Flujo CI/CD

| Trigger | Workflow | Acción |
|---------|----------|--------|
| `pull_request` opened/sync | `pr_validation.yml` | Ejecuta agente, comenta findings, bloquea si CRITICAL |
| `push` a `develop` | `deploy_dev.yml` | Deploy automático a target `dev` |
| `release` published | `deploy_prod.yml` | Deploy manual a target `prod` |

## 📊 Dashboard

El dashboard de métricas está en:  
**Databricks → Dashboards → "DLCO QA Agent Metrics"**

Métricas trackeadas:
- Total de PRs validados (día/semana/mes)
- Tasa de aprobación vs rechazo
- Findings por severidad y por regla
- Tiempo promedio de validación
- Top reglas más violadas

## 📚 Documentación

- [Arquitectura detallada](docs/ARCHITECTURE.md)
- [Guía CI/CD](docs/CICD_GUIDE.md)
- [Catálogo de reglas DLCO](docs/DLCO_RULES.md)

## 👤 Contacto

- **Owner:** ReinaldoB
- **Equipo:** División Data Corporativo - Credicorp
- **Slack:** #dlco-qa-agents
