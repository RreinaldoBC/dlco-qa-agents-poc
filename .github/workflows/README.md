# 🤖 GitHub Actions Workflows - DLCO QA Agents

Este directorio contiene los workflows de CI/CD del proyecto DLCO QA Agents.

## 📋 Workflows disponibles

| Workflow | Archivo | Trigger | Acción |
|----------|---------|---------|--------|
| **PR Validation** | `pr_validation.yml` | PR → main | Valida código con agente DLCO. Bloquea merge si CRITICAL. |
| **Deploy DEV** | `deploy_dev.yml` | Manual (workflow_dispatch) | Despliega bundle al target `dev` |
| **Deploy PROD** | `deploy_prod.yml` | Manual o tag `v*.*.*` | Despliega a `prod` (requiere aprobación) |

---

## 🔧 Secrets requeridos

Los workflows requieren los siguientes secrets configurados en  
`Settings → Secrets and variables → Actions`:

| Secret | Valor |
|--------|-------|
| `DATABRICKS_HOST` | `https://<workspace>.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Personal Access Token con scope `workspace` |

---

## 🚀 Cómo usar cada workflow

### 1. PR Validation

**Automático.** Se ejecuta al abrir/actualizar un PR contra `main`.

Comportamiento:
- Detecta archivos `.py` modificados en el PR
- Invoca el job `[DLCO QA] Validación de PR` en Databricks
- Comenta el reporte de findings en el PR
- Falla el check si hay 1+ findings CRITICAL → bloquea merge

Archivos validados:
- `notebooks/**/*.py`
- `src/**/*.py`

Archivos ignorados:
- `tests/**`
- `.github/**`

### 2. Deploy DEV

**Manual.** Desde la UI:

1. Ir a tab **Actions**
2. Seleccionar workflow **DLCO QA - Deploy DEV** en el sidebar
3. Click **Run workflow** (botón gris)
4. Opcional: agregar razón del deploy
5. Click **Run workflow** (botón verde)

Tiempo estimado: 2-3 min

### 3. Deploy PROD

**Manual con aprobación.** Dos formas de disparar:

**Forma A — Desde la UI:**
1. Ir a tab **Actions**
2. Seleccionar workflow **DLCO QA - Deploy PROD**
3. Click **Run workflow**
4. Completar **version** (ej: `v1.0.0`) y **reason**
5. Click **Run workflow**

**Forma B — Desde un tag de release:**
```bash
git tag -a v1.0.0 -m "Release v1.0.0 - PoC inicial"
git push origin v1.0.0
```

**Después del trigger:**
1. El workflow valida el bundle
2. Se pausa esperando aprobación del environment `production`
3. Vas al PR, en la sección Reviews aparece "Review pending deployment"
4. Click **Review deployments** → **Approve** → **Approve and deploy**
5. Se ejecuta el deploy a prod

---

## 🔍 Troubleshooting

### El workflow falla con "Error: cannot find job [DLCO QA] Validación de PR"

**Causa:** El bundle no está desplegado en el workspace.

**Solución:** Ejecuta primero el workflow **Deploy DEV** o desde local:
```bash
databricks bundle deploy --target dev --profile dlco-dev
```

### El workflow falla con "DATABRICKS_TOKEN expired"

**Causa:** El PAT venció (90 días de vida).

**Solución:**
1. Generar nuevo token en Databricks → User Settings → Developer → Access tokens
2. Actualizar el secret en GitHub → Settings → Secrets → Actions

### El deploy a prod se queda en "Waiting for review"

**Causa:** Estás esperando que alguien (tú mismo) apruebe el deployment.

**Solución:** Ve a la pestaña Actions, abre el run actual, busca el botón "Review deployments" arriba.
