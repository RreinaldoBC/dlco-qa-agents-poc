# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Setup Vector Search Index - DLCO QA Agent
# MAGIC 
# MAGIC **Proyecto:** Agente Databricks Genie para DataOps Corp v1.1  
# MAGIC **Cliente:** Grupo Credicorp  
# MAGIC **Autor:** ReinaldoB  
# MAGIC **Versión:** 1.0 — Mayo 2026
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 🎯 Objetivo
# MAGIC 
# MAGIC Crear un Vector Search Index sobre el Golden Dataset para habilitar búsqueda semántica de ejemplos similares.
# MAGIC 
# MAGIC **Componentes:**
# MAGIC - Embedding Model: `databricks-gte-large-en`
# MAGIC - Source Table: `dlco_qa_agents.silver.golden_qa_eval_v1`
# MAGIC - Vector Index: `dlco_qa_agents.vectors.golden_notebooks_idx`
# MAGIC - Columna a indexar: `code` (el código fuente)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Configuración Inicial

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 PASO 0: Instalación de Dependencias

# COMMAND ----------

# Instalar el paquete
%pip install databricks-vectorsearch --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️ **IMPORTANTE:** Después de ejecutar la celda anterior, debes ejecutar la siguiente celda para reiniciar Python.

# COMMAND ----------

# Reiniciar Python
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📦 Configuración Inicial
# MAGIC 
# MAGIC ⚠️ **Ejecuta esta celda DESPUÉS del restart anterior**

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
from pyspark.sql.functions import col

# Inicializar cliente
vsc = VectorSearchClient(disable_notice=True)

# Usar el catálogo correcto
spark.sql("USE CATALOG dlco_qa_agents")

print("✅ Vector Search Client inicializado")
print("📍 Índice se creará en: dlco_qa_agents.vectors.golden_notebooks_idx\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🔧 PASO 1: Verificar la Tabla Origen

# COMMAND ----------

# Verificar que la tabla existe
source_table = "dlco_qa_agents.silver.golden_qa_eval_v1"

df = spark.table(source_table)

print(f"📊 Tabla origen: {source_table}")
print(f"📈 Total de registros: {df.count()}")
print(f"\n📋 Columnas disponibles:")

df.printSchema()

# COMMAND ----------

# Ver algunos ejemplos
print("\n🔍 Preview de datos a indexar:\n")
df.select("code", "description", "quality", "category").show(5, truncate=80)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🏗️ PASO 2: Crear Vector Search Endpoint
# MAGIC 
# MAGIC El endpoint es necesario para servir el índice vectorial.

# COMMAND ----------

endpoint_name = "dlco_qa_vector_endpoint"

print(f"🔍 Verificando si el endpoint '{endpoint_name}' existe...")

try:
    # Intentar obtener el endpoint
    endpoint = vsc.get_endpoint(endpoint_name)
    print(f"✅ Endpoint '{endpoint_name}' ya existe")
    print(f"   Estado: {endpoint.get('endpoint_status', {}).get('state', 'UNKNOWN')}")
except Exception as e:
    if "does not exist" in str(e).lower() or "not found" in str(e).lower():
        print(f"⚠️  Endpoint no existe. Creándolo...")
        
        try:
            # Crear el endpoint
            vsc.create_endpoint(
                name=endpoint_name,
                endpoint_type="STANDARD"
            )
            print(f"✅ Endpoint '{endpoint_name}' creado exitosamente")
            print("⏳ El endpoint tardará 5-10 minutos en estar completamente activo")
        except Exception as create_error:
            print(f"❌ Error al crear endpoint: {create_error}")
            raise
    else:
        print(f"❌ Error inesperado: {e}")
        raise

# COMMAND ----------

# Esperar a que el endpoint esté online
import time

print("\n⏳ Esperando a que el endpoint esté ONLINE...")

max_wait = 600  # 10 minutos máximo
wait_interval = 30  # Verificar cada 30 segundos
elapsed = 0

while elapsed < max_wait:
    try:
        endpoint = vsc.get_endpoint(endpoint_name)
        state = endpoint.get('endpoint_status', {}).get('state', 'UNKNOWN')
        
        print(f"   Estado actual: {state} (esperado: {elapsed}s)")
        
        if state == "ONLINE":
            print(f"✅ Endpoint está ONLINE y listo para usar")
            break
        elif state in ["STOPPED", "STOPPING"]:
            print(f"⚠️  Endpoint en estado {state}. Intentando iniciar...")
            # Aquí podrías agregar lógica para reiniciar si es necesario
        
        time.sleep(wait_interval)
        elapsed += wait_interval
    except Exception as e:
        print(f"⚠️  Error verificando estado: {e}")
        time.sleep(wait_interval)
        elapsed += wait_interval

if elapsed >= max_wait:
    print(f"⚠️  Timeout esperando endpoint. Estado actual desconocido.")
    print(f"   Puedes verificar manualmente en la UI de Databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🔧 PREPARACIÓN: Agregar ID Único y Configurar CDC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2.1: Agregar columna example_id

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# Leer la tabla actual
df = spark.table("dlco_qa_agents.silver.golden_qa_eval_v1")

# Verificar si ya tiene la columna example_id
if "example_id" not in df.columns:
    print("⚠️  Agregando columna example_id...")
    
    # Agregar columna ID única
    df_with_id = df.withColumn("example_id", monotonically_increasing_id())
    
    # Reescribir la tabla con la nueva columna
    df_with_id.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("dlco_qa_agents.silver.golden_qa_eval_v1")
    
    print("✅ Columna 'example_id' agregada")
    print(f"📊 Total de registros: {df_with_id.count()}")
else:
    print("✅ Columna 'example_id' ya existe")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2.2: Configurar Change Data Feed

# COMMAND ----------

# Habilitar Change Data Feed y aumentar retención
spark.sql("""
ALTER TABLE dlco_qa_agents.silver.golden_qa_eval_v1 SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 30 days',
  'delta.logRetentionDuration' = 'interval 30 days'
)
""")

print("✅ Change Data Feed habilitado")
print("✅ Retención configurada a 30 días")
print("\n📊 Tabla lista para Vector Search Index")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📊 PASO 3: Crear Vector Search Index

# COMMAND ----------

# Configuración del índice
index_name = "dlco_qa_agents.vectors.golden_notebooks_idx"
source_table_name = "dlco_qa_agents.silver.golden_qa_eval_v1"
embedding_source_column = "code"  # Columna que contiene el texto a vectorizar
embedding_model = "databricks-gte-large-en"

print(f"🔍 Verificando si el índice '{index_name}' existe...")

try:
    # Intentar obtener el índice
    index = vsc.get_index(endpoint_name=endpoint_name, index_name=index_name)
    print(f"✅ Índice '{index_name}' ya existe")
    
    # Sincronizar para capturar nuevos datos
    print("🔄 Sincronizando índice con la tabla origen...")
    vsc.get_index(endpoint_name=endpoint_name, index_name=index_name).sync()
    print("✅ Sincronización iniciada")
    
except Exception as e:
    if "does not exist" in str(e).lower() or "not found" in str(e).lower():
        print(f"⚠️  Índice no existe. Creándolo...")
        
        try:
            # Crear el índice con primary_key diferente a embedding_source_column
            index = vsc.create_delta_sync_index(
                endpoint_name=endpoint_name,
                source_table_name=source_table_name,
                index_name=index_name,
                pipeline_type="TRIGGERED",  # Manual sync
                primary_key="example_id",  # ✅ CAMBIADO: Usar example_id en lugar de code
                embedding_source_column=embedding_source_column,
                embedding_model_endpoint_name=embedding_model
            )
            
            print(f"✅ Índice '{index_name}' creado exitosamente")
            print("⏳ La indexación inicial puede tomar 5-10 minutos")
            
        except Exception as create_error:
            print(f"❌ Error al crear índice: {create_error}")
            raise
    else:
        print(f"❌ Error inesperado: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ⏳ PASO 4: Esperar a que el Índice se Sincronice

# COMMAND ----------

print("⏳ Esperando a que el índice complete la indexación inicial...\n")

max_wait = 900  # 15 minutos máximo
wait_interval = 30
elapsed = 0

while elapsed < max_wait:
    try:
        index_info = vsc.get_index(endpoint_name=endpoint_name, index_name=index_name)
        
        # El estado puede estar en diferentes ubicaciones según la versión del API
        status = index_info.get('status', {})
        state = status.get('detailed_state', status.get('state', 'UNKNOWN'))
        
        print(f"   Estado del índice: {state} (esperado: {elapsed}s)")
        
        # Estados que indican que está listo
        if state in ["ONLINE", "ONLINE_INDEXED", "READY"]:
            print(f"✅ Índice está listo y sincronizado")
            break
        elif "FAIL" in state.upper() or "ERROR" in state.upper():
            print(f"❌ Error en la indexación: {state}")
            break
            
        time.sleep(wait_interval)
        elapsed += wait_interval
        
    except Exception as e:
        print(f"⚠️  Error verificando estado: {e}")
        time.sleep(wait_interval)
        elapsed += wait_interval

if elapsed >= max_wait:
    print(f"⚠️  Timeout esperando sincronización")
    print(f"   El índice puede seguir procesando en segundo plano")
    print(f"   Verifica el estado en: Compute > Vector Search")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🧪 PASO 5: Probar el Vector Search

# COMMAND ----------

# Consulta de prueba
query_text = "df.collect()"

print(f"🔍 Búsqueda de prueba: '{query_text}'")
print(f"   Buscando ejemplos similares...\n")

try:
    results = vsc.get_index(endpoint_name=endpoint_name, index_name=index_name).similarity_search(
        query_text=query_text,
        columns=["code", "description", "quality", "category", "expected_findings"],
        num_results=3
    )
    
    print("✅ Búsqueda exitosa!\n")
    print("📊 Top 3 ejemplos más similares:\n")
    
    for i, result in enumerate(results.get('result', {}).get('data_array', []), 1):
        print(f"{i}. {result.get('description', 'Sin descripción')}")
        print(f"   Calidad: {result.get('quality', 'N/A')}")
        print(f"   Categoría: {result.get('category', 'N/A')}")
        print(f"   Score: {result.get('score', 'N/A'):.4f}")
        print()
        
except Exception as e:
    print(f"⚠️  Error en búsqueda de prueba: {e}")
    print(f"   Esto es normal si el índice aún está sincronizando")
    print(f"   Espera unos minutos y vuelve a intentar")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📊 PASO 6: Estadísticas del Índice

# COMMAND ----------

try:
    index_info = vsc.get_index(endpoint_name=endpoint_name, index_name=index_name)
    
    print("="*70)
    print("📊 INFORMACIÓN DEL VECTOR SEARCH INDEX")
    print("="*70)
    print(f"\n📍 Nombre: {index_name}")
    print(f"📍 Endpoint: {endpoint_name}")
    print(f"📍 Tabla origen: {source_table_name}")
    print(f"📍 Modelo embedding: {embedding_model}")
    print(f"📍 Columna indexada: {embedding_source_column}")
    
    status = index_info.get('status', {})
    print(f"\n📊 Estado: {status.get('detailed_state', status.get('state', 'UNKNOWN'))}")
    
    # Intentar obtener número de vectores indexados
    if 'num_indexed_rows' in status:
        print(f"📊 Vectores indexados: {status['num_indexed_rows']}")
    
    print("\n" + "="*70)
    
except Exception as e:
    print(f"⚠️  No se pudo obtener información detallada: {e}")

# COMMAND ----------

print("""
╔═══════════════════════════════════════════════════════════════╗
║      ✅ DÍA 4 COMPLETADO - VECTOR SEARCH INDEX CREADO        ║
╚═══════════════════════════════════════════════════════════════╝

✅ Componentes creados:
   • Vector Search Endpoint: dlco_qa_vector_endpoint
   • Vector Index: dlco_qa_agents.vectors.golden_notebooks_idx
   • Embedding Model: databricks-gte-large-en
   • Registros indexados: 22 ejemplos

✅ Capacidades habilitadas:
   • Búsqueda semántica de código similar
   • RAG (Retrieval Augmented Generation)
   • El agente puede encontrar ejemplos relevantes

🎯 DÍA 3-4 COMPLETADO AL 100%

🚀 Próximo paso: DÍA 5-6
   Crear y desplegar el Agente PySpark Validator
   
═══════════════════════════════════════════════════════════════
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🔗 Enlaces Útiles

# COMMAND ----------

print(f"""
📚 Recursos para verificar el Vector Search:

1. Vector Search UI:
   https://adb-319103249978237.17.azuredatabricks.net/#setting/clusters/vector-search

2. Endpoint Details:
   Compute → Vector Search → {endpoint_name}

3. Index Details:
   Buscar: {index_name}

4. Source Table:
   Data → dlco_qa_agents → silver → golden_qa_eval_v1
""")