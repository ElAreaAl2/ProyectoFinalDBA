"""
Script de Carga Masiva (Bulk Load) de Edificios a MongoDB
==========================================================

Este script carga los datos de edificios (Microsoft y Google) en formato GeoJSONL
a las colecciones de MongoDB usando bulk_write para máxima eficiencia.

Características:
- Carga por batches (bulk operations)
- Validación de geometrías
- Manejo de errores
- Progress tracking
- Estadísticas de carga

Fecha: Noviembre 10, 2025
"""

from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
import json
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import time

# Configuración
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "pdet_solar_analysis"
BATCH_SIZE = 10000  # Tamaño de batch para bulk_write

def count_lines(filepath):
    """Cuenta líneas en archivo para progress bar"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f)

def validate_building_document(doc, source):
    """
    Valida que el documento tenga la estructura correcta
    antes de insertarlo en MongoDB
    """
    # Campos requeridos
    if "geometry" not in doc:
        return False
    
    geometry = doc["geometry"]
    if not isinstance(geometry, dict):
        return False
    
    if "type" not in geometry or "coordinates" not in geometry:
        return False
    
    # Asegurar que tenga properties
    if "properties" not in doc:
        doc["properties"] = {}
    
    # Asegurar que tenga area_m2
    if "area_m2" not in doc["properties"]:
        doc["properties"]["area_m2"] = 0.0
    
    # Agregar metadata
    doc["metadata"] = {
        "source": source,
        "load_date": datetime.utcnow(),
        "version": "v1.0" if source == "Microsoft" else "v3",
        "batch_loaded": True
    }
    
    return True

def load_geojsonl_to_mongodb(
    geojsonl_path: Path,
    collection_name: str,
    source: str,
    batch_size: int = BATCH_SIZE
):
    """
    Carga un archivo GeoJSONL a MongoDB usando bulk operations
    
    Args:
        geojsonl_path: Ruta al archivo .geojsonl
        collection_name: Nombre de la colección MongoDB
        source: "Microsoft" o "Google"
        batch_size: Tamaño del batch para bulk_write
    
    Returns:
        dict: Estadísticas de la carga
    """
    
    print(f"\n{'='*70}")
    print(f"Cargando: {geojsonl_path.name}")
    print(f"Colección: {collection_name}")
    print(f"Fuente: {source}")
    print(f"{'='*70}\n")
    
    # Conectar a MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[collection_name]
    
    # Estadísticas
    stats = {
        "total_read": 0,
        "total_inserted": 0,
        "total_errors": 0,
        "invalid_docs": 0,
        "start_time": time.time()
    }
    
    # Contar líneas para progress bar
    print("Contando registros...")
    total_lines = count_lines(geojsonl_path)
    print(f"Total de edificios a cargar: {total_lines:,}\n")
    
    # Leer y cargar en batches
    batch = []
    
    with open(geojsonl_path, 'r', encoding='utf-8') as f:
        for line in tqdm(f, total=total_lines, desc="Cargando edificios"):
            stats["total_read"] += 1
            
            try:
                # Parse JSON
                doc = json.loads(line.strip())
                
                # Validar y preparar documento
                if not validate_building_document(doc, source):
                    stats["invalid_docs"] += 1
                    continue
                
                # Agregar a batch
                batch.append(InsertOne(doc))
                
                # Ejecutar bulk_write cuando el batch está lleno
                if len(batch) >= batch_size:
                    try:
                        result = collection.bulk_write(batch, ordered=False)
                        stats["total_inserted"] += result.inserted_count
                    except BulkWriteError as bwe:
                        # Algunos documentos se insertaron, otros fallaron
                        stats["total_inserted"] += bwe.details.get('nInserted', 0)
                        stats["total_errors"] += len(bwe.details.get('writeErrors', []))
                    except Exception as e:
                        print(f"\n❌ Error en bulk_write: {e}")
                        stats["total_errors"] += len(batch)
                    
                    batch = []
                    
            except json.JSONDecodeError as e:
                stats["total_errors"] += 1
                continue
            except Exception as e:
                stats["total_errors"] += 1
                continue
    
    # Insertar el último batch si quedó algo
    if batch:
        try:
            result = collection.bulk_write(batch, ordered=False)
            stats["total_inserted"] += result.inserted_count
        except BulkWriteError as bwe:
            stats["total_inserted"] += bwe.details.get('nInserted', 0)
            stats["total_errors"] += len(bwe.details.get('writeErrors', []))
        except Exception as e:
            print(f"\n❌ Error en último batch: {e}")
            stats["total_errors"] += len(batch)
    
    # Calcular tiempo
    stats["duration_seconds"] = time.time() - stats["start_time"]
    stats["docs_per_second"] = stats["total_inserted"] / stats["duration_seconds"] if stats["duration_seconds"] > 0 else 0
    
    # Verificar conteo en MongoDB
    stats["count_in_db"] = collection.count_documents({})
    
    # Cerrar conexión
    client.close()
    
    return stats

def print_stats(stats, source):
    """Imprime estadísticas de carga"""
    print(f"\n{'='*70}")
    print(f"📊 ESTADÍSTICAS DE CARGA - {source}")
    print(f"{'='*70}")
    print(f"Total leído:              {stats['total_read']:>15,}")
    print(f"Total insertado:          {stats['total_inserted']:>15,}")
    print(f"Documentos inválidos:     {stats['invalid_docs']:>15,}")
    print(f"Errores:                  {stats['total_errors']:>15,}")
    print(f"Documentos en MongoDB:    {stats['count_in_db']:>15,}")
    print(f"{'─'*70}")
    print(f"Tiempo total:             {stats['duration_seconds']:>15.2f} segundos")
    print(f"Velocidad:                {stats['docs_per_second']:>15,.0f} docs/seg")
    print(f"{'='*70}\n")

def verify_spatial_indexes(db_name=DB_NAME):
    """Verifica que existan los índices espaciales necesarios"""
    client = MongoClient(MONGO_URI)
    db = client[db_name]
    
    print("\n🔍 Verificando índices espaciales...\n")
    
    for collection_name in ["buildings_microsoft", "buildings_google"]:
        collection = db[collection_name]
        indexes = list(collection.list_indexes())
        
        has_2dsphere = any("2dsphere" in str(idx.get("key", {})) for idx in indexes)
        
        if has_2dsphere:
            print(f"✅ {collection_name}: Índice 2dsphere encontrado")
        else:
            print(f"⚠️  {collection_name}: Índice 2dsphere NO encontrado")
            print(f"   Creando índice espacial...")
            collection.create_index([("geometry", "2dsphere")])
            print(f"✅ Índice creado exitosamente")
    
    client.close()

def main():
    """Función principal de carga"""
    
    print("\n" + "="*70)
    print("🚀 CARGA MASIVA DE EDIFICIOS A MONGODB")
    print("="*70)
    
    # Definir rutas de archivos
    data_dir = Path("data")
    
    # Rutas Microsoft
    ms_geojsonl = data_dir / "ms" / "ms_co.geojsonl"
    
    # Rutas Google
    gg_geojsonl = data_dir / "gg" / "gg_co.geojsonl"
    
    # Verificar que existan los archivos
    print("\n📁 Verificando archivos de entrada...\n")
    
    files_to_load = []
    
    if ms_geojsonl.exists():
        print(f"✅ Microsoft: {ms_geojsonl}")
        files_to_load.append(("Microsoft", ms_geojsonl, "buildings_microsoft"))
    else:
        print(f"⚠️  Microsoft: {ms_geojsonl} NO ENCONTRADO")
    
    if gg_geojsonl.exists():
        print(f"✅ Google:    {gg_geojsonl}")
        files_to_load.append(("Google", gg_geojsonl, "buildings_google"))
    else:
        print(f"⚠️  Google:    {gg_geojsonl} NO ENCONTRADO")
    
    if not files_to_load:
        print("\n❌ No se encontraron archivos para cargar. Ejecuta primero los scripts de descarga.")
        return
    
    # Verificar índices espaciales
    verify_spatial_indexes()
    
    # Cargar cada dataset
    all_stats = {}
    
    for source, filepath, collection_name in files_to_load:
        stats = load_geojsonl_to_mongodb(
            filepath,
            collection_name,
            source,
            batch_size=BATCH_SIZE
        )
        print_stats(stats, source)
        all_stats[source] = stats
    
    # Resumen final
    print("\n" + "="*70)
    print("✅ CARGA COMPLETADA")
    print("="*70)
    
    total_inserted = sum(s["total_inserted"] for s in all_stats.values())
    total_time = sum(s["duration_seconds"] for s in all_stats.values())
    
    print(f"\nTotal de edificios cargados: {total_inserted:,}")
    print(f"Tiempo total: {total_time:.2f} segundos ({total_time/60:.2f} minutos)")
    print(f"\n🎉 Datos listos para análisis espacial!\n")

if __name__ == "__main__":
    main()
