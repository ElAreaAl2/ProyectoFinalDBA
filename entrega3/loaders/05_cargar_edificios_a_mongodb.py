"""
Script para cargar edificios a MongoDB con validación y bulk insert
"""

import json
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from tqdm import tqdm
import os
from datetime import datetime

# Configuración
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "pdet_solar"
BATCH_SIZE = 1000

# Archivos de entrada
INPUT_FILES = {
    'microsoft': '../../data/buildings/buildings_microsoft_final.geojsonl',
    'google': '../../data/buildings/buildings_google_final.geojsonl'  # Opcional
}

def preparar_documento_mongodb(feature, source):
    """
    Convierte un Feature GeoJSON en documento MongoDB
    """
    props = feature.get('properties', {})
    geometry = feature.get('geometry', {})
    
    doc = {
        'municipality_code': props.get('municipality_code', ''),
        'municipality_name': props.get('municipality_name', ''),
        'department': props.get('department', props.get('departamento', '')),
        'geometry': geometry,
        'area_m2': float(props.get('area_m2', 0)),
        'source': source,
        'metadata': {
            'fecha_carga': datetime.utcnow(),
            'confidence': props.get('confidence', None)
        }
    }
    
    return doc

def cargar_dataset(file_path, collection_name, source_name):
    """
    Carga un dataset completo a MongoDB
    """
    print(f"\n{'='*70}")
    print(f"📥 CARGANDO: {source_name}")
    print(f"{'='*70}\n")
    
    if not os.path.exists(file_path):
        print(f"⚠️  Archivo no encontrado: {file_path}")
        print(f"   Saltando {source_name}...")
        return 0
    
    # Conectar a MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[collection_name]
    
    # Limpiar colección (opcional)
    print("🗑️  Limpiando colección existente...")
    collection.delete_many({})
    print("✅ Colección limpiada\n")
    
    # Contar líneas
    print(f"📂 Leyendo archivo: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f)
    
    print(f"✅ Encontrados {total_lines} edificios\n")
    
    # Cargar en lotes
    print(f"💾 Cargando a MongoDB (lotes de {BATCH_SIZE})...")
    
    documentos_cargados = 0
    documentos_error = 0
    lote_actual = []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in tqdm(f, total=total_lines, desc="Cargando"):
            try:
                feature = json.loads(line)
                doc = preparar_documento_mongodb(feature, source_name)
                
                lote_actual.append(doc)
                
                # Insertar lote cuando alcanza el tamaño
                if len(lote_actual) >= BATCH_SIZE:
                    try:
                        result = collection.insert_many(lote_actual, ordered=False)
                        documentos_cargados += len(result.inserted_ids)
                    except BulkWriteError as e:
                        documentos_cargados += e.details['nInserted']
                        documentos_error += len(e.details.get('writeErrors', []))
                    
                    lote_actual = []
            
            except Exception as e:
                documentos_error += 1
                continue
        
        # Insertar último lote
        if lote_actual:
            try:
                result = collection.insert_many(lote_actual, ordered=False)
                documentos_cargados += len(result.inserted_ids)
            except BulkWriteError as e:
                documentos_cargados += e.details['nInserted']
                documentos_error += len(e.details.get('writeErrors', []))
    
    # Estadísticas
    print(f"\n{'='*70}")
    print(f"✅ CARGA COMPLETADA: {source_name}")
    print(f"{'='*70}")
    print(f"\n📊 Estadísticas:")
    print(f"  • Documentos cargados: {documentos_cargados}")
    print(f"  • Documentos con error: {documentos_error}")
    print(f"  • Tasa de éxito: {(documentos_cargados/total_lines*100):.1f}%")
    
    # Verificar en BD
    total_bd = collection.count_documents({})
    print(f"\n🔍 Verificación en MongoDB:")
    print(f"  • Total en colección: {total_bd}")
    
    # Estadísticas por municipio
    print(f"\n📍 Top 5 municipios con más edificios:")
    pipeline = [
        {"$group": {
            "_id": "$municipality_name",
            "count": {"$sum": 1},
            "area_total": {"$sum": "$area_m2"}
        }},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    
    for mun in collection.aggregate(pipeline):
        print(f"  • {mun['_id']}: {mun['count']} edificios, {mun['area_total']:,.0f} m²")
    
    client.close()
    return documentos_cargados

def main():
    print("\n" + "="*70)
    print("🚀 CARGA MASIVA DE EDIFICIOS A MONGODB")
    print("="*70 + "\n")
    
    total_cargado = 0
    
    # Cargar Microsoft
    if os.path.exists(INPUT_FILES['microsoft']):
        total_cargado += cargar_dataset(
            INPUT_FILES['microsoft'],
            'buildings_microsoft',
            'Microsoft'
        )
    else:
        print(f"⚠️  No se encontró archivo Microsoft: {INPUT_FILES['microsoft']}")
    
    # Cargar Google (si existe)
    if os.path.exists(INPUT_FILES['google']):
        total_cargado += cargar_dataset(
            INPUT_FILES['google'],
            'buildings_google',
            'Google'
        )
    else:
        print(f"\n⚠️  No se encontró archivo Google (opcional): {INPUT_FILES['google']}")
        print("   Continuando solo con Microsoft...")
    
    # Resumen final
    print("\n" + "="*70)
    print("✅ PROCESO COMPLETADO")
    print("="*70)
    print(f"\n📊 Total edificios cargados: {total_cargado}")
    print("\n💡 Siguiente paso:")
    print("   cd ../eda")
    print("   python3 01_analisis_rapido_top_municipios_y_area.py")
    print("="*70 + "\n")
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
