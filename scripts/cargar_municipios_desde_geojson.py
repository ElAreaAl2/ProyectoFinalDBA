"""
Script para cargar municipios PDET desde GeoJSON a MongoDB
"""

import json
from pymongo import MongoClient
from datetime import datetime

# Configuración
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "pdet_solar"
COLLECTION_NAME = "municipalities"

# Ruta corregida del archivo
GEOJSON_FILE = "entrega2/MGN2024_MUNICIPIOS_PDET.geojson"

def main():
    print("="*70)
    print("📍 CARGA DE MUNICIPIOS PDET A MONGODB")
    print("="*70 + "\n")
    
    # Leer GeoJSON
    print(f"📂 Leyendo archivo: {GEOJSON_FILE}")
    try:
        with open(GEOJSON_FILE, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo {GEOJSON_FILE}")
        print("\n💡 Verifica que el archivo exista en:")
        print("   ~/ProyectoFinalDBA/entrega2/MGN2024_MUNICIPIOS_PDET.geojson")
        return 1
    
    features = geojson_data.get('features', [])
    print(f"✅ Archivo leído: {len(features)} municipios encontrados\n")
    
    # Conectar a MongoDB
    print(f"🔌 Conectando a MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    
    print(f"✅ Conectado a base de datos: {DATABASE_NAME}\n")
    
    # Limpiar colección existente (opcional)
    print("🗑️  Limpiando colección existente...")
    collection.delete_many({})
    print("✅ Colección limpiada\n")
    
    # Preparar documentos para inserción
    print("🔄 Preparando documentos...")
    documents = []
    
    for feature in features:
        props = feature.get('properties', {})
        geometry = feature.get('geometry', {})
        
        # Crear documento MongoDB usando los campos REALES del GeoJSON
        doc = {
            'codigo_dane': props.get('mpio_cdpmp', props.get('Código DANE Municipio', '')),
            'nombre': props.get('mpio_cnmbr', props.get('Municipio', '')),
            'departamento': props.get('dpto_cnmbr', props.get('Departamento', '')),
            'codigo_departamento': props.get('dpto_ccdgo', props.get('Código DANE Departamento', '')),
            'subregion_pdet': props.get('Subregión PDET', ''),
            'is_pdet': True,  # Todos son PDET en este archivo
            'geometry': geometry,
            'metadata': {
                'fecha_carga': datetime.utcnow(),
                'fuente': 'DANE MGN 2024',
                'area_km2': props.get('mpio_narea', 0),
                'año': props.get('mpio_nano', 2024),
                'tipo': props.get('mpio_tipo', 'MUNICIPIO')
            }
        }
        
        documents.append(doc)
    
    print(f"✅ {len(documents)} documentos preparados\n")
    
    # Insertar en MongoDB
    print("💾 Insertando documentos en MongoDB...")
    try:
        result = collection.insert_many(documents, ordered=False)
        print(f"✅ {len(result.inserted_ids)} municipios insertados correctamente\n")
    except Exception as e:
        print(f"⚠️  Advertencia durante inserción: {e}\n")
    
    # Verificar inserción
    print("🔍 Verificando inserción...")
    total = collection.count_documents({})
    pdet_count = collection.count_documents({'is_pdet': True})
    
    print(f"  • Total municipios en BD: {total}")
    print(f"  • Municipios PDET: {pdet_count}")
    
    # Mostrar algunos ejemplos
    print("\n📋 Ejemplos de municipios cargados:")
    sample = collection.find().limit(5)
    for i, mun in enumerate(sample, 1):
        print(f"  {i}. {mun['nombre']} ({mun['departamento']}) - Código: {mun['codigo_dane']}")
        print(f"      Subregión PDET: {mun.get('subregion_pdet', 'N/A')}")
    
    # Mostrar estadísticas por departamento
    print("\n📊 Municipios PDET por departamento:")
    pipeline = [
        {"$group": {
            "_id": "$departamento",
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    
    for dept in collection.aggregate(pipeline):
        print(f"  • {dept['_id']}: {dept['count']} municipios")
    
    print("\n" + "="*70)
    print("✅ CARGA COMPLETADA EXITOSAMENTE")
    print("="*70)
    
    # Cerrar conexión
    client.close()
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
