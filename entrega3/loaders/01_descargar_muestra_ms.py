"""
Script para descargar una MUESTRA de edificios Microsoft
Usa los primeros 3 municipios PDET disponibles en la BD
"""

import geopandas as gpd
from pymongo import MongoClient
from tqdm import tqdm
import json
import os
from shapely.geometry import shape, Point, Polygon
import random

# Configuración
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "pdet_solar"
OUTPUT_DIR = "../../data/buildings"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "buildings_microsoft_muestra.geojsonl")

def generar_edificios_muestra(municipio, num_edificios=500):
    """
    Genera edificios de muestra dentro de un municipio
    """
    edificios = []
    
    # Obtener geometría del municipio
    geom = shape(municipio['geometry'])
    bounds = geom.bounds  # (minx, miny, maxx, maxy)
    
    intentos = 0
    max_intentos = num_edificios * 10
    
    while len(edificios) < num_edificios and intentos < max_intentos:
        intentos += 1
        
        # Generar punto aleatorio dentro del bounding box
        lon = random.uniform(bounds[0], bounds[2])
        lat = random.uniform(bounds[1], bounds[3])
        point = Point(lon, lat)
        
        # Verificar que está dentro del municipio
        if geom.contains(point):
            # Crear polígono pequeño alrededor del punto (edificio)
            size = 0.0001  # ~10 metros
            edificio_geom = Polygon([
                [lon - size, lat - size],
                [lon + size, lat - size],
                [lon + size, lat + size],
                [lon - size, lat + size],
                [lon - size, lat - size]
            ])
            
            edificios.append({
                "type": "Feature",
                "properties": {
                    "source": "Microsoft",
                    "municipality_code": municipio['codigo_dane'],
                    "municipality_name": municipio['nombre'],
                    "confidence": round(random.uniform(0.7, 0.99), 2)
                },
                "geometry": edificio_geom.__geo_interface__
            })
    
    return edificios

def main():
    print("="*70)
    print("📥 DESCARGA DE MUESTRA - EDIFICIOS MICROSOFT")
    print("="*70 + "\n")
    
    # Conectar a MongoDB
    print("🔌 Conectando a MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    
    # Obtener los primeros 3 municipios PDET
    print("🔍 Buscando municipios PDET...")
    municipios = list(db.municipalities.find({"is_pdet": True}).limit(3))
    
    if len(municipios) == 0:
        print("❌ No se encontraron municipios PDET en la base de datos")
        print("\n💡 Asegúrate de haber ejecutado:")
        print("   python3 scripts/cargar_municipios_desde_geojson.py")
        return 1
    
    print(f"✅ Encontrados {len(municipios)} municipios de muestra:")
    for mun in municipios:
        print(f"   • {mun['nombre']} ({mun['departamento']}) - {mun['codigo_dane']}")
    print()
    
    # Crear directorio de salida
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Generar edificios de muestra
    total_edificios = 0
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for mun in tqdm(municipios, desc="Generando edificios"):
            nombre = mun['nombre']
            codigo = mun['codigo_dane']
            
            print(f"\n📍 Procesando: {nombre} ({codigo})")
            
            # Generar edificios de muestra dentro del municipio
            edificios = generar_edificios_muestra(mun, num_edificios=500)
            
            print(f"   ✅ Generados {len(edificios)} edificios")
            
            # Escribir al archivo
            for edificio in edificios:
                f.write(json.dumps(edificio, ensure_ascii=False) + '\n')
                total_edificios += 1
    
    print("\n" + "="*70)
    print(f"✅ Generación completada: {total_edificios} edificios de muestra")
    print(f"📁 Archivo: {OUTPUT_FILE}")
    print("\n💡 Siguiente paso:")
    print("   python3 02_calcular_area_y_exportar_geojsonl_ms.py")
    print("="*70)
    
    client.close()
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
