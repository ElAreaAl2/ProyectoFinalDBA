"""
Script para asignar municipios a edificios usando operaciones espaciales
"""

import json
from pymongo import MongoClient
from shapely.geometry import shape, Point
from tqdm import tqdm
import os

# Configuración
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "pdet_solar"

# Archivos
INPUT_FILE = "../../data/buildings/buildings_microsoft_con_area.geojsonl"
OUTPUT_FILE = "../../data/buildings/buildings_microsoft_final.geojsonl"

def main():
    print("="*70)
    print("🗺️  ASIGNACIÓN DE MUNICIPIOS A EDIFICIOS")
    print("="*70 + "\n")
    
    # Verificar archivo de entrada
    if not os.path.exists(INPUT_FILE):
        print(f"❌ No se encontró el archivo: {INPUT_FILE}")
        print("\n💡 Ejecuta primero:")
        print("   python3 02_calcular_area_y_exportar_geojsonl_ms.py")
        return 1
    
    # Conectar a MongoDB
    print("🔌 Conectando a MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    
    # Cargar municipios en memoria
    print("📍 Cargando municipios PDET...")
    municipios = list(db.municipalities.find({"is_pdet": True}))
    print(f"✅ Cargados {len(municipios)} municipios\n")
    
    # Convertir geometrías de municipios a shapely
    print("🔄 Preparando geometrías...")
    municipios_shapes = []
    for mun in municipios:
        municipios_shapes.append({
            'codigo': mun['codigo_dane'],
            'nombre': mun['nombre'],
            'departamento': mun['departamento'],
            'geometry': shape(mun['geometry'])
        })
    print(f"✅ {len(municipios_shapes)} geometrías preparadas\n")
    
    # Contar edificios
    print(f"📂 Leyendo edificios: {INPUT_FILE}")
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        total_edificios = sum(1 for _ in f)
    
    print(f"✅ Encontrados {total_edificios} edificios\n")
    
    # Procesar edificios
    print("🔄 Asignando municipios a edificios...")
    
    edificios_asignados = 0
    edificios_sin_municipio = 0
    
    with open(INPUT_FILE, 'r', encoding='utf-8') as f_in, \
         open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
        
        for line in tqdm(f_in, total=total_edificios, desc="Procesando"):
            try:
                edificio = json.loads(line)
                
                # Obtener geometría del edificio
                geom_edificio = shape(edificio['geometry'])
                centroid = geom_edificio.centroid
                
                # Buscar en qué municipio está
                municipio_encontrado = None
                
                for mun in municipios_shapes:
                    if mun['geometry'].contains(centroid):
                        municipio_encontrado = mun
                        break
                
                # Agregar información del municipio
                if municipio_encontrado:
                    edificio['properties']['municipality_code'] = municipio_encontrado['codigo']
                    edificio['properties']['municipality_name'] = municipio_encontrado['nombre']
                    edificio['properties']['department'] = municipio_encontrado['departamento']
                    edificios_asignados += 1
                else:
                    # Mantener la información que ya tenía
                    edificios_sin_municipio += 1
                
                # Escribir al archivo de salida
                f_out.write(json.dumps(edificio, ensure_ascii=False) + '\n')
                
            except Exception as e:
                print(f"\n⚠️ Error procesando edificio: {e}")
                continue
    
    print("\n" + "="*70)
    print("✅ ASIGNACIÓN COMPLETADA")
    print("="*70)
    print(f"\n📊 Estadísticas:")
    print(f"  • Total edificios: {total_edificios}")
    print(f"  • Edificios asignados: {edificios_asignados}")
    print(f"  • Edificios sin municipio: {edificios_sin_municipio}")
    print(f"  • Tasa de éxito: {(edificios_asignados/total_edificios*100):.1f}%")
    print(f"\n📁 Archivo generado: {OUTPUT_FILE}")
    print("\n💡 Siguiente paso:")
    print("   python3 05_cargar_edificios_a_mongodb.py")
    print("="*70)
    
    client.close()
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
