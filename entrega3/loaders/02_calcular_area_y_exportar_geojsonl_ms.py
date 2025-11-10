"""
Script para calcular áreas de edificios y exportar a GeoJSONL
"""

import json
from shapely.geometry import shape
from shapely.ops import transform
import pyproj
from functools import partial
from tqdm import tqdm
import os

# Archivos
INPUT_FILE = "../../data/buildings/buildings_microsoft_muestra.geojsonl"
OUTPUT_FILE = "../../data/buildings/buildings_microsoft_con_area.geojsonl"

def calcular_area_m2(geometry_dict):
    """
    Calcula el área en metros cuadrados de una geometría GeoJSON
    """
    try:
        # Convertir a shapely geometry
        geom = shape(geometry_dict)
        
        # Obtener centroide para determinar la zona UTM apropiada
        centroid = geom.centroid
        lon, lat = centroid.x, centroid.y
        
        # Determinar zona UTM (simplificado para Colombia)
        # Colombia está principalmente en zonas 17N, 18N
        utm_zone = int((lon + 180) / 6) + 1
        
        # Proyección: WGS84 (EPSG:4326) a UTM
        wgs84 = pyproj.CRS('EPSG:4326')
        utm = pyproj.CRS(f'EPSG:326{utm_zone}')  # 326XX es norte
        
        project = partial(
            pyproj.transform,
            pyproj.Proj(wgs84),
            pyproj.Proj(utm)
        )
        
        # Transformar a UTM y calcular área
        geom_utm = transform(project, geom)
        area_m2 = geom_utm.area
        
        return round(area_m2, 2)
    
    except Exception as e:
        print(f"⚠️ Error calculando área: {e}")
        return 0.0

def main():
    print("="*70)
    print("📐 CÁLCULO DE ÁREAS - EDIFICIOS MICROSOFT")
    print("="*70 + "\n")
    
    # Verificar que existe el archivo de entrada
    if not os.path.exists(INPUT_FILE):
        print(f"❌ No se encontró el archivo: {INPUT_FILE}")
        print("\n💡 Ejecuta primero:")
        print("   python3 01_descargar_muestra_ms.py")
        return 1
    
    # Contar líneas del archivo
    print(f"📂 Leyendo archivo: {INPUT_FILE}")
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f)
    
    print(f"✅ Encontrados {total_lines} edificios\n")
    
    # Procesar edificios
    print("🔄 Calculando áreas...")
    
    edificios_procesados = 0
    area_total = 0
    
    with open(INPUT_FILE, 'r', encoding='utf-8') as f_in, \
         open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
        
        for line in tqdm(f_in, total=total_lines, desc="Procesando"):
            try:
                edificio = json.loads(line)
                
                # Calcular área
                geometry = edificio.get('geometry', {})
                area_m2 = calcular_area_m2(geometry)
                
                # Agregar área a las propiedades
                if 'properties' not in edificio:
                    edificio['properties'] = {}
                
                edificio['properties']['area_m2'] = area_m2
                
                # Escribir al archivo de salida
                f_out.write(json.dumps(edificio, ensure_ascii=False) + '\n')
                
                edificios_procesados += 1
                area_total += area_m2
                
            except Exception as e:
                print(f"\n⚠️ Error procesando línea: {e}")
                continue
    
    print("\n" + "="*70)
    print("✅ CÁLCULO COMPLETADO")
    print("="*70)
    print(f"\n📊 Estadísticas:")
    print(f"  • Edificios procesados: {edificios_procesados}")
    print(f"  • Área total: {area_total:,.2f} m²")
    print(f"  • Área promedio: {area_total/edificios_procesados:,.2f} m²")
    print(f"\n📁 Archivo generado: {OUTPUT_FILE}")
    print("\n💡 Siguiente paso:")
    print("   python3 04_asignar_municipio_a_edificios.py")
    print("="*70)
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
