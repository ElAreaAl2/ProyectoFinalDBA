"""
Script de Validación Rápida - Entrega 3
========================================

Verifica que todos los componentes de la Entrega 3 estén listos:
- Archivos de datos existen
- MongoDB está accesible
- Colecciones tienen datos
- Índices están creados
- Scripts están completos

Fecha: Noviembre 10, 2025
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pathlib import Path
import sys

# Configuración
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "pdet_solar_analysis"

def check_mongodb_connection():
    """Verifica conexión a MongoDB"""
    print("\n Verificando conexión a MongoDB...")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.server_info()
        print("   MongoDB está accesible")
        return client
    except ConnectionFailure:
        print("    No se puede conectar a MongoDB")
        print("      Asegúrate de que MongoDB esté corriendo:")
        print("      > net start MongoDB (Windows)")
        print("      > mongod --dbpath <ruta> (manual)")
        return None

def check_collections(db):
    """Verifica que las colecciones existan y tengan datos"""
    print("\n Verificando colecciones en MongoDB...")
    
    required_collections = [
        "municipalities",
        "buildings_microsoft",
        "buildings_google"
    ]
    
    all_ok = True
    
    for coll_name in required_collections:
        count = db[coll_name].count_documents({})
        if count > 0:
            print(f"   Hay {coll_name}: {count:,} documentos")
        else:
            print(f"   No hay documentos en {coll_name}: 0 documentos (vacía)")
            all_ok = False
    
    return all_ok

def check_spatial_indexes(db):
    """Verifica índices espaciales"""
    print("\n🗺️  Verificando índices espaciales (2dsphere)...")
    
    collections_to_check = [
        "municipalities",
        "buildings_microsoft",
        "buildings_google"
    ]
    
    all_ok = True
    
    for coll_name in collections_to_check:
        indexes = list(db[coll_name].list_indexes())
        has_2dsphere = any("2dsphere" in str(idx.get("key", {})) for idx in indexes)
        
        if has_2dsphere:
            print(f"    {coll_name}: índice 2dsphere encontrado")
        else:
            print(f"    {coll_name}: índice 2dsphere NO encontrado")
            all_ok = False
    
    return all_ok

def check_municipality_assignment(db):
    """Verifica asignación de municipios a edificios"""
    print("\n Verificando asignación de municipios...")
    
    all_ok = True
    
    for coll_name in ["buildings_microsoft", "buildings_google"]:
        total = db[coll_name].count_documents({})
        with_muni = db[coll_name].count_documents({"municipality_code": {"$exists": True}})
        
        if total > 0:
            percentage = (with_muni / total) * 100
            if percentage > 50:
                print(f"    {coll_name}: {with_muni:,}/{total:,} ({percentage:.1f}%) con municipio")
            else:
                print(f"     {coll_name}: {with_muni:,}/{total:,} ({percentage:.1f}%) con municipio")
                print(f"      Ejecuta: 04_asignar_municipio_a_edificios.py")
                all_ok = False
        else:
            print(f"     {coll_name}: colección vacía")
            all_ok = False
    
    return all_ok

def check_data_files():
    """Verifica existencia de archivos de datos"""
    print("\n Verificando archivos de datos...")
    
    files_to_check = [
        ("data/ms/ms_co.geojsonl", "Microsoft GeoJSONL"),
        ("data/gg/gg_co.geojsonl", "Google GeoJSONL"),
        ("entrega2/MGN2024_MUNICIPIOS_PDET.geojson", "Municipios PDET")
    ]
    
    all_ok = True
    
    for filepath, description in files_to_check:
        path = Path(filepath)
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            print(f"    {description}: {path.name} ({size_mb:.2f} MB)")
        else:
            print(f"    {description}: {filepath} NO ENCONTRADO")
            all_ok = False
    
    return all_ok

def check_scripts():
    """Verifica que los scripts de la entrega estén completos"""
    print("\n Verificando scripts de la entrega...")
    
    scripts_to_check = [
        ("entrega3/loaders/01_descargar_edificios_ms_desde_planetary_computer.py", "> 5 KB"),
        ("entrega3/loaders/04_asignar_municipio_a_edificios.py", "> 1 KB"),
        ("entrega3/loaders/05_cargar_edificios_a_mongodb.py", "> 10 KB"),
        ("entrega3/eda/01_analisis_rapido_top_municipios_y_area.py", "> 10 KB"),
        ("entrega3/README.md", "> 5 KB")
    ]
    
    all_ok = True
    
    for filepath, size_req in scripts_to_check:
        path = Path(filepath)
        if path.exists():
            size_kb = path.stat().st_size / 1024
            min_size = int(size_req.split()[1])
            
            if size_kb >= min_size:
                print(f"    {path.name}: {size_kb:.1f} KB")
            else:
                print(f"     {path.name}: {size_kb:.1f} KB (esperado {size_req})")
                all_ok = False
        else:
            print(f"    {filepath}: NO ENCONTRADO")
            all_ok = False
    
    return all_ok

def check_eda_outputs():
    """Verifica outputs del EDA"""
    print("\n Verificando outputs del EDA...")
    
    output_dir = Path("entrega3/eda/results")
    
    if not output_dir.exists():
        print(f"     Carpeta {output_dir} no existe")
        print("      Ejecuta: 01_analisis_rapido_top_municipios_y_area.py")
        return False
    
    expected_files = [
        "top_municipios_microsoft.csv",
        "top_municipios_google.csv",
        "comparacion_completa.csv",
        "top_municipios_microsoft.png",
        "top_municipios_google.png",
        "comparacion_datasets.png"
    ]
    
    all_ok = True
    
    for filename in expected_files:
        filepath = output_dir / filename
        if filepath.exists():
            print(f"    {filename}")
        else:
            print(f"     {filename} no generado")
            all_ok = False
    
    if not all_ok:
        print("\n    Ejecuta: python entrega3/eda/01_analisis_rapido_top_municipios_y_area.py")
    
    return all_ok

def print_summary(results):
    """Imprime resumen final"""
    print("\n" + "="*70)
    print("📋 RESUMEN DE VALIDACIÓN - ENTREGA 3")
    print("="*70)
    
    checks = [
        ("MongoDB Connection", results["mongodb"]),
        ("Colecciones con datos", results["collections"]),
        ("Índices espaciales", results["indexes"]),
        ("Asignación de municipios", results["assignment"]),
        ("Archivos de datos", results["files"]),
        ("Scripts completos", results["scripts"]),
        ("Outputs del EDA", results["eda"])
    ]
    
    all_passed = all(r for r in results.values())
    
    for check_name, passed in checks:
        status = "✅" if passed else "❌"
        print(f"{status} {check_name}")
    
    print("="*70)
    
    if all_passed:
        print("\n ¡TODO LISTO!")
        print("   Todos los componentes están completos y funcionales.\n")
        return True
    else:
        print("\n HAY COMPONENTES PENDIENTES")
        print("   Revisar los mensajes arriba y completa lo que falta.\n")
        return False

def main():
    """Función principal de validación"""
    
    print("="*70)
    print(" VALIDACIÓN DE ENTREGA 3")
    print("   Carga de Datos de Edificios y EDA Inicial")
    print("="*70)
    
    results = {}
    
    # 1. MongoDB
    client = check_mongodb_connection()
    results["mongodb"] = client is not None
    
    if not client:
        print("\n No se puede continuar sin MongoDB")
        print_summary(results)
        sys.exit(1)
    
    db = client[DB_NAME]
    
    # 2. Colecciones
    results["collections"] = check_collections(db)
    
    # 3. Índices espaciales
    results["indexes"] = check_spatial_indexes(db)
    
    # 4. Asignación de municipios
    results["assignment"] = check_municipality_assignment(db)
    
    # Cerrar conexión MongoDB
    client.close()
    
    # 5. Archivos de datos
    results["files"] = check_data_files()
    
    # 6. Scripts
    results["scripts"] = check_scripts()
    
    # 7. Outputs EDA
    results["eda"] = check_eda_outputs()
    
    # Resumen final
    success = print_summary(results)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
