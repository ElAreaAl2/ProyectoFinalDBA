"""
Análisis Exploratorio de Datos (EDA) - Edificios PDET
======================================================

Este script realiza un análisis exploratorio inicial de los datos de edificios
cargados en MongoDB, generando estadísticas básicas y visualizaciones.

Incluye:
- Conteo de edificios por municipio
- Distribución de áreas
- Top municipios por área total
- Comparación Microsoft vs Google
- Estadísticas de calidad de datos

Fecha: Noviembre 10, 2025
"""

from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime

# Configuración
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "pdet_solar_analysis"
OUTPUT_DIR = Path("entrega3/eda/results")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Estilo de gráficos
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

def get_collection_stats(db, collection_name):
    """Obtiene estadísticas básicas de una colección"""
    collection = db[collection_name]
    
    stats = {
        "collection": collection_name,
        "total_documents": collection.count_documents({}),
        "with_municipality": collection.count_documents({"municipality_code": {"$exists": True}}),
        "without_municipality": collection.count_documents({"municipality_code": {"$exists": False}}),
    }
    
    # Estadísticas de área
    pipeline = [
        {"$match": {"properties.area_m2": {"$exists": True}}},
        {"$group": {
            "_id": None,
            "total_area_m2": {"$sum": "$properties.area_m2"},
            "avg_area_m2": {"$avg": "$properties.area_m2"},
            "min_area_m2": {"$min": "$properties.area_m2"},
            "max_area_m2": {"$max": "$properties.area_m2"}
        }}
    ]
    
    result = list(collection.aggregate(pipeline))
    if result:
        stats.update(result[0])
        del stats["_id"]
    
    return stats

def get_top_municipalities(db, collection_name, top_n=10):
    """
    Obtiene los top N municipios por número de edificios y área total
    """
    collection = db[collection_name]
    
    pipeline = [
        {
            "$match": {
                "municipality_code": {"$exists": True, "$ne": None}
            }
        },
        {
            "$group": {
                "_id": "$municipality_code",
                "count_buildings": {"$sum": 1},
                "total_area_m2": {"$sum": "$properties.area_m2"},
                "avg_area_m2": {"$avg": "$properties.area_m2"}
            }
        },
        {
            "$sort": {"total_area_m2": -1}
        },
        {
            "$limit": top_n
        }
    ]
    
    results = list(collection.aggregate(pipeline))
    
    # Convertir a DataFrame
    if results:
        df = pd.DataFrame(results)
        df.rename(columns={"_id": "codigo_dane"}, inplace=True)
        
        # Obtener nombres de municipios
        munis = db.municipalities.find(
            {"codigo_dane": {"$in": df["codigo_dane"].tolist()}},
            {"codigo_dane": 1, "nombre_municipio": 1, "nombre_departamento": 1}
        )
        
        muni_dict = {m["codigo_dane"]: f"{m['nombre_municipio']}, {m['nombre_departamento']}" 
                     for m in munis}
        
        df["nombre"] = df["codigo_dane"].map(muni_dict)
        
        # Convertir área a km²
        df["total_area_km2"] = df["total_area_m2"] / 1_000_000
        
        return df
    
    return pd.DataFrame()

def compare_datasets(db):
    """
    Compara los datasets de Microsoft y Google
    """
    print("\n" + "="*70)
    print("📊 COMPARACIÓN DE DATASETS")
    print("="*70 + "\n")
    
    # Obtener municipios PDET
    pdet_munis = list(db.municipalities.find(
        {"is_pdet": True},
        {"codigo_dane": 1, "nombre_municipio": 1}
    ))
    
    pdet_codes = [m["codigo_dane"] for m in pdet_munis]
    
    comparison_data = []
    
    for muni_code in pdet_codes:
        # Contar edificios Microsoft
        ms_count = db.buildings_microsoft.count_documents(
            {"municipality_code": muni_code}
        )
        
        # Contar edificios Google
        gg_count = db.buildings_google.count_documents(
            {"municipality_code": muni_code}
        )
        
        # Área total Microsoft
        ms_area = list(db.buildings_microsoft.aggregate([
            {"$match": {"municipality_code": muni_code}},
            {"$group": {"_id": None, "total": {"$sum": "$properties.area_m2"}}}
        ]))
        ms_area = ms_area[0]["total"] if ms_area else 0
        
        # Área total Google
        gg_area = list(db.buildings_google.aggregate([
            {"$match": {"municipality_code": muni_code}},
            {"$group": {"_id": None, "total": {"$sum": "$properties.area_m2"}}}
        ]))
        gg_area = gg_area[0]["total"] if gg_area else 0
        
        if ms_count > 0 or gg_count > 0:
            comparison_data.append({
                "codigo_dane": muni_code,
                "ms_buildings": ms_count,
                "gg_buildings": gg_count,
                "ms_area_m2": ms_area,
                "gg_area_m2": gg_area
            })
    
    df_comparison = pd.DataFrame(comparison_data)
    
    if not df_comparison.empty:
        # Agregar nombres de municipios
        muni_names = {m["codigo_dane"]: m["nombre_municipio"] for m in pdet_munis}
        df_comparison["municipio"] = df_comparison["codigo_dane"].map(muni_names)
        
        # Calcular diferencias
        df_comparison["diff_buildings"] = df_comparison["gg_buildings"] - df_comparison["ms_buildings"]
        df_comparison["diff_area_m2"] = df_comparison["gg_area_m2"] - df_comparison["ms_area_m2"]
        
        # Convertir a km²
        df_comparison["ms_area_km2"] = df_comparison["ms_area_m2"] / 1_000_000
        df_comparison["gg_area_km2"] = df_comparison["gg_area_m2"] / 1_000_000
        
        return df_comparison
    
    return pd.DataFrame()

def generate_visualizations(df_ms_top, df_gg_top, df_comparison):
    """
    Genera visualizaciones del EDA
    """
    print("\n📈 Generando visualizaciones...\n")
    
    # 1. Top municipios por área - Microsoft
    if not df_ms_top.empty:
        fig, ax = plt.subplots(figsize=(14, 8))
        df_ms_top_plot = df_ms_top.head(15).sort_values("total_area_km2")
        ax.barh(df_ms_top_plot["nombre"], df_ms_top_plot["total_area_km2"], color='#00A4EF')
        ax.set_xlabel("Área Total de Techos (km²)")
        ax.set_title("Top 15 Municipios PDET por Área de Techos - Microsoft Building Footprints", 
                     fontsize=14, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / "top_municipios_microsoft.png", dpi=300, bbox_inches='tight')
        print("✅ Guardado: top_municipios_microsoft.png")
        plt.close()
    
    # 2. Top municipios por área - Google
    if not df_gg_top.empty:
        fig, ax = plt.subplots(figsize=(14, 8))
        df_gg_top_plot = df_gg_top.head(15).sort_values("total_area_km2")
        ax.barh(df_gg_top_plot["nombre"], df_gg_top_plot["total_area_km2"], color='#34A853')
        ax.set_xlabel("Área Total de Techos (km²)")
        ax.set_title("Top 15 Municipios PDET por Área de Techos - Google Open Buildings", 
                     fontsize=14, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / "top_municipios_google.png", dpi=300, bbox_inches='tight')
        print("✅ Guardado: top_municipios_google.png")
        plt.close()
    
    # 3. Comparación lado a lado
    if not df_comparison.empty:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
        
        # Top 10 por diferencia en número de edificios
        df_top_diff = df_comparison.nlargest(10, 'diff_buildings')
        ax1.barh(df_top_diff["municipio"], df_top_diff["diff_buildings"], color='#FBBC04')
        ax1.set_xlabel("Diferencia (Google - Microsoft)")
        ax1.set_title("Top 10: Mayor diferencia en número de edificios", fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)
        
        # Top 10 por diferencia en área
        df_top_area = df_comparison.nlargest(10, 'diff_area_m2')
        diff_area_km2 = df_top_area["diff_area_m2"] / 1_000_000
        ax2.barh(df_top_area["municipio"], diff_area_km2, color='#EA4335')
        ax2.set_xlabel("Diferencia en Área (km²)")
        ax2.set_title("Top 10: Mayor diferencia en área de techos", fontweight='bold')
        ax2.grid(axis='x', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / "comparacion_datasets.png", dpi=300, bbox_inches='tight')
        print("✅ Guardado: comparacion_datasets.png")
        plt.close()

def print_summary_stats(stats_ms, stats_gg, df_comparison):
    """Imprime resumen de estadísticas"""
    print("\n" + "="*70)
    print("📊 RESUMEN DE ESTADÍSTICAS - EDA INICIAL")
    print("="*70 + "\n")
    
    print("MICROSOFT BUILDING FOOTPRINTS")
    print("─"*70)
    print(f"Total de edificios:           {stats_ms['total_documents']:>15,}")
    print(f"Con municipio asignado:       {stats_ms['with_municipality']:>15,}")
    print(f"Sin municipio:                {stats_ms['without_municipality']:>15,}")
    if 'total_area_m2' in stats_ms:
        print(f"Área total:                   {stats_ms['total_area_m2']/1_000_000:>15,.2f} km²")
        print(f"Área promedio por edificio:   {stats_ms['avg_area_m2']:>15,.2f} m²")
    print()
    
    print("GOOGLE OPEN BUILDINGS")
    print("─"*70)
    print(f"Total de edificios:           {stats_gg['total_documents']:>15,}")
    print(f"Con municipio asignado:       {stats_gg['with_municipality']:>15,}")
    print(f"Sin municipio:                {stats_gg['without_municipality']:>15,}")
    if 'total_area_m2' in stats_gg:
        print(f"Área total:                   {stats_gg['total_area_m2']/1_000_000:>15,.2f} km²")
        print(f"Área promedio por edificio:   {stats_gg['avg_area_m2']:>15,.2f} m²")
    print()
    
    if not df_comparison.empty:
        print("COMPARACIÓN GENERAL")
        print("─"*70)
        total_ms = df_comparison["ms_buildings"].sum()
        total_gg = df_comparison["gg_buildings"].sum()
        area_ms = df_comparison["ms_area_km2"].sum()
        area_gg = df_comparison["gg_area_km2"].sum()
        
        print(f"Total edificios Microsoft:    {total_ms:>15,}")
        print(f"Total edificios Google:       {total_gg:>15,}")
        print(f"Diferencia:                   {total_gg - total_ms:>15,} ({((total_gg/total_ms - 1)*100):+.1f}%)")
        print(f"\nÁrea total Microsoft:         {area_ms:>15,.2f} km²")
        print(f"Área total Google:            {area_gg:>15,.2f} km²")
        print(f"Diferencia:                   {area_gg - area_ms:>15,.2f} km² ({((area_gg/area_ms - 1)*100):+.1f}%)")
    
    print("\n" + "="*70)

def export_results(df_ms_top, df_gg_top, df_comparison):
    """Exporta resultados a CSV"""
    print("\n💾 Exportando resultados a CSV...\n")
    
    if not df_ms_top.empty:
        df_ms_top.to_csv(OUTPUT_DIR / "top_municipios_microsoft.csv", index=False)
        print("✅ Guardado: top_municipios_microsoft.csv")
    
    if not df_gg_top.empty:
        df_gg_top.to_csv(OUTPUT_DIR / "top_municipios_google.csv", index=False)
        print("✅ Guardado: top_municipios_google.csv")
    
    if not df_comparison.empty:
        df_comparison.to_csv(OUTPUT_DIR / "comparacion_completa.csv", index=False)
        print("✅ Guardado: comparacion_completa.csv")

def main():
    """Función principal del EDA"""
    
    print("\n" + "="*70)
    print("🔍 ANÁLISIS EXPLORATORIO DE DATOS (EDA)")
    print("Edificios en Territorios PDET")
    print("="*70)
    
    # Conectar a MongoDB
    print("\n📡 Conectando a MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    # 1. Estadísticas básicas
    print("\n📊 Recopilando estadísticas básicas...")
    stats_ms = get_collection_stats(db, "buildings_microsoft")
    stats_gg = get_collection_stats(db, "buildings_google")
    
    # 2. Top municipios
    print("\n🏆 Identificando top municipios...")
    df_ms_top = get_top_municipalities(db, "buildings_microsoft", top_n=20)
    df_gg_top = get_top_municipalities(db, "buildings_google", top_n=20)
    
    # 3. Comparación de datasets
    print("\n⚖️  Comparando datasets...")
    df_comparison = compare_datasets(db)
    
    # 4. Generar visualizaciones
    generate_visualizations(df_ms_top, df_gg_top, df_comparison)
    
    # 5. Exportar resultados
    export_results(df_ms_top, df_gg_top, df_comparison)
    
    # 6. Imprimir resumen
    print_summary_stats(stats_ms, stats_gg, df_comparison)
    
    # Cerrar conexión
    client.close()
    
    print("\n✅ EDA completado exitosamente!")
    print(f"📁 Resultados guardados en: {OUTPUT_DIR.absolute()}\n")

if __name__ == "__main__":
    main()
