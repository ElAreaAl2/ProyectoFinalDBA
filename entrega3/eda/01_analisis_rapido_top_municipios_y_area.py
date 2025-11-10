"""
Análisis Exploratorio de Datos (EDA) - Edificios PDET
Versión corregida para Entrega 3
"""

from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os

# Configuración
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "pdet_solar"  # Base de datos correcta
OUTPUT_DIR = "."  # Guardar en la carpeta actual (entrega3/eda)

# Estilo de gráficos
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

def conectar_mongodb():
    """Conecta a MongoDB y retorna la base de datos"""
    print("🔌 Conectando a MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return client, db

def obtener_estadisticas_generales(db):
    """Obtiene estadísticas generales de las colecciones"""
    print("\n📊 Recopilando estadísticas generales...")
    
    stats = {
        'municipios_pdet': db.municipalities.count_documents({'is_pdet': True}),
        'edificios_microsoft': db.buildings_microsoft.count_documents({}),
        'edificios_google': db.buildings_google.count_documents({})
    }
    
    # Área total Microsoft
    pipeline_ms = [
        {"$group": {
            "_id": None,
            "total_area": {"$sum": "$area_m2"},
            "avg_area": {"$avg": "$area_m2"}
        }}
    ]
    
    result_ms = list(db.buildings_microsoft.aggregate(pipeline_ms))
    if result_ms:
        stats['area_total_ms_m2'] = result_ms[0]['total_area']
        stats['area_promedio_ms_m2'] = result_ms[0]['avg_area']
    else:
        stats['area_total_ms_m2'] = 0
        stats['area_promedio_ms_m2'] = 0
    
    # Área total Google (si existe)
    result_gg = list(db.buildings_google.aggregate(pipeline_ms))
    if result_gg:
        stats['area_total_gg_m2'] = result_gg[0]['total_area']
        stats['area_promedio_gg_m2'] = result_gg[0]['avg_area']
    else:
        stats['area_total_gg_m2'] = 0
        stats['area_promedio_gg_m2'] = 0
    
    return stats

def obtener_top_municipios_microsoft(db, top_n=15):
    """Obtiene los top N municipios por área total - Microsoft"""
    print(f"\n🏆 Obteniendo top {top_n} municipios (Microsoft)...")
    
    pipeline = [
        {
            "$group": {
                "_id": "$municipality_code",
                "municipio": {"$first": "$municipality_name"},
                "departamento": {"$first": "$department"},
                "num_edificios": {"$sum": 1},
                "area_total_m2": {"$sum": "$area_m2"},
                "area_promedio_m2": {"$avg": "$area_m2"}
            }
        },
        {
            "$sort": {"area_total_m2": -1}
        },
        {
            "$limit": top_n
        }
    ]
    
    results = list(db.buildings_microsoft.aggregate(pipeline))
    
    if results:
        df = pd.DataFrame(results)
        df['area_total_km2'] = df['area_total_m2'] / 1_000_000
        df['fuente'] = 'Microsoft'
        return df
    
    return pd.DataFrame()

def obtener_top_municipios_google(db, top_n=15):
    """Obtiene los top N municipios por área total - Google"""
    print(f"\n🏆 Obteniendo top {top_n} municipios (Google)...")
    
    # Verificar si hay datos de Google
    if db.buildings_google.count_documents({}) == 0:
        print("   ⚠️  No hay datos de Google Open Buildings")
        return pd.DataFrame()
    
    pipeline = [
        {
            "$group": {
                "_id": "$municipality_code",
                "municipio": {"$first": "$municipality_name"},
                "departamento": {"$first": "$department"},
                "num_edificios": {"$sum": 1},
                "area_total_m2": {"$sum": "$area_m2"},
                "area_promedio_m2": {"$avg": "$area_m2"}
            }
        },
        {
            "$sort": {"area_total_m2": -1}
        },
        {
            "$limit": top_n
        }
    ]
    
    results = list(db.buildings_google.aggregate(pipeline))
    
    if results:
        df = pd.DataFrame(results)
        df['area_total_km2'] = df['area_total_m2'] / 1_000_000
        df['fuente'] = 'Google'
        return df
    
    return pd.DataFrame()

def generar_visualizaciones(df_ms, df_gg, stats):
    """Genera las visualizaciones del análisis"""
    print("\n📈 Generando visualizaciones...")
    
    # 1. Gráfico Microsoft
    if not df_ms.empty:
        plt.figure(figsize=(14, 8))
        df_plot = df_ms.sort_values('area_total_km2', ascending=True)
        
        plt.barh(range(len(df_plot)), df_plot['area_total_km2'], color='#00A4EF')
        plt.yticks(range(len(df_plot)), 
                   [f"{row['municipio']}, {row['departamento']}" 
                    for _, row in df_plot.iterrows()])
        
        plt.xlabel('Área Total de Techos (km²)', fontsize=12)
        plt.title('Top 15 Municipios PDET - Área de Techos Disponibles\nMicrosoft Building Footprints', 
                  fontsize=14, fontweight='bold', pad=20)
        plt.grid(axis='x', alpha=0.3)
        
        # Añadir valores en las barras
        for i, (_, row) in enumerate(df_plot.iterrows()):
            plt.text(row['area_total_km2'] + 0.01, i, 
                    f"{row['area_total_km2']:.2f} km²", 
                    va='center', fontsize=9)
        
        plt.tight_layout()
        plt.savefig('top_15_municipios_microsoft.png', dpi=300, bbox_inches='tight')
        print("   ✅ Guardado: top_15_municipios_microsoft.png")
        plt.close()
    
    # 2. Gráfico Google (si hay datos)
    if not df_gg.empty:
        plt.figure(figsize=(14, 8))
        df_plot = df_gg.sort_values('area_total_km2', ascending=True)
        
        plt.barh(range(len(df_plot)), df_plot['area_total_km2'], color='#34A853')
        plt.yticks(range(len(df_plot)), 
                   [f"{row['municipio']}, {row['departamento']}" 
                    for _, row in df_plot.iterrows()])
        
        plt.xlabel('Área Total de Techos (km²)', fontsize=12)
        plt.title('Top 15 Municipios PDET - Área de Techos Disponibles\nGoogle Open Buildings', 
                  fontsize=14, fontweight='bold', pad=20)
        plt.grid(axis='x', alpha=0.3)
        
        for i, (_, row) in enumerate(df_plot.iterrows()):
            plt.text(row['area_total_km2'] + 0.01, i, 
                    f"{row['area_total_km2']:.2f} km²", 
                    va='center', fontsize=9)
        
        plt.tight_layout()
        plt.savefig('top_15_municipios_google.png', dpi=300, bbox_inches='tight')
        print("   ✅ Guardado: top_15_municipios_google.png")
        plt.close()
    
    # 3. Comparación (si hay ambos datasets)
    if not df_ms.empty and not df_gg.empty:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Número de edificios
        ax1.bar(['Microsoft', 'Google'], 
                [stats['edificios_microsoft'], stats['edificios_google']],
                color=['#00A4EF', '#34A853'])
        ax1.set_ylabel('Número de Edificios', fontsize=12)
        ax1.set_title('Comparación: Total de Edificios', fontsize=12, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)
        
        for i, v in enumerate([stats['edificios_microsoft'], stats['edificios_google']]):
            ax1.text(i, v + 50, f"{v:,}", ha='center', fontsize=10, fontweight='bold')
        
        # Área total
        ax2.bar(['Microsoft', 'Google'], 
                [stats['area_total_ms_m2']/1_000_000, stats['area_total_gg_m2']/1_000_000],
                color=['#00A4EF', '#34A853'])
        ax2.set_ylabel('Área Total (km²)', fontsize=12)
        ax2.set_title('Comparación: Área Total de Techos', fontsize=12, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)
        
        for i, v in enumerate([stats['area_total_ms_m2']/1_000_000, 
                               stats['area_total_gg_m2']/1_000_000]):
            ax2.text(i, v + 0.005, f"{v:.3f} km²", ha='center', fontsize=10, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig('comparacion_fuentes.png', dpi=300, bbox_inches='tight')
        print("   ✅ Guardado: comparacion_fuentes.png")
        plt.close()

def exportar_resultados(df_ms, df_gg, stats):
    """Exporta los resultados a archivos CSV"""
    print("\n💾 Exportando resultados a CSV...")
    
    # Estadísticas generales
    df_stats = pd.DataFrame([stats])
    df_stats.to_csv('estadisticas_generales.csv', index=False)
    print("   ✅ Guardado: estadisticas_generales.csv")
    
    # Top municipios Microsoft
    if not df_ms.empty:
        df_ms.to_csv('top_municipios_microsoft.csv', index=False)
        print("   ✅ Guardado: top_municipios_microsoft.csv")
    
    # Top municipios Google
    if not df_gg.empty:
        df_gg.to_csv('top_municipios_google.csv', index=False)
        print("   ✅ Guardado: top_municipios_google.csv")

def imprimir_resumen(stats, df_ms, df_gg):
    """Imprime un resumen de los resultados"""
    print("\n" + "="*70)
    print("📋 RESUMEN DEL ANÁLISIS EXPLORATORIO")
    print("="*70 + "\n")
    
    print("DATOS CARGADOS:")
    print(f"  • Municipios PDET: {stats['municipios_pdet']}")
    print(f"  • Edificios Microsoft: {stats['edificios_microsoft']:,}")
    print(f"  • Edificios Google: {stats['edificios_google']:,}")
    
    print("\nÁREAS TOTALES:")
    print(f"  • Microsoft: {stats['area_total_ms_m2']/1_000_000:.3f} km²")
    print(f"  • Google: {stats['area_total_gg_m2']/1_000_000:.3f} km²")
    
    print("\nÁREAS PROMEDIO POR EDIFICIO:")
    print(f"  • Microsoft: {stats['area_promedio_ms_m2']:.2f} m²")
    print(f"  • Google: {stats['area_promedio_gg_m2']:.2f} m²")
    
    if not df_ms.empty:
        print(f"\nTOP 3 MUNICIPIOS (MICROSOFT):")
        for i, row in df_ms.head(3).iterrows():
            print(f"  {i+1}. {row['municipio']}, {row['departamento']}")
            print(f"     • Edificios: {row['num_edificios']:,}")
            print(f"     • Área total: {row['area_total_km2']:.3f} km²")
    
    print("\n" + "="*70)

def main():
    """Función principal"""
    print("\n" + "="*70)
    print("🚀 ANÁLISIS EXPLORATORIO DE DATOS (EDA)")
    print("   Edificios en Territorios PDET - Colombia")
    print("="*70)
    
    # Conectar a MongoDB
    client, db = conectar_mongodb()
    
    try:
        # Obtener estadísticas
        stats = obtener_estadisticas_generales(db)
        
        # Obtener top municipios
        df_ms = obtener_top_municipios_microsoft(db, top_n=15)
        df_gg = obtener_top_municipios_google(db, top_n=15)
        
        # Generar visualizaciones
        generar_visualizaciones(df_ms, df_gg, stats)
        
        # Exportar resultados
        exportar_resultados(df_ms, df_gg, stats)
        
        # Imprimir resumen
        imprimir_resumen(stats, df_ms, df_gg)
        
        print("\n✅ EDA completado exitosamente!")
        print(f"📁 Archivos generados en: {os.getcwd()}")
        print("\n💡 Siguiente paso:")
        print("   cd ..")
        print("   python3 validar_entrega.py")
        
    finally:
        client.close()
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
