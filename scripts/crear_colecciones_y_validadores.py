"""
Script para crear colecciones en MongoDB con validación de esquemas
y creación de índices espaciales para el proyecto PDET Solar.
"""

from pymongo import MongoClient, GEOSPHERE
from pymongo.errors import CollectionInvalid
import sys

# Configuración de conexión
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "pdet_solar"

def create_collections_and_indexes():
    """
    Crea las colecciones con validación de esquemas e índices espaciales
    """
    try:
        # Conectar a MongoDB
        print(f"🔌 Conectando a MongoDB en {MONGO_URI}...")
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        
        print(f"✅ Conectado a base de datos: {DATABASE_NAME}\n")
        
        # ======================================================================
        # COLECCIÓN: municipalities
        # ======================================================================
        print("📍 Creando colección: municipalities")
        
        municipalities_validator = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["codigo_dane", "nombre", "departamento", "geometry"],
                "properties": {
                    "codigo_dane": {
                        "bsonType": "string",
                        "description": "Código DANE del municipio (5 dígitos)"
                    },
                    "nombre": {
                        "bsonType": "string",
                        "description": "Nombre del municipio"
                    },
                    "departamento": {
                        "bsonType": "string",
                        "description": "Nombre del departamento"
                    },
                    "codigo_departamento": {
                        "bsonType": "string",
                        "description": "Código DANE del departamento (2 dígitos)"
                    },
                    "is_pdet": {
                        "bsonType": "bool",
                        "description": "Indica si es municipio PDET"
                    },
                    "geometry": {
                        "bsonType": "object",
                        "required": ["type", "coordinates"],
                        "properties": {
                            "type": {
                                "enum": ["Polygon", "MultiPolygon"],
                                "description": "Tipo de geometría GeoJSON"
                            },
                            "coordinates": {
                                "bsonType": "array",
                                "description": "Coordenadas de la geometría"
                            }
                        }
                    }
                }
            }
        }
        
        try:
            db.create_collection("municipalities", validator=municipalities_validator)
            print("  ✅ Colección 'municipalities' creada")
        except CollectionInvalid:
            print("  ⚠️  Colección 'municipalities' ya existe")
        
        # Crear índices para municipalities
        print("  📊 Creando índices espaciales...")
        db.municipalities.create_index([("geometry", GEOSPHERE)], name="geometry_2dsphere")
        db.municipalities.create_index("codigo_dane", unique=True, name="codigo_dane_unique")
        db.municipalities.create_index("is_pdet", name="is_pdet_index")
        print("  ✅ Índices creados para 'municipalities'\n")
        
        # ======================================================================
        # COLECCIÓN: buildings_microsoft
        # ======================================================================
        print("🏢 Creando colección: buildings_microsoft")
        
        buildings_ms_validator = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["geometry", "area_m2", "source"],
                "properties": {
                    "municipality_code": {
                        "bsonType": "string",
                        "description": "Código DANE del municipio al que pertenece"
                    },
                    "municipality_name": {
                        "bsonType": "string",
                        "description": "Nombre del municipio"
                    },
                    "geometry": {
                        "bsonType": "object",
                        "required": ["type", "coordinates"],
                        "properties": {
                            "type": {
                                "enum": ["Polygon", "MultiPolygon"],
                                "description": "Tipo de geometría GeoJSON"
                            },
                            "coordinates": {
                                "bsonType": "array",
                                "description": "Coordenadas del edificio"
                            }
                        }
                    },
                    "area_m2": {
                        "bsonType": "double",
                        "minimum": 0,
                        "description": "Área del techo en metros cuadrados"
                    },
                    "source": {
                        "enum": ["Microsoft"],
                        "description": "Fuente de los datos"
                    },
                    "confidence": {
                        "bsonType": "double",
                        "minimum": 0,
                        "maximum": 1,
                        "description": "Nivel de confianza de la detección"
                    },
                    "capture_date": {
                        "bsonType": "date",
                        "description": "Fecha de captura de la imagen"
                    }
                }
            }
        }
        
        try:
            db.create_collection("buildings_microsoft", validator=buildings_ms_validator)
            print("  ✅ Colección 'buildings_microsoft' creada")
        except CollectionInvalid:
            print("  ⚠️  Colección 'buildings_microsoft' ya existe")
        
        # Crear índices para buildings_microsoft
        print("  📊 Creando índices...")
        db.buildings_microsoft.create_index([("geometry", GEOSPHERE)], name="geometry_2dsphere")
        db.buildings_microsoft.create_index("municipality_code", name="municipality_code_index")
        db.buildings_microsoft.create_index("area_m2", name="area_m2_index")
        print("  ✅ Índices creados para 'buildings_microsoft'\n")
        
        # ======================================================================
        # COLECCIÓN: buildings_google
        # ======================================================================
        print("🏗️  Creando colección: buildings_google")
        
        buildings_gg_validator = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["geometry", "area_m2", "source"],
                "properties": {
                    "municipality_code": {
                        "bsonType": "string",
                        "description": "Código DANE del municipio al que pertenece"
                    },
                    "municipality_name": {
                        "bsonType": "string",
                        "description": "Nombre del municipio"
                    },
                    "geometry": {
                        "bsonType": "object",
                        "required": ["type", "coordinates"],
                        "properties": {
                            "type": {
                                "enum": ["Polygon", "MultiPolygon"],
                                "description": "Tipo de geometría GeoJSON"
                            },
                            "coordinates": {
                                "bsonType": "array",
                                "description": "Coordenadas del edificio"
                            }
                        }
                    },
                    "area_m2": {
                        "bsonType": "double",
                        "minimum": 0,
                        "description": "Área del techo en metros cuadrados"
                    },
                    "source": {
                        "enum": ["Google"],
                        "description": "Fuente de los datos"
                    },
                    "confidence": {
                        "bsonType": "double",
                        "minimum": 0,
                        "maximum": 1,
                        "description": "Nivel de confianza de la detección"
                    },
                    "full_plus_code": {
                        "bsonType": "string",
                        "description": "Plus Code completo del edificio"
                    }
                }
            }
        }
        
        try:
            db.create_collection("buildings_google", validator=buildings_gg_validator)
            print("  ✅ Colección 'buildings_google' creada")
        except CollectionInvalid:
            print("  ⚠️  Colección 'buildings_google' ya existe")
        
        # Crear índices para buildings_google
        print("  📊 Creando índices...")
        db.buildings_google.create_index([("geometry", GEOSPHERE)], name="geometry_2dsphere")
        db.buildings_google.create_index("municipality_code", name="municipality_code_index")
        db.buildings_google.create_index("area_m2", name="area_m2_index")
        print("  ✅ Índices creados para 'buildings_google'\n")
        
        # ======================================================================
        # RESUMEN
        # ======================================================================
        print("="*70)
        print("✨ RESUMEN DE COLECCIONES CREADAS")
        print("="*70)
        
        collections = db.list_collection_names()
        print(f"\n📚 Colecciones en '{DATABASE_NAME}':")
        for col in collections:
            count = db[col].count_documents({})
            indexes = list(db[col].list_indexes())  # FIX: Convertir a lista
            print(f"  • {col:25} | Documentos: {count:6} | Índices: {len(indexes)}")
        
        print("\n" + "="*70)
        print("🎉 Proceso completado exitosamente!")
        print("="*70)
        
        # Cerrar conexión
        client.close()
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def verify_indexes():
    """
    Verifica que los índices espaciales estén correctamente creados
    """
    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        
        print("\n🔍 Verificando índices espaciales...")
        print("="*70)
        
        collections = ["municipalities", "buildings_microsoft", "buildings_google"]
        
        for col_name in collections:
            print(f"\n📋 Colección: {col_name}")
            indexes = list(db[col_name].list_indexes())
            
            for idx in indexes:
                print(f"  • {idx['name']}")
                if 'key' in idx:
                    for key, val in idx['key'].items():
                        if val == '2dsphere':
                            print(f"    ✅ Índice espacial 2dsphere en campo '{key}'")
        
        print("\n" + "="*70)
        client.close()
        
    except Exception as e:
        print(f"❌ Error al verificar índices: {e}")

def main():
    """
    Función principal
    """
    print("\n" + "="*70)
    print("🚀 CREACIÓN DE ESQUEMA MONGODB - PROYECTO PDET SOLAR")
    print("="*70 + "\n")
    
    # Crear colecciones e índices
    success = create_collections_and_indexes()
    
    if success:
        # Verificar índices
        verify_indexes()
        
        print("\n✅ El esquema de MongoDB está listo para usar")
        print("\n💡 Próximos pasos:")
        print("  1. Cargar municipios PDET: python3 scripts/cargar_municipios_desde_geojson.py")
        print("  2. Ejecutar pipeline de datos: cd entrega3/loaders && python3 05_cargar_edificios_a_mongodb.py")
        print("  3. Ejecutar EDA: cd entrega3/eda && python3 01_analisis_rapido_top_municipios_y_area.py")
        
        return 0
    else:
        print("\n❌ Hubo errores al crear el esquema")
        return 1

if __name__ == "__main__":
    sys.exit(main())
