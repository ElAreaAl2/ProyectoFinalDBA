"""
MongoDB Collection Creation Script with JSON Schema Validation
This script creates the three main collections for the PDET Solar Potential project
with appropriate validation rules and indexes.

Author: Database Administration Final Project
Date: October 27, 2025
"""

from pymongo import MongoClient, ASCENDING, DESCENDING, GEOSPHERE
from pymongo.errors import CollectionInvalid
import json

# MongoDB connection configuration
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "pdet_solar_analysis"

def create_municipalities_collection(db):
    """
    Create municipalities collection with validation and indexes
    """
    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["codigo_dane", "nombre_municipio", "nombre_departamento", 
                        "is_pdet", "geometry", "metadata"],
            "properties": {
                "codigo_dane": {
                    "bsonType": "string",
                    "pattern": "^[0-9]{5}$",
                    "description": "5-digit DANE municipal code"
                },
                "nombre_municipio": {
                    "bsonType": "string",
                    "minLength": 1,
                    "maxLength": 100
                },
                "nombre_departamento": {
                    "bsonType": "string",
                    "minLength": 1,
                    "maxLength": 100
                },
                "is_pdet": {
                    "bsonType": "bool"
                },
                "geometry": {
                    "bsonType": "object",
                    "required": ["type", "coordinates"],
                    "properties": {
                        "type": {
                            "enum": ["Polygon", "MultiPolygon"]
                        },
                        "coordinates": {
                            "bsonType": "array"
                        }
                    }
                },
                "area_km2": {
                    "bsonType": "double",
                    "minimum": 0
                },
                "metadata": {
                    "bsonType": "object",
                    "required": ["source", "load_date"],
                    "properties": {
                        "source": {
                            "bsonType": "string"
                        },
                        "load_date": {
                            "bsonType": "date"
                        }
                    }
                }
            }
        }
    }
    
    try:
        db.create_collection(
            "municipalities",
            validator=validator,
            validationLevel="strict",
            validationAction="error"
        )
        print("✓ Created collection: municipalities")
    except CollectionInvalid:
        print("! Collection 'municipalities' already exists")
    
    # Create indexes
    collection = db.municipalities
    
    # Unique index on codigo_dane
    collection.create_index(
        [("codigo_dane", ASCENDING)],
        unique=True,
        name="codigo_dane_unique"
    )
    print("  ✓ Created unique index on codigo_dane")
    
    # 2dsphere spatial index on geometry
    collection.create_index(
        [("geometry", GEOSPHERE)],
        name="geometry_2dsphere"
    )
    print("  ✓ Created 2dsphere index on geometry")
    
    # Index on is_pdet for filtering
    collection.create_index(
        [("is_pdet", ASCENDING)],
        name="is_pdet_index"
    )
    print("  ✓ Created index on is_pdet")
    
    # Text index for municipality name search
    collection.create_index(
        [("nombre_municipio", ASCENDING)],
        name="nombre_municipio_index"
    )
    print("  ✓ Created index on nombre_municipio")


def create_buildings_microsoft_collection(db):
    """
    Create buildings_microsoft collection with validation and indexes
    """
    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["municipality_code", "geometry", "properties", "metadata"],
            "properties": {
                "municipality_code": {
                    "bsonType": "string",
                    "pattern": "^[0-9]{5}$"
                },
                "geometry": {
                    "bsonType": "object",
                    "required": ["type", "coordinates"],
                    "properties": {
                        "type": {
                            "enum": ["Polygon"]
                        },
                        "coordinates": {
                            "bsonType": "array"
                        }
                    }
                },
                "properties": {
                    "bsonType": "object",
                    "required": ["area_m2"],
                    "properties": {
                        "area_m2": {
                            "bsonType": "double",
                            "minimum": 0
                        },
                        "confidence": {
                            "bsonType": "double",
                            "minimum": 0,
                            "maximum": 1
                        }
                    }
                },
                "metadata": {
                    "bsonType": "object",
                    "required": ["source", "load_date"],
                    "properties": {
                        "source": {
                            "enum": ["Microsoft"]
                        },
                        "load_date": {
                            "bsonType": "date"
                        }
                    }
                }
            }
        }
    }
    
    try:
        db.create_collection(
            "buildings_microsoft",
            validator=validator,
            validationLevel="strict",
            validationAction="error"
        )
        print("✓ Created collection: buildings_microsoft")
    except CollectionInvalid:
        print("! Collection 'buildings_microsoft' already exists")
    
    # Create indexes
    collection = db.buildings_microsoft
    
    # 2dsphere spatial index
    collection.create_index(
        [("geometry", GEOSPHERE)],
        name="geometry_2dsphere"
    )
    print("  ✓ Created 2dsphere index on geometry")
    
    # Index on municipality_code
    collection.create_index(
        [("municipality_code", ASCENDING)],
        name="municipality_code_index"
    )
    print("  ✓ Created index on municipality_code")
    
    # Compound index for municipality + area queries
    collection.create_index(
        [("municipality_code", ASCENDING), ("properties.area_m2", DESCENDING)],
        name="municipality_area_compound"
    )
    print("  ✓ Created compound index on municipality_code + area_m2")
    
    # Index on area for sorting/filtering
    collection.create_index(
        [("properties.area_m2", DESCENDING)],
        name="area_index"
    )
    print("  ✓ Created index on area_m2")


def create_buildings_google_collection(db):
    """
    Create buildings_google collection with validation and indexes
    """
    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["municipality_code", "geometry", "properties", "metadata"],
            "properties": {
                "municipality_code": {
                    "bsonType": "string",
                    "pattern": "^[0-9]{5}$"
                },
                "geometry": {
                    "bsonType": "object",
                    "required": ["type", "coordinates"],
                    "properties": {
                        "type": {
                            "enum": ["Polygon"]
                        },
                        "coordinates": {
                            "bsonType": "array"
                        }
                    }
                },
                "properties": {
                    "bsonType": "object",
                    "required": ["area_m2", "confidence"],
                    "properties": {
                        "area_m2": {
                            "bsonType": "double",
                            "minimum": 0
                        },
                        "confidence": {
                            "bsonType": "double",
                            "minimum": 0,
                            "maximum": 1
                        }
                    }
                },
                "metadata": {
                    "bsonType": "object",
                    "required": ["source", "load_date", "dataset_version"],
                    "properties": {
                        "source": {
                            "enum": ["Google"]
                        },
                        "dataset_version": {
                            "bsonType": "string",
                            "pattern": "^v[0-9]+$"
                        },
                        "load_date": {
                            "bsonType": "date"
                        }
                    }
                }
            }
        }
    }
    
    try:
        db.create_collection(
            "buildings_google",
            validator=validator,
            validationLevel="strict",
            validationAction="error"
        )
        print("✓ Created collection: buildings_google")
    except CollectionInvalid:
        print("! Collection 'buildings_google' already exists")
    
    # Create indexes
    collection = db.buildings_google
    
    # 2dsphere spatial index
    collection.create_index(
        [("geometry", GEOSPHERE)],
        name="geometry_2dsphere"
    )
    print("  ✓ Created 2dsphere index on geometry")
    
    # Index on municipality_code
    collection.create_index(
        [("municipality_code", ASCENDING)],
        name="municipality_code_index"
    )
    print("  ✓ Created index on municipality_code")
    
    # Compound index for municipality + area queries
    collection.create_index(
        [("municipality_code", ASCENDING), ("properties.area_m2", DESCENDING)],
        name="municipality_area_compound"
    )
    print("  ✓ Created compound index on municipality_code + area_m2")
    
    # Index on confidence score
    collection.create_index(
        [("properties.confidence", DESCENDING)],
        name="confidence_index"
    )
    print("  ✓ Created index on confidence")
    
    # Index on area
    collection.create_index(
        [("properties.area_m2", DESCENDING)],
        name="area_index"
    )
    print("  ✓ Created index on area_m2")


def print_database_info(db):
    """
    Print information about the created database and collections
    """
    print("\n" + "="*60)
    print(f"Database: {db.name}")
    print("="*60)
    
    collections = db.list_collection_names()
    print(f"\nCollections ({len(collections)}):")
    for coll_name in collections:
        coll = db[coll_name]
        count = coll.count_documents({})
        indexes = list(coll.list_indexes())
        print(f"\n  📁 {coll_name}")
        print(f"     Documents: {count}")
        print(f"     Indexes: {len(indexes)}")
        for idx in indexes:
            print(f"       - {idx['name']}")


def main():
    """
    Main function to create database and collections
    """
    print("\n" + "="*60)
    print("MongoDB Schema Setup - PDET Solar Potential Analysis")
    print("="*60 + "\n")
    
    # Connect to MongoDB
    print(f"Connecting to MongoDB at {MONGO_URI}...")
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    print(f"✓ Connected to database: {DATABASE_NAME}\n")
    
    # Create collections
    print("Creating collections with validation...\n")
    create_municipalities_collection(db)
    print()
    create_buildings_microsoft_collection(db)
    print()
    create_buildings_google_collection(db)
    
    # Print database info
    print_database_info(db)
    
    print("\n" + "="*60)
    print("✓ Database schema setup completed successfully!")
    print("="*60 + "\n")
    
    # Close connection
    client.close()


if __name__ == "__main__":
    main()
