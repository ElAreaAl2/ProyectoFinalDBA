# 🌞 Análisis Geoespacial de Potencial Solar en Territorios PDET

**Proyecto Final - Administración de Bases de Datos**  
Pontificia Universidad Javeriana Bogotá | Octubre - Noviembre 2025

[![MongoDB](https://img.shields.io/badge/MongoDB-7.0+-green.svg)](https://www.mongodb.com/)
[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-Academic-yellow.svg)]()

---

## 📋 Descripción del Proyecto

Este proyecto apoya la iniciativa de la **UPME** (Unidad de Planeación Minero Energética de Colombia) para identificar ubicaciones potenciales para proyectos de energía solar en territorios PDET (Programas de Desarrollo con Enfoque Territorial).

### Objetivos Principales

1. **Diseñar e implementar** una solución NoSQL escalable para almacenar datos geoespaciales masivos
2. **Integrar y comparar** dos datasets abiertos de edificaciones:
   - 🏢 Microsoft Building Footprints (999M edificios)
   - 🏘️ Google Open Buildings (1.8B edificios)
3. **Analizar espacialmente** el número de edificios y área total de techos por municipio PDET
4. **Generar recomendaciones** para ubicaciones óptimas de proyectos piloto de energía solar

---

## 🗄️ Tecnología NoSQL Seleccionada: MongoDB

### Justificación Técnica

**MongoDB** ha sido seleccionado como la solución NoSQL por las siguientes razones:

✅ **Soporte Geoespacial Nativo**
- Índices espaciales `2dsphere` para consultas en superficies esféricas
- Operadores nativos: `$geoWithin`, `$geoIntersects`, `$near`
- Formato GeoJSON estándar

✅ **Escalabilidad Horizontal**
- Sharding automático para distribuir billones de documentos
- Replicación nativa para alta disponibilidad

✅ **Modelo de Documentos Flexible**
- Esquema adaptable para datasets heterogéneos
- Facilita comparación entre Microsoft y Google

✅ **Framework de Agregación Potente**
- Pipelines optimizados para cálculos estadísticos complejos
- Agrupaciones eficientes por municipio

✅ **Ecosistema Robusto**
- Integración con Python (PyMongo, Motor)
- Herramientas de visualización (Compass, Charts)

---

## 🏗️ Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────┐
│           Fuentes de Datos Externas                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  Microsoft   │  │    Google    │  │  DANE MGN    │      │
│  │  Buildings   │  │  Buildings   │  │ (Municipios) │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────┐
│          Capa de Ingesta de Datos (Python)                  │
│  • ETL Pipeline  • Validación Geometrías  • Bulk Loader    │
└─────────────────────────────────────────────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                  MongoDB Database                           │
│  ┌──────────────────┐  ┌──────────────────┐                │
│  │  municipalities  │  │ buildings_ms/gg  │                │
│  │   (~170 docs)    │  │  (~2.8B docs)    │                │
│  │  [2dsphere idx]  │  │  [2dsphere idx]  │                │
│  └──────────────────┘  └──────────────────┘                │
└─────────────────────────────────────────────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────┐
│         Capa de Procesamiento y Análisis                    │
│  • Consultas Espaciales  • Agregaciones  • Comparaciones   │
└─────────────────────────────────────────────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────┐
│              Presentación de Resultados                     │
│  • Reportes Técnicos  • Mapas  • Visualizaciones          │
└─────────────────────────────────────────────────────────────┘
```

---

## 📊 Modelo de Datos NoSQL

### Colecciones Principales

#### 1. `municipalities`
Almacena límites administrativos de municipios PDET (DANE MGN)

```json
{
  "_id": ObjectId("..."),
  "codigo_dane": "05001",
  "nombre_municipio": "Medellín",
  "nombre_departamento": "Antioquia",
  "is_pdet": true,
  "geometry": {
    "type": "Polygon",
    "coordinates": [[[-75.6068, 6.2442], ...]]
  },
  "area_km2": 105.2,
  "metadata": { ... }
}
```

**Índices**: `codigo_dane` (unique), `geometry` (2dsphere), `is_pdet`

#### 2. `buildings_microsoft`
Edificios del dataset Microsoft Building Footprints

```json
{
  "_id": ObjectId("..."),
  "municipality_code": "05001",
  "geometry": {
    "type": "Polygon",
    "coordinates": [[[-75.6050, 6.2450], ...]]
  },
  "properties": {
    "area_m2": 156.8,
    "confidence": 0.95
  },
  "metadata": { ... }
}
```

**Índices**: `geometry` (2dsphere), `municipality_code`, compound index

#### 3. `buildings_google`
Edificios del dataset Google Open Buildings (v3)

```json
{
  "_id": ObjectId("..."),
  "municipality_code": "05001",
  "geometry": {
    "type": "Polygon",
    "coordinates": [[[-75.6052, 6.2451], ...]]
  },
  "properties": {
    "area_m2": 148.3,
    "confidence": 0.87
  },
  "metadata": { ... }
}
```

**Índices**: `geometry` (2dsphere), `municipality_code`, `confidence`

---

## 📁 Estructura del Proyecto

```
ProyectoFinalDBA/
├── README.md                          # Este archivo
├── Project.pdf                        # Especificaciones del proyecto
│
├── entrega1/                          # 📦 ENTREGA 1 (27 Oct)
│   ├── docs/
│   │   └── implementation_plan.tex   # Plan de implementación (LaTeX)
│   ├── diagrams/
│   │   ├── arquitectura del sistema.png         # Diagrama de arquitectura
│   │   ├── Modelo de Datos NoSQL.png           # Modelo de datos NoSQL
│   │   ├── Flujo de Trabajo - Procesamiento Geoespacial.png             # Flujo de procesamiento
│   │   └── Diagrama Secuencia Consulta Espacial.png       # Secuencia de consultas espaciales
│   └── schemas/
│       ├── municipalities_schema.json
│       ├── buildings_microsoft_schema.json
│       ├── buildings_google_schema.json
│       ├── create_collections.py     # Script de creación de colecciones
│       └── README.md
│
├── entrega2/                          # 📦 ENTREGA 2 (3 Nov)
│   └── (Integración municipios PDET)
│
├── entrega3/                          # 📦 ENTREGA 3 (10 Nov)
│   └── (Carga de edificios)
│
├── entrega4/                          # 📦 ENTREGA 4 (17 Nov)
│   └── (Workflow geoespacial)
│
└── entrega5/                          # 📦 ENTREGA 5 (24 Nov)
    └── (Reporte final)
```

---

## 🚀 Cronograma de Entregas

| Fecha | Entrega | Contenido |
|-------|---------|-----------|
| **27 Oct** | **Entrega 1** | ✅ Diseño de esquema NoSQL y plan de implementación |
| **3 Nov** | Entrega 2 | Integración de municipios PDET (DANE MGN) |
| **10 Nov** | Entrega 3 | Carga de datasets Microsoft y Google |
| **17 Nov** | Entrega 4 | Workflow geoespacial reproducible |
| **24 Nov** | Entrega 5 | Reporte técnico final y recomendaciones |

---

## 📖 Entrega 1: Diseño de Esquema NoSQL

### Contenido Entregado

#### 1. **Plan de Implementación** 

#### 2. **Diagramas** 

#### 3. **Esquemas JSON** (`entrega1/schemas/`)
Definiciones completas de esquemas con:
- Validación JSON Schema
- Especificación de índices
- Ejemplos de documentos
- Script Python para crear colecciones

**Para ejecutar:**
```bash
cd entrega1/schemas
pip install pymongo
python create_collections.py
```

---

## 🛠️ Stack Tecnológico

| Componente | Tecnología | Versión |
|------------|------------|---------|
| Base de Datos | MongoDB | 7.0+ |
| Lenguaje | Python | 3.11+ |
| Driver MongoDB | PyMongo | 4.5+ |
| Procesamiento Geo | GeoPandas, Shapely | 0.14+, 2.0+ |
| Análisis | Pandas, NumPy | 2.0+, 1.24+ |
| Visualización | Matplotlib, Folium | Latest |
| Documentación | LaTeX, Markdown | - |

---

## 📚 Fuentes de Datos

### 1. Microsoft Building Footprints
- **URL**: https://planetarycomputer.microsoft.com/dataset/ms-buildings
- **Tamaño**: 999M edificios
- **Cobertura**: Global (2014-2021)
- **Licencia**: ODbL

### 2. Google Open Buildings
- **URL**: https://sites.research.google/gr/open-buildings/
- **Tamaño**: 1.8B edificios
- **Cobertura**: América Latina (v3)
- **Licencia**: CC BY-4.0, ODbL v1.0

### 3. DANE Marco Geoestadístico Nacional (MGN)
- **URL**: https://geoportal.dane.gov.co/
- **Contenido**: Límites municipales de Colombia
- **Formato**: Shapefile, GeoJSON

---

## 👥 Información del Equipo

**Curso**: Administración de Bases de Datos  
**Instructor**: Dr. Andrés Oswaldo Calderón Romero  
**Grupo**: Santiago Mesa, Natalia Avila, Juan Diego Arias y Nicolas Camacho  
**Institución**: Pontificia Universidad Javeriana Bogotá
**Período**: Octubre - Noviembre 2025

---

## 📄 Licencia

Este proyecto es de carácter académico. Los datos utilizados están sujetos a las licencias de sus respectivos proveedores (Microsoft ODbL, Google CC BY-4.0, DANE).

---

## 🔗 Enlaces Útiles

- [MongoDB Documentation](https://docs.mongodb.com/)
- [GeoPandas Documentation](https://geopandas.org/)
- [PlantUML Documentation](https://plantuml.com/)
- [DANE Geoportal](https://geoportal.dane.gov.co/)

---

**Última actualización**: 27 de Octubre, 2025
