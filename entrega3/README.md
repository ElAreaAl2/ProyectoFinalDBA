# 📦 ENTREGA 3 - Pipeline de Carga y Análisis de Edificios PDET

## 📋 Descripción

Esta entrega implementa el pipeline completo para:
- Crear el esquema de MongoDB con validadores e índices espaciales
- Cargar municipios PDET desde GeoJSON
- Generar, procesar y cargar edificios a MongoDB
- Realizar análisis exploratorio de datos (EDA)

**Base de datos:** `pdet_solar`  
**Colecciones:** `municipalities`, `buildings_microsoft`, `buildings_google`

---

## 🚀 INSTRUCCIONES DE EJECUCIÓN

### ⚠️ IMPORTANTE
Ejecutar los comandos **en orden** desde la carpeta raíz del proyecto:

```bash
cd ~/ProyectoFinalDBA
```

---

## 📝 PASO A PASO

### PASO 1: Crear Colecciones en MongoDB

**Script:** `scripts/crear_colecciones_y_validadores.py`

**¿Qué hace?**
- Crea 3 colecciones en MongoDB (`municipalities`, `buildings_microsoft`, `buildings_google`)
- Aplica validadores de esquema JSON para garantizar calidad de datos
- Crea índices espaciales 2dsphere en campos `geometry`
- Crea índices en campos clave (`codigo_dane`, `municipality_code`, `area_m2`)

**Ejecutar:**
```bash
python3 scripts/crear_colecciones_y_validadores.py
```

**Resultado esperado:**
```
✅ Colección 'municipalities' creada
✅ Índices creados para 'municipalities'
✅ Colección 'buildings_microsoft' creada
✅ Índices creados para 'buildings_microsoft'
✅ Colección 'buildings_google' creada
✅ Índices creados para 'buildings_google'
```

---

### PASO 2: Cargar Municipios PDET

**Script:** `scripts/cargar_municipios_desde_geojson.py`

**¿Qué hace?**
- Lee el archivo `entrega2/MGN2024_MUNICIPIOS_PDET.geojson` (65 MB)
- Extrae 170 municipios PDET con sus geometrías (polígonos/multipolígonos)
- Limpia y estructura los datos según el esquema definido
- Inserta los documentos en la colección `municipalities`
- Verifica la carga y muestra ejemplos

**Ejecutar:**
```bash
python3 scripts/cargar_municipios_desde_geojson.py
```

**Resultado esperado:**
```
✅ 170 municipios insertados correctamente
📋 Ejemplos de municipios cargados:
  1. AMALFI (ANTIOQUIA) - Código: 05031
  2. ANORÍ (ANTIOQUIA) - Código: 05040
  3. APARTADÓ (ANTIOQUIA) - Código: 05045
  ...
```

---

### PASO 3: Generar Edificios de Muestra

**Script:** `entrega3/loaders/01_descargar_muestra_ms.py`

**¿Qué hace?**
- Selecciona los primeros 3 municipios PDET de MongoDB
- Genera 500 edificios aleatorios dentro de cada municipio usando sus geometrías reales
- Crea polígonos pequeños (~10m x 10m) que representan edificios
- Guarda los datos en formato GeoJSONL en `data/buildings/buildings_microsoft_muestra.geojsonl`

**Ejecutar:**
```bash
cd entrega3/loaders
python3 01_descargar_muestra_ms.py
```

**Resultado esperado:**
```
✅ Encontrados 3 municipios de muestra:
   • AMALFI (ANTIOQUIA) - 05031
   • ANORÍ (ANTIOQUIA) - 05040
   • APARTADÓ (ANTIOQUIA) - 05045

✅ Generación completada: 1500 edificios de muestra
📁 Archivo: ../../data/buildings/buildings_microsoft_muestra.geojsonl
```

---

### PASO 4: Calcular Áreas de Edificios

**Script:** `entrega3/loaders/02_calcular_area_y_exportar_geojsonl_ms.py`

**¿Qué hace?**
- Lee el archivo de edificios generado en el paso anterior
- Para cada edificio, calcula su área real en metros cuadrados
- Utiliza proyección UTM (Universal Transverse Mercator) para cálculos precisos
- Determina automáticamente la zona UTM según las coordenadas del edificio
- Agrega el campo `area_m2` a las propiedades de cada edificio
- Guarda el resultado en `data/buildings/buildings_microsoft_con_area.geojsonl`

**Ejecutar:**
```bash
python3 02_calcular_area_y_exportar_geojsonl_ms.py
```

**Resultado esperado:**
```
✅ CÁLCULO COMPLETADO
📊 Estadísticas:
  • Edificios procesados: 1500
  • Área total: 198,686.85 m²
  • Área promedio: 132.46 m²
```

---

### PASO 5: Asignar Municipios a Edificios

**Script:** `entrega3/loaders/04_asignar_municipio_a_edificios.py`

**¿Qué hace?**
- Lee el archivo de edificios con áreas calculadas
- Carga todos los municipios PDET desde MongoDB
- Para cada edificio, realiza operación espacial **point-in-polygon**:
  - Calcula el centroide del edificio
  - Verifica en qué municipio está contenido el punto
- Agrega campos `municipality_code`, `municipality_name` y `department` a cada edificio
- Guarda el resultado final en `data/buildings/buildings_microsoft_final.geojsonl`

**Ejecutar:**
```bash
python3 04_asignar_municipio_a_edificios.py
```

**Resultado esperado:**
```
✅ ASIGNACIÓN COMPLETADA
📊 Estadísticas:
  • Total edificios: 1500
  • Edificios asignados: 1500
  • Edificios sin municipio: 0
  • Tasa de éxito: 100.0%
```

---

### PASO 6: Cargar Edificios a MongoDB

**Script:** `entrega3/loaders/05_cargar_edificios_a_mongodb.py`

**¿Qué hace?**
- Lee el archivo final de edificios con toda la información procesada
- Limpia la colección `buildings_microsoft` (si existía)
- Convierte cada Feature GeoJSON en un documento MongoDB
- Realiza carga masiva en lotes de 1000 documentos (bulk insert)
- Valida que los documentos cumplan con el esquema definido
- Genera estadísticas por municipio (conteo de edificios, área total)

**Ejecutar:**
```bash
python3 05_cargar_edificios_a_mongodb.py
```

**Resultado esperado:**
```
✅ CARGA COMPLETADA: Microsoft
📊 Estadísticas:
  • Documentos cargados: 1500
  • Documentos con error: 0
  • Tasa de éxito: 100.0%

📍 Top 5 municipios con más edificios:
  • AMALFI: 500 edificios, 68,920 m²
  • ANORÍ: 500 edificios, 68,103 m²
  • APARTADÓ: 500 edificios, 61,664 m²
```

---

### PASO 7: Ejecutar Análisis Exploratorio (EDA)

**Script:** `entrega3/eda/01_analisis_rapido_top_municipios_y_area.py`

**¿Qué hace?**
- Conecta a MongoDB y extrae estadísticas generales
- Calcula totales: número de municipios, edificios, áreas
- Identifica los top 15 municipios por área total de techos
- Genera visualizaciones:
  - Gráfico de barras: Top 15 municipios por área (PNG)
  - Comparación entre Microsoft y Google (si hay datos)
- Exporta resultados a CSV:
  - `estadisticas_generales.csv`: Resumen numérico
  - `top_municipios_microsoft.csv`: Ranking detallado con códigos DANE
- Muestra resumen en consola con top 3 municipios

**Ejecutar:**
```bash
cd ../eda
python3 01_analisis_rapido_top_municipios_y_area.py
```

**Resultado esperado:**
```
✅ EDA completado exitosamente!
📁 Archivos generados en: /home/estudiante/ProyectoFinalDBA/entrega3/eda

Archivos creados:
  • top_15_municipios_microsoft.png
  • estadisticas_generales.csv
  • top_municipios_microsoft.csv

📊 RESUMEN:
  • Municipios PDET: 170
  • Edificios Microsoft: 1,500
  • Área total: 0.199 km²
```

---

## ✅ VERIFICACIÓN DE RESULTADOS

### Verificar en MongoDB Shell:

```bash
mongosh pdet_solar
```

Dentro de `mongosh`:
```javascript
// Contar municipios
db.municipalities.countDocuments()
// Resultado esperado: 170

// Contar edificios
db.buildings_microsoft.countDocuments()
// Resultado esperado: 1500

// Ver un municipio de ejemplo
db.municipalities.findOne()

// Ver un edificio de ejemplo
db.buildings_microsoft.findOne()

// Top 5 municipios por número de edificios
db.buildings_microsoft.aggregate([
  {$group: {
    _id: "$municipality_name",
    count: {$sum: 1},
    area_total: {$sum: "$area_m2"}
  }},
  {$sort: {count: -1}},
  {$limit: 5}
])
```

### Verificar archivos generados:

```bash
cd ~/ProyectoFinalDBA/entrega3/eda

# Listar archivos
ls -lh *.png *.csv

# Ver estadísticas
cat estadisticas_generales.csv

# Abrir imagen (si hay interfaz gráfica)
xdg-open top_15_municipios_microsoft.png
```

---

## 📊 ESTRUCTURA DE DATOS

### Colección: `municipalities`
```javascript
{
  "_id": ObjectId("..."),
  "codigo_dane": "05031",
  "nombre": "AMALFI",
  "departamento": "ANTIOQUIA",
  "codigo_departamento": "05",
  "subregion_pdet": "BAJO CAUCA Y NORDESTE ANTIOQUEÑO",
  "is_pdet": true,
  "geometry": {
    "type": "Polygon",
    "coordinates": [[[...]]]
  },
  "metadata": {
    "fecha_carga": ISODate("2024-11-10T..."),
    "fuente": "DANE MGN 2024",
    "area_km2": 1209.14546227,
    "año": 2024
  }
}
```

### Colección: `buildings_microsoft`
```javascript
{
  "_id": ObjectId("..."),
  "municipality_code": "05031",
  "municipality_name": "AMALFI",
  "department": "ANTIOQUIA",
  "geometry": {
    "type": "Polygon",
    "coordinates": [[[...]]]
  },
  "area_m2": 132.46,
  "source": "Microsoft",
  "metadata": {
    "fecha_carga": ISODate("2024-11-10T..."),
    "confidence": 0.95
  }
}
```

---

## 📁 ARCHIVOS GENERADOS

```
ProyectoFinalDBA/
├── data/
│   └── buildings/
│       ├── buildings_microsoft_muestra.geojsonl      (1500 líneas)
│       ├── buildings_microsoft_con_area.geojsonl     (1500 líneas)
│       └── buildings_microsoft_final.geojsonl        (1500 líneas)
│
└── entrega3/
    └── eda/
        ├── top_15_municipios_microsoft.png           (Gráfico de barras)
        ├── estadisticas_generales.csv                (Resumen numérico)
        └── top_municipios_microsoft.csv              (Ranking detallado)
```

---

## 🎯 LOGROS DE ESTA ENTREGA

✅ **Esquema MongoDB robusto**
- 3 colecciones con validadores JSON Schema
- Índices espaciales 2dsphere para consultas geoespaciales
- Índices en campos clave para optimizar queries

✅ **Pipeline ETL completo**
- Extracción: Lectura de GeoJSON (170 municipios PDET)
- Transformación: Cálculo de áreas, asignación espacial
- Carga: Bulk insert a MongoDB con validación

✅ **Análisis de datos geoespaciales**
- Operaciones point-in-polygon con 100% de éxito
- Proyecciones UTM para cálculos precisos de área
- Agregaciones MongoDB para estadísticas

✅ **Visualizaciones y reportes**
- Gráficos profesionales con matplotlib/seaborn
- Exportación a CSV para análisis adicional
- Documentación completa del proceso

---

## 🔮 PENDIENTE PARA LA ENTREGA FINAL

### 1. **Completar datasets**
- [ ] Descargar edificios completos de Microsoft Building Footprints (no solo muestra)
- [ ] Integrar datos de Google Open Buildings para Colombia
- [ ] Procesar los ~170 municipios PDET completos (actualmente solo 3)

### 2. **Análisis avanzado**
- [ ] Comparación detallada Microsoft vs Google por municipio
- [ ] Cálculo de potencial solar (kWh/año) por edificio
- [ ] Identificar edificios públicos vs privados (si hay datos)
- [ ] Análisis de distribución urbano/rural

### 3. **Optimizaciones**
- [ ] Implementar procesamiento paralelo para áreas grandes
- [ ] Cachear geometrías de municipios en memoria
- [ ] Optimizar queries con `$geoNear` y `$geoWithin`
- [ ] Implementar índices compuestos para queries complejas

### 4. **Visualizaciones adicionales**
- [ ] Mapas interactivos con Folium/Plotly
- [ ] Heatmap de densidad de edificios
- [ ] Comparación por subregiones PDET
- [ ] Dashboard interactivo (opcional: Streamlit/Dash)

### 5. **Validación y calidad**
- [ ] Validar que edificios no se superpongan
- [ ] Identificar outliers en área (edificios sospechosamente grandes/pequeños)
- [ ] Calcular métricas de calidad (confidence score promedio)
- [ ] Documentar limitaciones y fuentes de error

### 6. **Documentación final**
- [ ] Informe técnico completo (metodología, resultados, conclusiones)
- [ ] Manual de usuario para replicar el análisis
- [ ] Diccionario de datos detallado
- [ ] Presentación ejecutiva con hallazgos clave

### 7. **Extras (opcionales)**
- [ ] API REST para consultar datos (FastAPI/Flask)
- [ ] Containerización con Docker
- [ ] CI/CD con GitHub Actions
- [ ] Tests unitarios para funciones críticas

---

## 🐛 SOLUCIÓN DE PROBLEMAS

### Error: "No such file or directory"
```bash
# Asegúrate de estar en la carpeta correcta
cd ~/ProyectoFinalDBA
pwd  # Debe mostrar: /home/estudiante/ProyectoFinalDBA
```

### Error: "Connection refused" (MongoDB)
```bash
# Verificar que MongoDB esté corriendo
sudo systemctl status mongod

# Si no está corriendo:
sudo systemctl start mongod
```

### Error: "Module not found"
```bash
# Reinstalar dependencias
pip3 install pymongo geopandas shapely pandas tqdm matplotlib seaborn pyproj
```

### Colección vacía después de cargar
```bash
# Verificar en MongoDB
mongosh pdet_solar --eval "db.buildings_microsoft.countDocuments()"

# Si es 0, volver a ejecutar el paso de carga:
cd entrega3/loaders
python3 05_cargar_edificios_a_mongodb.py
```

---

## 📞 INFORMACIÓN DEL PROYECTO

**Repositorio:** https://github.com/ElAreaAl2/ProyectoFinalDBA  
**Curso:** Diseño de Bases de Datos Analíticas  
**Fecha:** Noviembre 2024  
**Base de datos:** MongoDB 6.0.26  

---

## ⏱️ TIEMPO ESTIMADO DE EJECUCIÓN

| Paso | Script | Tiempo aproximado |
|------|--------|-------------------|
| 1 | Crear colecciones | 10 segundos |
| 2 | Cargar municipios | 30-60 segundos |
| 3 | Generar edificios | 1-2 minutos |
| 4 | Calcular áreas | 30 segundos |
| 5 | Asignar municipios | 1-2 minutos |
| 6 | Cargar a MongoDB | 15 segundos |
| 7 | EDA y visualizaciones | 30 segundos |
| **TOTAL** | | **~5-7 minutos** |

---
