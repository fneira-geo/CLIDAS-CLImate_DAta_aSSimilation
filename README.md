# clima-db-estaciones

Base de datos climática de estaciones meteorológicas de Chile, orientada a consultas rápidas y análisis aplicado para planificación hidrológica y agrícola.

## Objetivo

Consolidar datos de múltiples fuentes (DGA, INIA) en un esquema único almacenado en **DuckDB** y **Parquet**, con valores preprocesados listos para uso directo:

-   Estadísticas anuales (media, máximo, mínimo, percentiles 5 y 95)
-   Índices agroclimáticos calculados por estación

## Fuentes de datos

| Fuente | Organismo | Variables | Formato original |
|----|----|----|----|
| DGA | Dirección General de Aguas | Precipitación (PP), Tmax (TX), Tmin (TN) | XLS por estación |
| INIA | Red Agrometeorológica | PP, TX, TN, Humedad relativa (HR), Radiación (RD) | CSV por estación |

**Período**: 1990–2025 · **Resolución**: diaria

## Estructura del proyecto

```         
clima-db-estaciones/
├── src/
│   ├── parser_dga.R          # lectura y normalización de archivos DGA
│   ├── parser_inia.R         # lectura y normalización de archivos INIA
│   ├── ingest_duckdb.R       # ingesta a DuckDB
│   ├── qc_duplicated_data.R  # control de calidad: duplicados
│   └── check_sheets_file_data.R
├── normalizar_bbdd.R          # script principal de procesamiento
├── .Rprofile                  # configuración R + ruta Python por OS
└── .gitignore
```

## Esquema de la base de datos

``` sql
-- Observaciones diarias
inia_data (DATE, SOURCE, TA_MIN, TA_MAX, HR_AVG, PP_SUM, RD_AVG,
           TA_MIN_QC, TA_MAX_QC, HR_AVG_QC, PP_SUM_QC, RD_AVG_QC)

-- Metadatos de estaciones
inia_meta (source, region, id, nombre, date_from, date_to)
```

## Estadísticas preprocesadas (en desarrollo)

Por estación y mes/año:

| Estadístico     | Descripción                                          |
|-----------------|------------------------------------------------------|
| Media histórica | Promedio del período completo                        |
| Máximo / Mínimo | Valores extremos registrados                         |
| P05 / P95       | Percentiles 5 y 95 (año seco / húmedo de referencia) |

## Índices agroclimáticos (planificados)

| Índice | Descripción | Uso |
|----|----|----|
| GDD | Grados-día de crecimiento (base 10 °C) | Fenología cultivos |
| ETP | Evapotranspiración potencial (Hargreaves) | Demanda hídrica |
| Días helada | Días con Tmin ≤ 0 °C | Riesgo agrícola |
| Días lluvia | Días con PP \> 1 mm | Calendario de labores |
| Índice de aridez | P / ETP anual | Clasificación climática |
| Período libre de heladas | Días entre última/primera helada | Planificación siembra |

## Índices agroclimáticos (planificados)

| Índice | Fórmula / Criterio | Variables | Uso |
|----|----|----|----|
| GDD | `max(0, (TX + TN)/2 - Tbase)` acumulado, Tbase=10°C | TA_MIN, TA_MAX | Fenología cultivos |
| ETP (Hargreaves) | `0.0023 × Ra × (Tmean + 17.8) × √(TX - TN)` | TA_MIN, TA_MAX, latitud | Demanda hídrica |
| Días helada | `count(TN ≤ 0)` por mes/año | TA_MIN | Riesgo agrícola |
| Días lluvia | `count(PP > 1 mm)` por mes/año | PP_SUM | Calendario labores |
| Índice de aridez | `∑PP / ∑ETP` anual | PP_SUM, ETP | Clasificación climática |
| Período libre heladas | Días entre última helada primavera – primera helada otoño | TA_MIN, DATE | Planificación siembra |

> **Nota**: ETP Hargreaves requiere Ra (radiación extraterrestre), calculada con latitud + día del año. Si se dispone de radiación medida (RD_AVG) y humedad (HR_AVG), se puede usar Penman-Monteith simplificado.

## Uso rápido

``` r
library(DBI)
library(duckdb)

con <- dbConnect(duckdb(), dbdir = "clima.duckdb", read_only = TRUE)

# Precipitación media mensual por estación
dbGetQuery(con, "
    SELECT
        m.nombre,
        month(DATE) AS mes,
        round(avg(PP_SUM), 1) AS pp_media_mm,
        round(percentile_cont(0.05) WITHIN GROUP (ORDER BY PP_SUM), 1) AS pp_p05,
        round(percentile_cont(0.95) WITHIN GROUP (ORDER BY PP_SUM), 1) AS pp_p95
    FROM inia_data d
    JOIN inia_meta m ON d.SOURCE = m.source
    GROUP BY m.nombre, mes
    ORDER BY m.nombre, mes
")

dbDisconnect(con, shutdown = TRUE)
```

## Dependencias R

``` r
install.packages(c("duckdb", "DBI", "dplyr", "tidyr", "readr",
                   "readxl", "purrr", "writexl"))
```

## Entornos Python (reticulate)

| Sistema | Gestor     | Entorno      |
|---------|------------|--------------|
| Windows | UV         | `global-env` |
| macOS   | micromamba | `geoenv`     |

La ruta se selecciona automáticamente desde `.Rprofile`.
