## =============================================================================
## ingest_duckdb.R
## =============================================================================
## Propósito:
##   Ingestar los resultados de parse_inia() en una base de datos DuckDB local
##   ("test_clima.duckdb"). Crea dos tablas:
##     - inia_data : observaciones diarias (DATE, SOURCE, variables + QC)
##     - inia_meta : metadatos de estaciones (source, region, id, nombre, fechas)
##
## Requisito previo:
##   La variable `resultado` debe estar en el ambiente (salida de parse_inia()).
##
## Dependencias: duckdb, DBI
## =============================================================================

# -- Conexión ------------------------------------------------------------------
con <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = "test_clima.duckdb"
)

# -- Ingesta -------------------------------------------------------------------
DBI::dbWriteTable(con, "inia_data", resultado$data,
                  overwrite = TRUE, append = FALSE)

DBI::dbWriteTable(con, "inia_meta", resultado$meta,
                  overwrite = TRUE, append = FALSE)

# -- Verificación rápida -------------------------------------------------------
message("Tablas en test_clima.duckdb: ",
        paste(DBI::dbListTables(con), collapse = ", "))
message("Filas inia_data : ", DBI::dbGetQuery(con, "SELECT COUNT(*) FROM inia_data")[[1]])
message("Filas inia_meta : ", DBI::dbGetQuery(con, "SELECT COUNT(*) FROM inia_meta")[[1]])

# -- Cerrar conexión -----------------------------------------------------------
DBI::dbDisconnect(con, shutdown = TRUE)
rm(con)
