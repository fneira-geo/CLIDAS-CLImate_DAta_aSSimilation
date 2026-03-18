## METADATA -------------------------------------------------------------------
## nombre script    : normalizar_bbdd.R
## proposito        : normalizar base de datos para ficha CIREN previo climatol
## fecha creacion   : 2026-03-04
## Copyright        : Fernando Neira Román / fneira.roman@gmail.com

## LIMPIAR AMBIENTE -----------------------------------------------------------
rm(list = ls())

## OPTIONS --------------------------------------------------------------------
set.seed(12345)

## LIBRERIAS ------------------------------------------------------------------
librerias <- c('dplyr', 'tidyr', 'readr', 'purrr', 'readxl',
               'writexl', 'lubridate', 'stringr', 'terra',
               'tidyterra', 'ggplot2', 'duckdb')
sapply(librerias, require, character.only = TRUE)


## FUNCIONES ------------------------------------------------------------------
source("src/parser_inia.R")
source("src/parser_dga.R")

# source("src/test_parser_inia.R")
# source("src/qc_duplicated_data.R")


## DIRECTORIOS ----------------------------------------------------------------
readRenviron(".env")
DATA_RAW_DGA  <- Sys.getenv("DATA_RAW_DGA")
DATA_RAW_INIA <- Sys.getenv("DATA_RAW_INIA")
DATA_OUT      <- Sys.getenv("DATA_OUT")

META_XLSX_INIA <- "src/metadata_inia-agrometeorologia.xlsx"


## CODIGOS ---------------------------------------------------------------------
# DGA LECTURA
dga_tx <- parser_dga(file.path(DATA_RAW_DGA, "TEMPERATURAS"), variable = "tx")
dga_tn <- parser_dga(file.path(DATA_RAW_DGA, "TEMPERATURAS"), variable = "tn")
dga_pp <- parser_dga(file.path(DATA_RAW_DGA, "PRECIPITACIONES"), variable = "pp")

meta_dga <- dplyr::bind_rows(
    dga_tx$meta,
    dga_tn$meta,
    dga_pp$meta
) |>
    dplyr::distinct()


# INIA LECTURA Y OBTENCION DE DATOS DESDE ARCHIVOS
inia_tx <- parser_inia(ruta = DATA_RAW_INIA, nom_var = "TA_MAX")
inia_tn <- parser_inia(ruta = DATA_RAW_INIA, nom_var = "TA_MIN")
inia_pp <- parser_inia(ruta = DATA_RAW_INIA, nom_var = "PP_SUM")
# inia_hr <- parser_inia(ruta = DATA_RAW_INIA, nom_var = "HR_AVG")
# inia_rd <- parser_inia(ruta = DATA_RAW_INIA, nom_var = "RD_AVG")
# inia_ps <- parser_inia(ruta = DATA_RAW_INIA, nom_var = "PS_AVG")

# INIA ASOCIACION DE METADATA COORDENADAS GEOGRAFICAS
list.files(DATA_RAW_INIA)

meta_inia <- readxl::read_excel(
    path = META_XLSX_INIA,
) %>%
    dplyr::mutate(
    )

get_data_inia(ruta = DATA_RAW_INIA)

list.files(DATA_RAW_INIA, recursive = TRUE,
           pattern = "manifest.csv|extraction_stats.json")

## SALIDAS ---------------------------------------------------------------------

writexl::write_xlsx(
    x = list(
        data_pp_dga = dga_pp$data,
        data_tx_dga = dga_tx$data,
        data_tn_dga = dga_tn$data,
        meta_dga = dga_tn$meta,
        data_pp_inia = inia_pp$data,
        data_tx_inia = inia_tx$data,
        data_tn_inia = inia_tn$data,
        meta_inia = inia_pp$meta
    ),
    path = file.path(DATA_OUT, paste0(Sys.Date(), "_LOS_RIOS.xlsx"))
)


## CONSULTA DUCKDB: PP_SUM estaciones Los Ríos ---------------------------------
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "test_clima.duckdb", read_only = TRUE)

inia_losrios_pp <- DBI::dbGetQuery(con, "
    SELECT
        d.DATE,
        m.nombre,
        m.id,
        d.PP_SUM,
        d.PP_SUM_QC
    FROM inia_data  AS d
    JOIN inia_meta  AS m ON d.SOURCE = m.source
    WHERE m.region = 'los_rios'
    ORDER BY m.nombre, d.DATE
")

DBI::dbDisconnect(con, shutdown = TRUE)
rm(con)

