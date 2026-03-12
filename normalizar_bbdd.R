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
librerias <- c('dplyr', 'terra', 'duckdb')
sapply(librerias, require, character.only = TRUE)


## FUNCIONES ------------------------------------------------------------------
source("src/parser_inia.R")
source("src/parser_dga.R")

# source("src/test_parser_inia.R")
# source("src/qc_duplicated_data.R")

parser_dga <- function(ruta, variable = c("pp", "tn", "tx")) {
    ini <- "1990-01-01"
    fin <- "2025-12-31"

    variable <- match.arg(variable)

    if (variable == "pp" ){
        out <- parser_dga_daily_pp(
            path = ruta,
            date_from = ini,
            date_to   = fin,
            verbose   = TRUE)
    } else{
        out <- parser_dga_daily_temp(
            path = ruta,
            date_from = ini,
            date_to   = fin,
            verbose   = TRUE,
            temp = variable)
    }

    return(out)
}


## DIRECTORIOS ----------------------------------------------------------------

DATA_RAW_DGA <- "E:/SALIDAS/2026/FICHA_LOS_RIOS/RAW/DGA"
DATA_RAW_INIA <- "E:/SALIDAS/2026/FICHA_LOS_RIOS/RAW/INIA-AGROMETEOROLOGIA"

DATA_RAW_INIA <- "E:/SALIDAS/2026/FICHA_LOS_RIOS/RAW/INIA-AGROMETEOROLOGIA"

DATA_OUT <- "E:/SALIDAS/2026/FICHA_LOS_RIOS/"

# DATA_RAW_INIA <- '/Users/fneira/Documents/GITHUB/climate-db-inia/'
# DATA_INIA_TEST <- "E:/SALIDAS/2026/FICHA_LOS_RIOS/RAW/test/los_rios__EXT-1010__fundo_el_maiten_paillaco_dmc_ALL_day_20160101-20251231.csv"
# DATA_OUT <- ""

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

names(meta_dga)


dga_tx |> dplyr::select(dplyr::all_of( unlist(meta_dga$nombre)))
# aa <- parse_dga_pp(file.path(DATA_RAW_DGA, "PRECIPITACIONES"))

# INIA LECTURA
# inia_all <- parser_inia(ruta = DATA_RAW_INIA)
# names(inia_all$data)

source('src/test_parser_inia.R')
inia_tx <- parser_inia(ruta = DATA_RAW_INIA, nom_var = "TA_MAX")
inia_tn <- parser_inia(ruta = DATA_RAW_INIA, nom_var = "TA_MIN")
inia_pp <- parser_inia(ruta = DATA_RAW_INIA, nom_var = "PP_SUM")


pivot_ancho <- function(data, nombres_desde, valores_desde, funciones = NULL){
  tidyr::pivot_wider(
    names_from = {{nombres_desde}},
    values_from = {{valores_desde}},
    values_fn = {{funciones}}
  )
}


aa <- tidyr::pivot_wider(
    data = inia_tn$data,
    names_from = "SOURCE",
    values_from = "TA_MIN",
    values_fn = length
) #|> unique()


writexl::write_xlsx(
    x = list(
        data_pp_dga = dga_pp$data,
        meta_pp_dga = dga_pp$meta,
        data_tx_dga = dga_tx$data,
        meta_tx_dga = dga_tx$meta,
        data_tn_dga = dga_tn$data,
        meta_tn_dga = dga_tn$meta,
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

