## =============================================================================
## parser_inia.R
## =============================================================================
## Propósito:
##   Leer y normalizar los archivos CSV exportados desde la plataforma INIA
##   (Red Agrometeorológica). Consolida todos los archivos de una carpeta en
##   un único data.frame con nombres de columna estandarizados.
##
## Formato de salida (equivalente a parser_dga):
##   list(
##     data = tibble con columnas:
##              DATE, SOURCE, TA_MIN, TA_MAX, HR_AVG, PP_SUM, RD_AVG
##              + QC: TA_MIN_QC, TA_MAX_QC, HR_AVG_QC, PP_SUM_QC, RD_AVG_QC
##     meta = data.frame con columnas:
##              source, region, id, nombre, date_from, date_to
##   )
##   La columna SOURCE en $data referencia la columna source en $meta.
##
## Nombre de archivo esperado:
##   REGION__ID__ESTACION_ALL_day_YYYYMMDD-YYYYMMDD.csv
##   Ejemplo: tarapaca__EXT-1012__colchane_colchane_dmc_ALL_day_20160101-20251231.csv
##
## Formato esperado de los CSV:
##   - Separador: coma
##   - Encabezado en fila 6 (skip = 5)
##   - Fechas en columna "Tiempo UTC-4" con formato %d-%m-%Y
##   - Decimales con punto, encoding UTF-8
##
## Dependencias: readr, purrr, dplyr, tools (base R)
## =============================================================================

# -- Funciones obsoletas / exploración previa ----------------------------------
# Las funciones listar_csv, read_csv, classify_climate_cols, build_skeleton y
# get_year_blocks, junto con el código suelto de prueba (lista, asdf, datos),
# fueron reemplazados por parse_inia(). Se mantienen comentados por referencia.
#
# listar_csv <- function(ruta) { ... }
# classify_climate_cols <- function(col_names) { ... }
# read_csv <- function(lista) { ... }
# build_skeleton <- function(meta, date_from, date_to) { ... }
# get_year_blocks <- function(raw) { ... }
# ------------------------------------------------------------------------------


# Extrae metadatos desde el basename de un archivo INIA.
# Formato: REGION__ID__ESTACION_ALL_day_YYYYMMDD-YYYYMMDD
.parse_inia_filename <- function(name) {
    parts  <- strsplit(name, "__")[[1]]
    region <- parts[1]
    id     <- parts[2]
    rest   <- strsplit(parts[3], "_ALL_day_")[[1]]
    nombre <- rest[1]
    dates  <- strsplit(rest[2], "-")[[1]]
    data.frame(
        source    = name,
        region    = region,
        id        = id,
        nombre    = nombre,
        date_from = as.Date(dates[1], format = "%Y%m%d"),
        date_to   = as.Date(dates[2], format = "%Y%m%d"),
        stringsAsFactors = FALSE
    )
}


parser_inia <- function(ruta) {
    localidad <- readr::locale(
        date_names = "es", date_format = "%d-%m-%Y", time_format = "",
        decimal_mark = ".", grouping_mark = ",",
        tz = "UTC", encoding = "UTF-8", asciify = FALSE
    )

    col_rename <- c(
        "DATE"      = "Tiempo UTC-4",
        "TA_MIN"    = "Temperatura del Aire Mínima ºC",
        "TA_MAX"    = "Temperatura del Aire Máxima ºC",
        "HR_AVG"    = "Humedad Relativa %",
        "PP_SUM"    = "Precipitación Acumulada mm",
        "RD_AVG"    = "Radiación Solar Mj/m²",
        "TA_MIN_QC" = "Temperatura del Aire Mínima % de datos ",
        "TA_MAX_QC" = "Temperatura del Aire Máxima % de datos ",
        "HR_AVG_QC" = "Humedad Relativa % de datos ",
        "PP_SUM_QC" = "Precipitación Acumulada % de datos ",
        "RD_AVG_QC" = "Radiación Solar % de datos "
    )

    files <- list.files(ruta, pattern = "\\.csv$", recursive = TRUE, full.names = TRUE) |>
        grep("manifest\\.csv$", x = _, value = TRUE, invert = TRUE)

    if (length(files) == 0)
        stop("No se encontraron archivos CSV en: ", ruta)

    data <- purrr::map(files, \(path) {
        suppressWarnings(
            readr::read_delim(path, skip = 5, delim = ",",
                locale = localidad, show_col_types = FALSE,
                col_types = readr::cols(
                    .default = readr::col_double(),
                    `Tiempo UTC-4` = readr::col_date(format = "%d-%m-%Y")
                )
            )
        ) |>
            dplyr::rename(dplyr::any_of(col_rename)) |>
            dplyr::mutate(SOURCE = tools::file_path_sans_ext(basename(path)), .before = 1)
    }, .progress = list(name = "Leyendo INIA", type = "iterator")) |>
        purrr::list_rbind()

    meta <- purrr::map(files, \(path) {
        .parse_inia_filename(tools::file_path_sans_ext(basename(path)))
    }) |>
        purrr::list_rbind()

    list(data = data, meta = meta)
}

# resultado <- parse_inia(DATA_RAW_INIA)  # prueba manual


