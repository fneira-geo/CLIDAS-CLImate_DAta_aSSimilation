## Propósito:
##   Explorar el output de parser_inia() y entender qué transformaciones
##   se necesitan para alinear su formato con el de parser_dga().
##
## Requiere en el ambiente: inia_all  (salida de parser_inia())
## Corre de forma interactiva, bloque a bloque.


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


.get_data_inia <- function(ruta) {
  localidad <- readr::locale(
    date_names = "es",
    date_format = "%d-%m-%Y",
    time_format = "",
    decimal_mark = ".",
    grouping_mark = ",",
    tz = "UTC",
    encoding = "UTF-8",
    asciify = FALSE
  )

  # Definimos los archivos a ignorar para mantener DRY
  ignored_files <- c("manifest.csv", "extraction_stats.json")

  # all_files <- list.files(DATA_RAW_INIA, recursive = TRUE, full.names = TRUE)
  #
  # # Filtrado por exclusión (Vectorizado)
  # files_to_process <- all_files[!(basename(all_files) %in% ignored_files)]

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

  all_files <- list.files(ruta,
                      pattern = "\\.csv$",
                      recursive = TRUE,
                      full.names = TRUE)

  files <- all_files[!(basename(all_files) %in% ignored_files)]


  if (length(files) == 0)
    stop("No se encontraron archivos CSV en: ", ruta)

  data <- purrr::map(files, \(path) {
    suppressWarnings(
      readr::read_delim(
        path,
        skip = 5,
        delim = ",",
        locale = localidad,
        show_col_types = FALSE,
        progress = FALSE,
        col_types = readr::cols(
          .default = readr::col_double(),
          `Tiempo UTC-4` = readr::col_date(format = "%d-%m-%Y")
        )
      )
    ) |>
      dplyr::rename(dplyr::any_of(col_rename)) |>
      dplyr::filter(!is.na(DATE)) |>
      dplyr::mutate(SOURCE = tools::file_path_sans_ext(basename(path)),
                    .before = 1)
  },  .progress = list(name = "Leyendo INIA", type = "tasks")) |>
     #  .progress = FALSE ) |>
    purrr::list_rbind()

  meta <- purrr::map(files, \(path) {
    .parse_inia_filename(tools::file_path_sans_ext(basename(path)))
  }) |>
    purrr::list_rbind()

  list(data = data, meta = meta)
}


.pivot_ancho <- function(data,
                         nombres_desde,
                         valores_desde,
                         funciones = mean) {
  tidyr::pivot_wider(
    data = data,
    names_from = dplyr::all_of(nombres_desde),
    values_from = dplyr::all_of(valores_desde),
    values_fn = funciones
  )
}



#df_wide <- df_long %>%
#  tidyr::pivot_wider(
#    names_from = variable_col,
#    values_from = value_col,
#    values_fn = mean # Agregación integrada
#)




parser_inia <- function(ruta,
                        nom_var = c(
                          'TA_AVG',
                          'TA_MIN',
                          'TA_MAX',
                          'HR_AVG',
                          'HR_MIN',
                          'HR_MAX',
                          'PP_SUM',
                          'PS_AVG',
                          'RD_AVG',
                          'TS00_AVG',
                          'TS00_MIN',
                          'TS00_MAX',
                          'TS10_AVG',
                          'TS10_MIN',
                          'TS10_MAX',
                          'VV_AVG',
                          'VV_MAX',
                          'DV_AVG'
                        )) {
  nom_var <- match.arg(nom_var)

  cols_siempre <- c("SOURCE", "DATE")

  data <- .get_data_inia(ruta)

  data_temporal <- data[["data"]][c(cols_siempre, nom_var)] |> dplyr::distinct()

  data[['data1']] <- data_temporal

  data_temporal2 <- .pivot_ancho(data = data_temporal,
                                 nombres_desde = "SOURCE",
                                 valores_desde = nom_var)


  data[['data2']] <- data_temporal2

  #ini <- min(data_temporal["DATE"])
  #fin <- max(data_temporal["DATE"])

  return(data)
}
