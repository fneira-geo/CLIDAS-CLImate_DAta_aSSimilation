## METADATA -------------------------------------------------------------------
## nombre script    : parser_dga.R
## proposito        : Parsear datos DGA de precipitacion y temperatura desde archivos XLS
## autor            : Fernando Neira-Román
## fecha creacion   : 2025-03-06
## version de R     : R version 4.4.2 (2024-10-31 ucrt)
## Copyright        : Fernando Neira-Román, 2025
## Email            : fneira.roman@gmail.com

## HELPERS COMPARTIDOS --------------------------------------------------------

# Convierte string Grados-Minutos-Segundos a grados decimales (hemisferio sur).
# Formato esperado: "DD° MM' SS''" (e.g. "40° 09' 58''").
.dms2dd <- function(dms_str) {
    tryCatch({
        parts   <- unlist(strsplit(trimws(dms_str), "[°'\"\\s]+"))
        degrees <- as.numeric(parts[1])
        minutes <- as.numeric(parts[2])
        seconds <- as.numeric(parts[3])
        (degrees + minutes / 60 + seconds / 3600) * -1
    }, error = function(e) {
        warning(sprintf("Cadena invalida GradoMinutoSegundo: '%s'. Devuelve NA.", dms_str))
        NA
    })
}


# Lee todas las hojas de un archivo XLS DGA como lista de data frames.
.read_xls <- function(path, range) {
    sheets <- readxl::excel_sheets(path)
    lapply(seq_along(sheets), function(i) {
        readxl::read_excel(
            path         = path,
            sheet        = i,
            range        = range,
            col_types    = "text",
            col_names    = FALSE,
            .name_repair = "unique_quiet"
        )
    })
}


# Extrae metadatos de estacion desde el encabezado estandar de una hoja DGA.
# Filas 6-9: nombre, BNA, cuenca, subcuenca en col3.
# Fila 7-9 col11: altura, latitud, longitud.
# Fila 7-8 col15: UTM Norte, UTM Este.
.get_meta <- function(data) {
    data.frame(
        nombre    = data[[6,  3]],
        bna       = data[[7,  3]],
        cuenca    = data[[8,  3]],
        subcuenca = data[[9,  3]],
        altura    = data[[7, 11]],
        latitud   = data[[8, 11]],
        longitud  = data[[9, 11]],
        utmN      = data[[7, 15]],
        utmE      = data[[8, 15]],
        lat       = .dms2dd(data[[8, 11]]),
        lon       = .dms2dd(data[[9, 11]]),
        stringsAsFactors = FALSE
    )
}


# Construye grilla de salida vacia (fechas x estaciones) con columnas de fecha.
.build_skeleton <- function(meta, date_from, date_to) {
    fechas       <- seq(as.Date(date_from), as.Date(date_to), by = "day")
    station_cols <- as.data.frame(
        matrix(NA_real_, nrow = length(fechas), ncol = nrow(meta))
    )
    names(station_cols) <- meta$nombre
    cbind(
        data.frame(
            fecha = fechas,
            año   = lubridate::year(fechas),
            mes   = lubridate::month(fechas),
            dia   = lubridate::day(fechas)
        ),
        station_cols
    )
}


# Detecta bloques anuales dentro de datos brutos buscando filas "AÑO XXXX".
# Retorna lista: idx = posiciones de fila AÑO, jars = años numericos,
#                ends = ultima fila de cada bloque.
.get_year_blocks <- function(raw) {
    idx  <- grep("AÑO", unlist(raw[, 1]))
    jars <- stringr::str_extract(unlist(raw[idx, 1]), "\\d{4}") |> as.numeric()
    ends <- c(idx[-1] - 1L, nrow(raw))
    list(idx = idx, jars = jars, ends = ends)
}


## EXTRACTORES DE DATOS -------------------------------------------------------

# Extrae datos de precipitacion de una hoja DGA bruta.
#
# Estructura del bloque anual (33 filas, inicio en fila AÑO):
#   fila 1      : "AÑO XXXX"
#   fila 2      : encabezado DIA / ENE ... DIC
#   filas 3-33  : valores diarios (dias 1-31)
#
# Columnas de meses (col3 y col9 son NA por celdas fusionadas en Excel):
#   col2=ENE  col4=FEB  col5=MAR  col6=ABR  col7=MAY  col8=JUN
#   col10=JUL col11=AGO col12=SEP col13=OCT col14=NOV col15=DIC
.get_data_pp <- function(data) {
    COLS  <- c(1, 2, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15)
    MONTHS <- sprintf("%02d", 1:12)

    raw    <- data[11:nrow(data), ]
    blocks <- .get_year_blocks(raw)

    dplyr::bind_rows(mapply(
        function(start, end, year) {
            raw[start:end, ][3:33, COLS] |>
                setNames(c("dia", MONTHS)) |>
                dplyr::mutate(año = year)
        },
        blocks$idx, blocks$ends, blocks$jars,
        SIMPLIFY = FALSE
    )) |>
        tidyr::pivot_longer(
            cols      = tidyr::all_of(MONTHS),
            names_to  = "mes",
            values_to = "valor"
        ) |>
        dplyr::select(dplyr::any_of(c("año", "mes", "dia", "valor"))) |>
        dplyr::arrange(año, mes, dia) |>
        dplyr::mutate(
            fecha = suppressWarnings(lubridate::ymd(paste(año, mes, dia))),
            .before = 1
        ) |>
        dplyr::filter(!is.na(fecha)) |>
        na.omit()
}


# Extrae datos de temperatura (minima o maxima) de una hoja DGA bruta.
#
# Estructura del bloque anual (67 filas, inicio en fila AÑO):
#   fila 1      : "AÑO XXXX"
#   filas 2-34  : semestre Ene-Jun
#                   fila 2  = encabezado DIA / ENE ... JUN
#                   fila 3  = subencabezado Min. / Max.
#                   filas 4-34 = valores diarios (dias 1-31)
#   filas 35-67 : semestre Jul-Dic  (misma estructura)
#
# Columnas por semestre (identicas en ambos semestres):
#   temp="tn" (minima): col1=dia, col2, col5, col7, col10, col12, col14
#   temp="tx" (maxima): col1=dia, col4, col6, col8, col11, col13, col15
.get_data_temp <- function(data, temp = c("tx", "tn")) {
    temp <- match.arg(temp)
    val_cols <- if (temp == "tn") c(2, 5, 7, 10, 12, 14) else c(4, 6, 8, 11, 13, 15)
    MONTHS   <- sprintf("%02d", 1:12)

    raw    <- data[11:nrow(data), ]
    blocks <- .get_year_blocks(raw)

    dplyr::bind_rows(mapply(
        function(start, end, year) {
            yb <- raw[start:end, ]
            h1 <- yb[4:34,  c(1L, val_cols)]  # Ene-Jun: dia + 6 valores
            h2 <- yb[37:67, val_cols]          # Jul-Dic: 6 valores (dia identico)
            cbind(h1, h2) |>
                setNames(c("dia", MONTHS)) |>
                dplyr::mutate(año = year)
        },
        blocks$idx, blocks$ends, blocks$jars,
        SIMPLIFY = FALSE
    )) |>
        tidyr::pivot_longer(
            cols      = tidyr::all_of(MONTHS),
            names_to  = "mes",
            values_to = "valor"
        ) |>
        dplyr::select(dplyr::any_of(c("año", "mes", "dia", "valor"))) |>
        dplyr::arrange(año, mes, dia) |>
        dplyr::mutate(
            fecha = suppressWarnings(lubridate::ymd(paste(año, mes, dia))),
            .before = 1
        ) |>
        dplyr::filter(!is.na(fecha)) |>
        na.omit()
}


## ORQUESTADOR ----------------------------------------------------------------

# Pipeline interno compartido por parser_dga_daily_pp y parser_dga_daily_temp.
# 1. Descubre archivos XLS en path
# 2. Lee todas las hojas de todos los archivos
# 3. Construye tabla de metadatos
# 4. Construye grilla de salida (fechas x estaciones)
# 5. Rellena la grilla usando get_data_fn hoja por hoja
.dga_base <- function(path, xls_range, file_pattern, get_data_fn, date_from, date_to, verbose) {
    lst_files <- list.files(path, pattern = file_pattern, recursive = TRUE,
                            full.names = TRUE, ignore.case = TRUE)
    if (length(lst_files) == 0)
        stop("No se encontraron archivos con patron '", file_pattern, "' en: ", path)

    # Leer todas las hojas de todos los archivos en una sola lista plana
    data <- unlist(
        lapply(seq_along(lst_files), function(i) {
            if (verbose)
                message(sprintf("[%02d/%02d] leyendo: %s", i, length(lst_files), basename(lst_files[i])))
            .read_xls(lst_files[i], xls_range)
        }),
        recursive = FALSE
    )

    # Construir tabla de metadatos sin duplicados
    out_meta <- dplyr::bind_rows(lapply(data, .get_meta)) |>
        unique() |>
        dplyr::arrange(nombre)
    row.names(out_meta) <- NULL

    # Construir grilla de salida vacia
    out_data <- .build_skeleton(out_meta, date_from, date_to)

    # Rellenar datos hoja por hoja
    n <- length(data)
    for (i in seq_along(data)) {
        if (verbose) message(sprintf("  [%03d/%03d] procesando...", i, n))
        est  <- data[[i]][[6, 3]]
        xls  <- get_data_fn(data[[i]])
        out_data[out_data$fecha %in% xls$fecha, est] <- as.numeric(xls[["valor"]])
    }

    list(data = out_data, meta = out_meta)
}


## API PUBLICA ----------------------------------------------------------------

#' Carga precipitacion diaria DGA desde una carpeta de archivos XLS.
#'
#' @param path      Ruta a la carpeta con archivos DGA_Precipitaciones_Diarias.xls
#' @param date_from Fecha inicio de la grilla de salida (default "1990-01-01")
#' @param date_to   Fecha fin de la grilla de salida   (default "2021-12-31")
#' @param verbose   Imprimir progreso en consola        (default TRUE)
#' @return Lista con $data (data.frame fechas x estaciones) y $meta (metadatos)
parser_dga_daily_pp <- function(path,
                                date_from = "1990-01-01",
                                date_to   = "2021-12-31",
                                verbose   = TRUE) {
    .dga_base(
        path         = path,
        xls_range    = "B1:P142",
        file_pattern = "\\.xls$",
        get_data_fn  = .get_data_pp,
        date_from    = date_from,
        date_to      = date_to,
        verbose      = verbose
    )
}


#' Carga temperaturas diarias extremas DGA desde una carpeta de archivos XLS.
#'
#' @param path      Ruta a la carpeta con archivos DGA_Temperaturas_Diarias_Extremas.xls
#' @param temp      Tipo: "tx" (maxima) o "tn" (minima)
#' @param date_from Fecha inicio de la grilla de salida (default "1990-01-01")
#' @param date_to   Fecha fin de la grilla de salida   (default "2021-12-31")
#' @param verbose   Imprimir progreso en consola        (default TRUE)
#' @return Lista con $data (data.frame fechas x estaciones) y $meta (metadatos)
parser_dga_daily_temp <- function(path,
                                  temp      = c("tx", "tn"),
                                  date_from = "1990-01-01",
                                  date_to   = "2021-12-31",
                                  verbose   = TRUE) {
    temp <- match.arg(temp)
    .dga_base(
        path         = path,
        xls_range    = "B1:P278",
        file_pattern = "\\.xls$",
        get_data_fn  = function(d) .get_data_temp(d, temp),
        date_from    = date_from,
        date_to      = date_to,
        verbose      = verbose
    )
}
