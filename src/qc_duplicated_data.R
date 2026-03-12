## =============================================================================
## qc_duplicated_data.R
## =============================================================================
## Propósito:
##   Verificar duplicados e inconsistencias en los datasets parseados antes
##   de la ingesta en DuckDB. Actúa sobre variables ya cargadas en el ambiente
##   (salidas de parser_dga y parser_inia).
##
## Variables requeridas en el ambiente:
##   dga_pp, dga_tx, dga_tn  — salida de parser_dga()
##   inia_all                 — salida de parser_inia()
##
## Formato de salida de parser_dga():
##   $data : data.frame ancho — fecha, año, mes, dia, <estacion_1>, ..., <estacion_N>
##   $meta : data.frame       — nombre, bna, cuenca, subcuenca, altura,
##                              latitud, longitud, utmN, utmE, lat, lon
##
## Formato de salida de parser_inia():
##   $data : tibble largo     — DATE, SOURCE, TA_MIN, TA_MAX, HR_AVG, PP_SUM, RD_AVG + QC
##   $meta : data.frame       — source, region, id, nombre, date_from, date_to
##
## Dependencias: dplyr
## =============================================================================

.qc_header <- function(label) {
    cat("\n", strrep("=", 70), "\n", sep = "")
    cat(" QC:", label, "\n")
    cat(strrep("=", 70), "\n", sep = "")
}

.qc_ok  <- function(msg) cat("  [OK]   ", msg, "\n", sep = "")
.qc_warn <- function(msg) cat("  [WARN] ", msg, "\n", sep = "")


## -----------------------------------------------------------------------------
## QC DGA: verifica $meta y $data de un dataset DGA (pp, tx o tn)
## -----------------------------------------------------------------------------
qc_dga <- function(dataset, label = "DGA") {
    .qc_header(label)

    meta <- dataset$meta
    data <- dataset$data

    # -- Meta: filas totales ---------------------------------------------------
    cat("  Estaciones en $meta:", nrow(meta), "\n")

    # -- Meta: nombres duplicados (mismo nombre, distinta fila) ----------------
    dup_nombres <- meta$nombre[duplicated(meta$nombre)]
    if (length(dup_nombres) == 0) {
        .qc_ok("Sin nombres de estacion duplicados en $meta")
    } else {
        .qc_warn(paste0(
            length(dup_nombres), " nombre(s) duplicado(s) en $meta: ",
            paste(unique(dup_nombres), collapse = ", ")
        ))
        print(meta[meta$nombre %in% dup_nombres, c("nombre", "bna", "lat", "lon")])
    }

    # -- Meta: near-duplicates por coordenadas (lat/lon identicos, nombre distinto) --
    coord_dup <- meta[duplicated(meta[, c("lat", "lon")]) |
                      duplicated(meta[, c("lat", "lon")], fromLast = TRUE), ]
    if (nrow(coord_dup) == 0) {
        .qc_ok("Sin estaciones con coordenadas duplicadas en $meta")
    } else {
        .qc_warn(paste0(nrow(coord_dup), " fila(s) comparten coordenadas en $meta:"))
        print(coord_dup[, c("nombre", "bna", "lat", "lon")])
    }

    # -- Meta: NA en coordenadas -----------------------------------------------
    na_coords <- sum(is.na(meta$lat) | is.na(meta$lon))
    if (na_coords == 0) {
        .qc_ok("Sin NA en lat/lon de $meta")
    } else {
        .qc_warn(paste0(na_coords, " fila(s) con lat o lon = NA en $meta"))
        print(meta[is.na(meta$lat) | is.na(meta$lon), c("nombre", "lat", "lon")])
    }

    # -- Data: columnas de estacion --------------------------------------------
    station_cols <- setdiff(names(data), c("fecha", "año", "mes", "dia"))
    cat("  Columnas de estacion en $data:", length(station_cols), "\n")

    # -- Data: nombres de columna duplicados -----------------------------------
    dup_cols <- station_cols[duplicated(station_cols)]
    if (length(dup_cols) == 0) {
        .qc_ok("Sin columnas duplicadas en $data")
    } else {
        .qc_warn(paste0(
            length(dup_cols), " columna(s) duplicada(s) en $data: ",
            paste(dup_cols, collapse = ", ")
        ))
    }

    # -- Cruce $meta vs $data --------------------------------------------------
    solo_en_meta <- setdiff(meta$nombre, station_cols)
    solo_en_data <- setdiff(station_cols, meta$nombre)
    if (length(solo_en_meta) == 0) {
        .qc_ok("Todas las estaciones de $meta tienen columna en $data")
    } else {
        .qc_warn(paste0(
            length(solo_en_meta), " estacion(es) en $meta sin columna en $data: ",
            paste(solo_en_meta, collapse = ", ")
        ))
    }
    if (length(solo_en_data) == 0) {
        .qc_ok("Todas las columnas de $data tienen entrada en $meta")
    } else {
        .qc_warn(paste0(
            length(solo_en_data), " columna(s) en $data sin entrada en $meta: ",
            paste(solo_en_data, collapse = ", ")
        ))
    }

    # -- Data: filas de fecha duplicadas ---------------------------------------
    dup_fechas <- sum(duplicated(data$fecha))
    if (dup_fechas == 0) {
        .qc_ok("Sin fechas duplicadas en $data")
    } else {
        .qc_warn(paste0(dup_fechas, " fecha(s) duplicada(s) en $data"))
    }

    # -- Data: cobertura por estacion (% no-NA) --------------------------------
    cat("  Cobertura de datos por estacion (% no-NA):\n")
    cobertura <- sapply(station_cols, function(col) {
        round(mean(!is.na(data[[col]])) * 100, 1)
    })
    cobertura_df <- data.frame(
        estacion   = names(cobertura),
        pct_no_na  = as.numeric(cobertura)
    )
    cobertura_df <- cobertura_df[order(cobertura_df$pct_no_na), ]
    print(cobertura_df, row.names = FALSE)

    invisible(NULL)
}


## -----------------------------------------------------------------------------
## QC INIA: verifica $meta y $data del dataset INIA
## -----------------------------------------------------------------------------
qc_inia <- function(dataset, label = "INIA") {
    .qc_header(label)

    meta <- dataset$meta
    data <- dataset$data

    # -- Meta: filas totales ---------------------------------------------------
    cat("  Estaciones en $meta:", nrow(meta), "\n")

    # -- Meta: source duplicado ------------------------------------------------
    dup_source <- meta$source[duplicated(meta$source)]
    if (length(dup_source) == 0) {
        .qc_ok("Sin source duplicado en $meta")
    } else {
        .qc_warn(paste0(
            length(dup_source), " source(s) duplicado(s) en $meta: ",
            paste(unique(dup_source), collapse = ", ")
        ))
    }

    # -- Meta: id duplicado ----------------------------------------------------
    dup_id <- meta$id[duplicated(meta$id)]
    if (length(dup_id) == 0) {
        .qc_ok("Sin id duplicado en $meta")
    } else {
        .qc_warn(paste0(
            length(dup_id), " id(s) duplicado(s) en $meta: ",
            paste(unique(dup_id), collapse = ", ")
        ))
        print(meta[meta$id %in% dup_id, ])
    }

    # -- Data: filas totales ---------------------------------------------------
    cat("  Filas en $data:", nrow(data), "\n")

    # -- Data: duplicados DATE x SOURCE ----------------------------------------
    dup_obs <- sum(duplicated(data[, c("DATE", "SOURCE")]))
    if (dup_obs == 0) {
        .qc_ok("Sin duplicados DATE x SOURCE en $data")
    } else {
        .qc_warn(paste0(dup_obs, " fila(s) duplicada(s) por DATE x SOURCE en $data"))
        dup_rows <- data[duplicated(data[, c("DATE", "SOURCE")]) |
                         duplicated(data[, c("DATE", "SOURCE")], fromLast = TRUE), ]
        print(dplyr::arrange(dup_rows, SOURCE, DATE))
    }

    # -- Data: SOURCE sin entrada en $meta -------------------------------------
    sources_sin_meta <- setdiff(unique(data$SOURCE), meta$source)
    if (length(sources_sin_meta) == 0) {
        .qc_ok("Todos los SOURCE de $data tienen entrada en $meta")
    } else {
        .qc_warn(paste0(
            length(sources_sin_meta), " SOURCE(s) en $data sin entrada en $meta: ",
            paste(sources_sin_meta, collapse = ", ")
        ))
    }

    # -- Data: cobertura PP_SUM por estacion -----------------------------------
    if ("PP_SUM" %in% names(data)) {
        cat("  Cobertura PP_SUM por estacion (% no-NA):\n")
        cobertura <- data |>
            dplyr::group_by(SOURCE) |>
            dplyr::summarise(
                n_total  = dplyr::n(),
                pct_no_na = round(mean(!is.na(PP_SUM)) * 100, 1),
                .groups = "drop"
            ) |>
            dplyr::arrange(pct_no_na)
        print(as.data.frame(cobertura), row.names = FALSE)
    }

    invisible(NULL)
}


## =============================================================================
## EJECUCION
## =============================================================================

required <- c("dga_pp", "dga_tx", "dga_tn", "inia_all")
missing  <- required[!sapply(required, exists)]
if (length(missing) > 0)
    stop("Variables no encontradas en el ambiente: ", paste(missing, collapse = ", "),
         "\nEjecuta normalizar_bbdd.R primero.")

qc_dga(dga_pp,  label = "DGA Precipitacion (dga_pp)")
qc_dga(dga_tx,  label = "DGA Temperatura Maxima (dga_tx)")
qc_dga(dga_tn,  label = "DGA Temperatura Minima (dga_tn)")
qc_inia(inia_all, label = "INIA Agrometeorologia (inia_all)")

cat("\n", strrep("=", 70), "\n", sep = "")
cat(" QC completado.\n")
cat(strrep("=", 70), "\n\n", sep = "")