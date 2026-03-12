readRenviron(".env")
DATA_RAW_DGA  <- Sys.getenv("DATA_RAW_DGA")

# Run this interactively to diagnose
files <- list.files(
    file.path(DATA_RAW_DGA, "PRECIPITACIONES"),
    pattern = "\\.xls$", recursive = TRUE, full.names = TRUE
)

sheets_por_archivo <- lapply(files, \(f) {
    data.frame(
        archivo  = basename(f),
        n_hojas  = length(readxl::excel_sheets(f)),
        estaciones = paste(readxl::excel_sheets(f), collapse = ", ")
    )
})
do.call(rbind, sheets_por_archivo)


