# ── Python (reticulate) — rutas por sistema operativo ─────────────────────
local({
    python_path <- if (.Platform$OS.type == "windows") {
        "C:/Users/fneira/.envs/global-env/Scripts/python.exe"
    } else {
        # macOS — micromamba: ajusta el nombre del entorno si es distinto
        path.expand("~/micromamba/envs/geoenv/bin/python")
    }
    if (file.exists(python_path)) {
        Sys.setenv(RETICULATE_PYTHON = python_path)
    }
})

# ── Opciones generales ─────────────────────────────────────────────────────
options(
    repos         = c(CRAN = "https://cloud.r-project.org"),
    width         = 100,
    scipen        = 999,
    digits        = 4,
    max.print     = 100,          # evita floods en consola con dataframes grandes
    warn          = 1,            # warnings inmediatos, no al final
    timeout       = 120           # descarga de paquetes/datos (default 60s es poco)
)
