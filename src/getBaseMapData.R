## METADATA -------------------------------------------------------------------
## nombre script    : getBaseMApaData.R
## proposito        : cargar capas espaciales necesarias para la base de datos
## autor            : Fernando Neira-Román, fneira.roman@gmail.com
## fecha creacion   : 2026-03-06


# DIRECTORIOS -----------------------------------------------------------------
.DIR_DATA_DPA_CL <- "D:/DATA/DPA_IDE_2023/"
.DIR_DATA_GADM <- 'D:/DATA/GADM_limites/'

library(tidyterra)
dpa_chl <- terra::vect(
    x = file.path(.DIR_DATA_DPA_CL, "REGIONES"),
    opts = "ENCODING=UTF-8"
) %>%
    terra::disagg() %>%
    terra::project("EPSG:32719") %>%
    dplyr::mutate(AREA_ha = terra::expanse(., unit="ha")) %>%
    dplyr::filter(AREA_ha > 20000) %>%
    terra::project("EPSG:4326")

dpa_arg <- ''
dpa_per <- ''
dpa_bol <- ''


plot(dpa_cl)
