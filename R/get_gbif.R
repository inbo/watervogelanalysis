#' Get GBIF data for a species
#'
#' @param scientific The scientific name of the species
#' @param verbose If TRUE, display progress messages
#' @return A data frame with the vernacular names in different languages
#' @export
#' @importFrom assertthat assert_that is.string noNA
#' @importFrom dplyr group_by inner_join slice_sample transmute ungroup
#' @importFrom n2kanalysis display
#' @importFrom rlang .data
get_gbif <- function(
  scientific,
  language = c("nld", "eng", "fra", "deu"),
  verbose = TRUE
) {
  assert_that(
    is.string(scientific),
    noNA(scientific),
    is.character(language),
    length(language) >= 1,
    noNA(language),
    requireNamespace("rgbif", quietly = TRUE)
  )
  display(verbose = verbose, sprintf("Getting GBIF data for %s", scientific))
  i <- 0
  while (i < 10) {
    backbone <- try(rgbif::name_backbone(scientific, class = "Aves"))
    if (inherits(backbone, "data.frame")) {
      break
    }
    i <- i + 1
    Sys.sleep(i)
  }
  stopifnot(inherits(backbone, "data.frame"), nrow(backbone) == 1)
  i <- 0
  while (i < 10) {
    gbif_lookup <- try(rgbif::name_lookup(backbone$scientificName))
    if (inherits(backbone, "data.frame")) {
      break
    }
    i <- i + 1
    Sys.sleep(i)
  }
  stopifnot(inherits(gbif_lookup, "gbif"))
  if (as.character(backbone$usageKey) %in% names(gbif_lookup$names)) {
    these_name <- gbif_lookup$names[[as.character(backbone$usageKey)]]
  } else {
    bind_rows(gbif_lookup$names) |>
      group_by(.data$language) |>
      slice_head(n = 1) -> these_name
  }
  these_name |>
    inner_join(
      data.frame(language = language),
      by = "language"
    ) |>
    group_by(.data$language) |>
    slice_sample(n = 1) |>
    ungroup() |>
    transmute(
      vernacular = .data$vernacularName,
      .data$language,
      key = as.character(backbone$usageKey)
    )
}
