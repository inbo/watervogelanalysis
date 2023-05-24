#' Create aggregation objects for imputed counts
#' @export
#' @importFrom assertthat assert_that has_name is.string noNA
#' @importFrom dplyr filter mutate pull select
#' @importFrom git2rdata read_vc
#' @importFrom purrr map map_chr map_dfr pmap
#' @importFrom n2kanalysis display n2k_aggregate store_model
#' @inheritParams prepare_analysis_imputation
#' @inheritParams prepare_dataset
#' @param imputations a data.frame with the imputations per location group
prepare_analysis_aggregate <- function(
  analysis_path, imputations, file_fingerprint, raw_repo, seed = 19790402,
  verbose = TRUE
) {
  set.seed(seed)
  assert_that(
    inherits(imputations, "data.frame"), has_name(imputations, "filename"),
    has_name(imputations, "species_group_id"),
    has_name(imputations, "location_group_id"),
    has_name(imputations, "scheme_id"),
    is.string(file_fingerprint), noNA(file_fingerprint)
  )

  location <- read_vc(file = "location/locationgroup_location", root = raw_repo)
  assert_that(
    has_name(location, "locationgroup"), has_name(location, "location")
  )

  display(verbose, paste("imputation:", file_fingerprint))
  imputations |>
    mutate(
      join = map(
        .data$location_group_id, ~filter(location, .data$locationgroup == .x)
      ) |>
        map(transmute, location = factor(.data$location)) |>
        map(as.data.frame, stringsAsFactors = FALSE),
      location_group_id = as.character(location_group_id),
      analysis = pmap(
        .l = list(
          result_datasource_id = .data$result_datasource_id, join = .data$join,
          species_group_id = .data$species_group_id,
          analysis_date = .data$analysis_date, scheme_id = .data$scheme_id,
          location_group_id = .data$location_group_id,
          first_imported_year = .data$first_imported_year,
          last_imported_year = .data$last_imported_year
        ),
        n2k_aggregate, status = "waiting", minimum = "minimum", seed = seed,
        model_type = "aggregate imputed: sum ~ year + month",
        formula = "~year + month", fun = sum, parent = file_fingerprint
      ),
      filename = map_chr(
        .data$analysis, store_model, base = analysis_path,
        project = "watervogels", overwrite = FALSE
      )
    ) |>
    pull("analysis") |>
    map_dfr(slot, "AnalysisMetadata") |>
    select(
      "result_datasource_id", "scheme_id", "species_group_id", "analysis_date",
      "location_group_id", "first_imported_year", "last_imported_year",
      "duration", "last_analysed_year", "status", "status_fingerprint",
      "file_fingerprint"
    ) |>
    mutate(parent = file_fingerprint)
}
