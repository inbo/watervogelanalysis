#' Create aggregation objects for imputed counts
#' @export
#' @importFrom assertthat assert_that is.count noNA
#' @importFrom dplyr filter mutate pull select
#' @importFrom git2rdata verify_vc
#' @importFrom purrr map map_chr map_dfr pmap
#' @importFrom n2kanalysis display get_file_fingerprint get_location_group_id
#' n2k_aggregate store_model
#' @inheritParams prepare_analysis_imputation
#' @inheritParams prepare_dataset
#' @param hurdle An `n2kHurdleImputed` object.
prepare_analysis_aggregate <- function(
  hurdle, analysis_path, raw_repo, seed = 19790402, verbose = TRUE
) {
  assert_that(
    inherits(hurdle, "n2kHurdleImputed"), is.count(seed), noNA(seed)
  )
  set.seed(seed)

  display(
    verbose, paste("imputation:", hurdle@AnalysisMetadata$file_fingerprint)
  )

  verify_vc(
    file = "location/locationgroup", root = raw_repo,
    variables = c("id", "impute")
  ) |>
    filter(.data$impute == as.integer(get_location_group_id(hurdle))) |>
    select(location_group_id = "id") |>
    inner_join(
      verify_vc(
        file = "location/locationgroup_location", root = raw_repo,
        variables = c("locationgroup", "location")
      ) |>
        mutate(location = as.character(.data$location)),
      by = c("location_group_id" = "locationgroup")
    ) |>
    nest(.by = "location_group_id", .key = "join") |>
    mutate(
      analysis = pmap(
        list(
          location_group_id = as.character(.data$location_group_id),
          join = .data$join
        ),
        n2k_aggregate,
        result_datasource_id = hurdle@AnalysisMetadata$result_datasource_id,
        scheme_id = hurdle@AnalysisMetadata$scheme_id,
        species_group_id = hurdle@AnalysisMetadata$species_group_id,
        model_type = "aggregate imputed: sum ~ year + month",
        formula = "~year + month", fun = sum,
        parent = hurdle@AnalysisMetadata$file_fingerprint,
        first_imported_year = hurdle@AnalysisMetadata$first_imported_year,
        last_imported_year = hurdle@AnalysisMetadata$last_imported_year,
        duration = hurdle@AnalysisMetadata$duration,
        last_analysed_year = hurdle@AnalysisMetadata$last_analysed_year,
        analysis_date = hurdle@AnalysisMetadata$analysis_date,
        status = "waiting"
      ),
      filename = map_chr(
        .data$analysis, store_model, base = analysis_path,
        project = "watervogels", overwrite = FALSE
      )
    ) |>
    pull("analysis")
}
