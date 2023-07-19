#' Create aggregation objects for imputed counts while ignoring the missing data
#' @export
#' @importFrom assertthat assert_that
#' @importFrom dplyr filter inner_join mutate pull select
#' @importFrom git2rdata verify_vc
#' @importFrom n2kanalysis get_file_fingerprint n2k_aggregate store_model
#' @importFrom purrr map_chr
#' @importFrom tidyr nest
#' @inheritParams prepare_analysis_imputation
#' @inheritParams prepare_dataset
#' @param count An `n2kInla` object holding the count data.
prepare_analysis_aggregate_ni <- function(
  count, analysis_path, raw_repo, verbose = TRUE
) {
  assert_that(inherits(count, "n2kInla"))

  parent <- get_file_fingerprint(count)
  display(verbose, paste("no imputation", parent))

  verify_vc(
    file = "location/locationgroup", root = raw_repo,
    variables = c("id", "impute")
  ) |>
    filter(
      .data$impute == as.integer(count@AnalysisMetadata$location_group_id)
    ) |>
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
        result_datasource_id = count@AnalysisMetadata$result_datasource_id,
        scheme_id = count@AnalysisMetadata$scheme_id,
        species_group_id = count@AnalysisMetadata$species_group_id,
        model_type = "aggregate not imputed: sum ~ year + month",
        formula = "~year + month", fun = sum, filter = list("count > 0"),
        parent = count@AnalysisMetadata$file_fingerprint,
        first_imported_year = count@AnalysisMetadata$first_imported_year,
        last_imported_year = count@AnalysisMetadata$last_imported_year,
        duration = count@AnalysisMetadata$duration,
        last_analysed_year = count@AnalysisMetadata$last_analysed_year,
        analysis_date = count@AnalysisMetadata$analysis_date,
        status = "waiting"
      ),
      filename = map_chr(
        .data$analysis, store_model, base = analysis_path,
        project = "watervogels", overwrite = FALSE
      )
    ) |>
    pull("analysis") |>
    map_chr(get_file_fingerprint) -> fingerprint
  data.frame(fingerprint = fingerprint, parent = parent)
}
