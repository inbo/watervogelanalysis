#' Create aggregation objects for imputed counts
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom dplyr %>% rowwise do pull select mutate
#' @importFrom n2kanalysis n2k_aggregate store_model get_file_fingerprint
#' validObject
#' @importFrom rlang .data
#' @inheritParams prepare_analysis_imputation
#' @inheritParams prepare_analysis_index
#' @inheritParams prepare_dataset
prepare_analysis_agg_max <- function(
  aggregation, analysis_path, verbose = TRUE
) {
  assert_that(inherits(aggregation, "n2kAggregate"), validObject(aggregation))
  display(
    verbose, linefeed = FALSE,
    sprintf(
      "%s-%s ", aggregation@AnalysisMetadata$location_group_id,
      aggregation@AnalysisMetadata$species_group_id
    )
  )
  object <- n2k_aggregate(
    result_datasource_id = aggregation@AnalysisMetadata$result_datasource_id,
    scheme_id = aggregation@AnalysisMetadata$scheme_id,
    species_group_id = aggregation@AnalysisMetadata$species_group_id,
    location_group_id = aggregation@AnalysisMetadata$location_group_id,
    model_type = "aggregate imputed: max ~ year",
    formula = "~year", fun = max,
    parent = aggregation@AnalysisMetadata$file_fingerprint,
    first_imported_year = aggregation@AnalysisMetadata$first_imported_year,
    last_imported_year = aggregation@AnalysisMetadata$last_imported_year,
    duration = aggregation@AnalysisMetadata$duration,
    last_analysed_year = aggregation@AnalysisMetadata$last_analysed_year,
    analysis_date = aggregation@AnalysisMetadata$analysis_date,
    seed = aggregation@AnalysisMetadata$seed,
    status = "waiting"
  )
  store_model(
    object, base = analysis_path, project = "watervogels", overwrite = FALSE
  )
  return(object)
}
