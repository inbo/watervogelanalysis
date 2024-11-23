#' Prepare the analysis files for the composite indices
#' @inheritParams n2kanalysis::store_model
#' @param models a dataframe with the parent models.
#' @export
#' @importFrom assertthat assert_that is.string has_name noNA
#' @importFrom n2kanalysis n2k_composite store_model
prepare_analysis_composite <- function(
  species_group_id, location_group_id, models, base, project,
  scheme_id, seed = 20070315, overwrite = FALSE, verbose = TRUE
) {
  assert_that(
    is.string(species_group_id), is.string(location_group_id),
    noNA(species_group_id), noNA(location_group_id),
    inherits(models, "data.frame"), has_name(models, "parent_analysis"),
    has_name(models, "parent_status"),
    has_name(models, "parentstatus_fingerprint"),
    has_name(models, "first_imported_year"),
    has_name(models, "last_imported_year"), has_name(models, "analysis_date")
  )
  display(
    verbose = verbose, linefeed = FALSE,
    message = c(location_group_id, " ", species_group_id)
  )

  extractor <- function(model) {
    results <- slot(model, "Results")
    data.frame(
      value = results$Parameter, estimate = results$Estimate,
      variance = results$SE ^ 2
    )
  }

  x <- n2k_composite(
    result_datasource_id = "watervogels", parent_status = models,
    seed = seed, scheme_id = scheme_id, species_group_id = species_group_id,
    location_group_id = location_group_id, formula = "~year",
    first_imported_year = min(models$first_imported_year),
    last_imported_year = max(models$last_imported_year), status = "waiting",
    model_type = "composite index: ~year", extractor = extractor,
    analysis_date = max(models$analysis_date)
  )
  store_model(x, base = base, project = project, overwrite = overwrite)
  x@AnalysisRelation |>
    select(fingerprint = "analysis", parent = "parent_analysis")
}
