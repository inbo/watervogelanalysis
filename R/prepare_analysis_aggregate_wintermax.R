#' Create aggregation objects for imputed counts
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom dplyr %>% rowwise do pull select mutate
#' @importFrom n2kanalysis n2k_aggregate store_model get_file_fingerprint
#' @importFrom rlang .data
#' @inheritParams prepare_analysis_imputation
#' @inheritParams prepare_analysis_model
#' @inheritParams prepare_dataset
prepare_analysis_agg_max <- function(
  aggregation, analysis_path, seed = 19790402, verbose = TRUE) {
  assert_that(
    inherits(aggregation, "data.frame"), has_name(aggregation, "Parent"),
    has_name(aggregation, "ResultDatasourceID"),
    has_name(aggregation, "SchemeID"), has_name(aggregation, "SpeciesGroupID"),
    has_name(aggregation, "LocationGroupID"),
    has_name(aggregation, "FirstImportedYear"),
    has_name(aggregation, "LastImportedYear"),
    has_name(aggregation, "AnalysisDate"),
    has_name(aggregation, "FileFingerprint"),
    has_name(aggregation, "ResultDatasourceID"))

  aggregation %>%
    rowwise() %>%
    do(
      Analysis = list(
        n2k_aggregate(
          status = "waiting",
          result.datasource.id = .data$ResultDatasourceID,
          scheme.id = .data$SchemeID,
          species.group.id = .data$SpeciesGroupID,
          location.group.id = .data$LocationGroupID,
          seed = seed,
          model.type = "aggregate imputed: max ~ Year",
          formula = "~Year",
          first.imported.year = .data$FirstImportedYear,
          last.imported.year = .data$LastImportedYear,
          analysis.date = .data$AnalysisDate,
          fun = max,
          parent = .data$FileFingerprint
        )
      )
    ) %>%
    dplyr::pull(.data$Analysis) %>%
    unlist(recursive = FALSE) -> models
  aggregation %>%
    select(
      "ResultDatasourceID", "SchemeID", "SpeciesGroupID", "LocationGroupID",
      "FirstImportedYear", "LastImportedYear", "AnalysisDate",
      Imputation = "Parent", Parent = "FileFingerprint") %>%
    mutate(
      FileFingerprint = sapply(models, get_file_fingerprint)
    ) -> output

  if (verbose) {
    message("Wintermaxima:")
    message("  aggregation")
    lapply(
      models[order(output$FileFingerprint)],
      function(x) {
        message("    ", get_file_fingerprint(x))
        store_model(
          x, base = analysis_path, project = "watervogels", overwrite = FALSE
        )
      }
    )
  } else {
    lapply(
      models, store_model, base = analysis_path, project = "watervogels",
      overwrite = FALSE
    )
  }
  return(output)
}
