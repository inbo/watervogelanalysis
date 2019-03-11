#' Create aggregation objects for imputed counts
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom dplyr %>% rowwise do pull select mutate
#' @importFrom n2kanalysis n2k_aggregate store_model get_file_fingerprint
#' @importFrom rlang .data
#' @inheritParams prepare_analysis_imputation
#' @inheritParams prepare_analysis_model
#' @inheritParams prepare_dataset
prepare_analysis_aggregate_wintermax <- function(
  aggregation,
  analysis.path,
  seed = 19790402,
  verbose = TRUE
){
  assert_that(inherits(aggregation, "data.frame"))
  assert_that(has_name(aggregation, "Parent"))
  assert_that(has_name(aggregation, "ResultDatasourceID"))
  assert_that(has_name(aggregation, "SchemeID"))
  assert_that(has_name(aggregation, "SpeciesGroupID"))
  assert_that(has_name(aggregation, "LocationGroupID"))
  assert_that(has_name(aggregation, "FirstImportedYear"))
  assert_that(has_name(aggregation, "LastImportedYear"))
  assert_that(has_name(aggregation, "AnalysisDate"))
  assert_that(has_name(aggregation, "FileFingerprint"))
  assert_that(has_name(aggregation, "ResultDatasourceID"))

  models <- aggregation %>%
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
    unlist(recursive = FALSE)
  output <- aggregation %>%
    select(
      .data$ResultDatasourceID,
      .data$SchemeID,
      .data$SpeciesGroupID,
      .data$LocationGroupID,
      .data$FirstImportedYear,
      .data$LastImportedYear,
      .data$AnalysisDate,
      Imputation = .data$Parent,
      Parent = .data$FileFingerprint
    ) %>%
    mutate(
      FileFingerprint = sapply(models, get_file_fingerprint)
    )

  if (verbose) {
    message("Wintermaxima:")
    message("  aggregation")
    stored <- lapply(
      models[order(output$FileFingerprint)],
      function(x) {
        message("    ", get_file_fingerprint(x))
        store_model(
          x,
          base = analysis.path,
          project = "watervogels",
          overwrite = FALSE
        )
      }
    )
  } else {
    stored <- lapply(
      models,
      store_model,
      base = analysis.path,
      project = "watervogels",
      overwrite = FALSE
    )
  }
  return(output)
}
