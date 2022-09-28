#' Generate the models to apply on the aggregated sets
#' @inheritParams prepare_analysis
#' @param aggregation the output of `prepare_analysis_aggregate()`
#' @export
#' @importFrom assertthat assert_that is.flag noNA
#' @importFrom n2kanalysis n2k_model_imputed get_file_fingerprint store_model
#' @importFrom dplyr arrange
#' @importFrom rlang .data
prepare_analysis_model_max <- function(aggregation, analysis_path,
                                             seed = 19790402, verbose = TRUE) {
  set.seed(seed)
  assert_that(inherits(aggregation, "data.frame"), is.flag(verbose),
              noNA(verbose))

  requireNamespace("INLA", quietly = TRUE)
  if (verbose) {
      message("  Shortterm average")
    aggregation <- arrange(aggregation, .data$FileFingerprint)
  }
  shortterm <- lapply(
    seq_along(aggregation$FileFingerprint),
    function(i) {
      if (verbose) {
        message("    ", aggregation[i, "FileFingerprint"])
      }
      object <- n2k_model_imputed(
        result.datasource.id = aggregation[i, "ResultDatasourceID"],
        scheme.id = aggregation[i, "SchemeID"],
        species.group.id = aggregation[i, "SpeciesGroupID"],
        location.group.id = aggregation[i, "LocationGroupID"],
        model.type = "imputed average: Total ~ cPeriod",
        formula = "~ cPeriod",
        first.imported.year = aggregation[i, "FirstImportedYear"],
        last.imported.year = aggregation[i, "LastImportedYear"],
        duration = 10,
        last.analysed.year = aggregation[i, "LastAnalysedYear"],
        analysis.date = aggregation[i, "AnalysisDate"],
        seed = seed,
        parent = aggregation[i, "FileFingerprint"],
        parent.status = aggregation[i, "Status"],
        parent.statusfingerprint = aggregation[i, "StatusFingerprint"],
        model.fun = INLA::inla,
        package = "INLA",
        extractor =  function(model) {
          model$summary.fixed[, c("mean", "sd")]
        },
        filter = list("Year > max(Year) - 10"),
        mutate = list(cPeriod = "ceiling((Year - max(Year)) / 5)"),
        model.args = list(family = "nbinomial")
      )
      store_model(object, base = analysis_path, project = "watervogels",
                  overwrite = FALSE)
      tibble(Fingerprint = get_file_fingerprint(object),
             Parent = aggregation[i, "FileFingerprint"])
    }
  )
  bind_rows(shortterm)
}
