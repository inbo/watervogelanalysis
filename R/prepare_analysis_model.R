#' Generate the models to apply on the aggregated sets
#' @export
#' @importFrom assertthat assert_that has_name is.flag noNA
#' @importFrom n2kanalysis n2k_model_imputed
prepare_analysis_model <- function(
  aggregation,
  analysis.path,
  verbose = TRUE
) {
  assert_that(inherits(aggregation, "data.frame"))
  assert_that(is.flag(verbose))
  assert_that(noNA(verbose))

  requireNamespace("INLA", quietly = TRUE)
  # yearly index
  yearly <- lapply(
    seq_along(aggregation$FileFingerprint),
    function(i){
      n2k_model_imputed(
        result.datasource.id = aggregation[i, "ResultDatasourceID"],
        scheme.id = aggregation[i, "SchemeID"],
        species.group.id = aggregation[i, "SpeciesGroupID"],
        location.group.id = aggregation[i, "LocationGroupID"],
        model.type = "yearly imputed index: Total ~ Year + fMonth",
        formula = "~ 0 + fYear + f(fMonth, model = \"iid\")",
        first.imported.year = aggregation[i, "FirstImportedYear"],
        last.imported.year = aggregation[i, "LastImportedYear"],
        duration = aggregation[i, "Duration"],
        last.analysed.year = aggregation[i, "LastAnalysedYear"],
        analysis.date = aggregation[i, "AnalysisDate"],
        parent = aggregation[i, "FileFingerprint"],
        parent.status = aggregation[i, "Status"],
        parent.statusfingerprint = aggregation[i, "StatusFingerprint"],
        model.fun = INLA::inla,
        package = "INLA",
        extractor =  function(model){
          fe <- model$summary.fixed[, c("mean", "sd")]
          log.index <- fe[grepl("fYear", rownames(fe)), ]
          log.index[, "mean"] <- log.index[, "mean"] - log.index[1, "mean"]
          log.index
        },
        mutate = list(fYear = "factor(Year)"),
        model.args = list(family = "nbinomial")
      )
    }
  )
  sapply(yearly, store_model, base = analysis.path, project = "watervogels")
}
