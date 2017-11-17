#' Generate the models to apply on the aggregated sets
#' @inheritParams prepare_analysis
#' @param aggregation the output of \code{prepare_analysis_aggregate}
#' @export
#' @importFrom assertthat assert_that has_name is.flag noNA
#' @importFrom n2kanalysis n2k_model_imputed
#' @importFrom dplyr arrange_
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
  if (verbose) {
    message("Yearly indices")
    aggregation <- arrange_(aggregation, ~FileFingerprint)
  }
  yearly <- lapply(
    seq_along(aggregation$FileFingerprint),
    function(i){
      if (verbose) {
        message("  ", aggregation[i, "FileFingerprint"])
      }
      object <- n2k_model_imputed(
        result.datasource.id = aggregation[i, "ResultDatasourceID"],
        scheme.id = aggregation[i, "SchemeID"],
        species.group.id = aggregation[i, "SpeciesGroupID"],
        location.group.id = aggregation[i, "LocationGroupID"],
        model.type = "yearly imputed index: Total ~ Year + fMonth",
        formula =
          "~ model(cYear, model = \"rw1\") + f(fMonth, model = \"iid\")",
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
        mutate = list(cYear = "Year - max(Year)"),
        model.args = list(family = "nbinomial")
      )
      store_model(object, base = analysis.path, project = "watervogels")
      data.frame(
        Fingerprint = get_file_fingerprint(object),
        Parent = aggregation[i, "FileFingerprint"],
        stringsAsFactors = FALSE
      )
    }
  )
  bind_rows(yearly)
}