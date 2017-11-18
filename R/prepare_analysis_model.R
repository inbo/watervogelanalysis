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
  seed = 19790402,
  verbose = TRUE
) {
  set.seed(seed)
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
          "~ f(cYear, model = \"rw1\") + f(fMonth, model = \"iid\")",
        first.imported.year = aggregation[i, "FirstImportedYear"],
        last.imported.year = aggregation[i, "LastImportedYear"],
        duration = aggregation[i, "Duration"],
        last.analysed.year = aggregation[i, "LastAnalysedYear"],
        analysis.date = aggregation[i, "AnalysisDate"],
        seed = seed,
        parent = aggregation[i, "FileFingerprint"],
        parent.status = aggregation[i, "Status"],
        parent.statusfingerprint = aggregation[i, "StatusFingerprint"],
        model.fun = INLA::inla,
        package = "INLA",
        extractor =  function(model){
          re <- model$summary.random$cYear[, c("ID", "mean", "sd")] %>%
            left_join(
              unique(model$.args$data[, c("Year", "cYear")]),
              by = c("ID" = "cYear")
            )
          rownames(re) <- paste0("Year", re$Year)
          re[, c("mean", "sd")]
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
  if (verbose) {
    message("Longterm trend")
  }
  longterm <- lapply(
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
        model.type = "imputed trend: Total ~ Year + fMonth",
        formula = "~ cYear + f(fMonth, model = \"iid\")",
        first.imported.year = aggregation[i, "FirstImportedYear"],
        last.imported.year = aggregation[i, "LastImportedYear"],
        duration = aggregation[i, "Duration"],
        last.analysed.year = aggregation[i, "LastAnalysedYear"],
        analysis.date = aggregation[i, "AnalysisDate"],
        seed = seed,
        parent = aggregation[i, "FileFingerprint"],
        parent.status = aggregation[i, "Status"],
        parent.statusfingerprint = aggregation[i, "StatusFingerprint"],
        model.fun = INLA::inla,
        package = "INLA",
        extractor =  function(model){
          model$summary.fixed[, c("mean", "sd")]
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
  if (verbose) {
    message("Shortterm trend")
  }
  shortterm <- lapply(
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
        model.type = "imputed trend: Total ~ Year + fMonth",
        formula = "~ cYear + f(fMonth, model = \"iid\")",
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
        extractor =  function(model){
          model$summary.fixed[, c("mean", "sd")]
        },
        filter = list("Year > max(Year) - 10"),
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
  bind_rows(yearly, longterm, shortterm)
}
