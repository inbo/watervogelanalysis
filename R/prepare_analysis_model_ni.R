#' Generate the models to apply on the aggregated sets that ignored missing data
#' @inheritParams prepare_analysis
#' @param aggregation the output of \code{prepare_analysis_aggregate}
#' @export
#' @importFrom assertthat assert_that has_name is.flag noNA
#' @importFrom n2kanalysis n2k_model_imputed
#' @importFrom dplyr arrange_
prepare_analysis_model_ni <- function(
  aggregation,
  analysis.path,
  seed = 19790402,
  verbose = TRUE
) {
  set.seed(seed)
  assert_that(inherits(aggregation, "data.frame"))
  assert_that(is.flag(verbose))
  assert_that(noNA(verbose))

  # yearly index
  if (verbose) {
    message("All-time maximum without imputation")
    aggregation <- arrange_(aggregation, ~FileFingerprint)
  }
  tot_max <- lapply(
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
        model.type = "all-time maximum: Total ~ 1",
        formula = "~ 1",
        first.imported.year = aggregation[i, "FirstImportedYear"],
        last.imported.year = aggregation[i, "LastImportedYear"],
        duration = aggregation[i, "Duration"],
        last.analysed.year = aggregation[i, "LastAnalysedYear"],
        analysis.date = aggregation[i, "AnalysisDate"],
        seed = seed,
        parent = aggregation[i, "FileFingerprint"],
        parent.status = aggregation[i, "Status"],
        parent.statusfingerprint = aggregation[i, "StatusFingerprint"],
        model.fun = function(form, data, ...){
          dplyr::summarise(
            data,
            Estimate = max(.data$Imputed),
            SE = 0
          )
        },
        package = "dplyr",
        extractor =  function(x){x},
        model.args = list()
      )
      store_model(
        object,
        base = analysis.path,
        project = "watervogels",
        overwrite = FALSE
      )
      data.frame(
        Fingerprint = get_file_fingerprint(object),
        Parent = aggregation[i, "FileFingerprint"],
        stringsAsFactors = FALSE
      )
    }
  )
  if (verbose) {
    message("Monthly total without imputation")
  }
  monthly <- lapply(
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
        model.type = "monthly total: Total ~ Year + fMonth",
        formula = "~ Year + fMonth",
        first.imported.year = aggregation[i, "FirstImportedYear"],
        last.imported.year = aggregation[i, "LastImportedYear"],
        duration = aggregation[i, "Duration"],
        last.analysed.year = aggregation[i, "LastAnalysedYear"],
        analysis.date = aggregation[i, "AnalysisDate"],
        seed = seed,
        parent = aggregation[i, "FileFingerprint"],
        parent.status = aggregation[i, "Status"],
        parent.statusfingerprint = aggregation[i, "StatusFingerprint"],
        model.fun = function(form, data, ...){
          rownames(data) <- sprintf("Year%i:Month%s", data$Year, data$fMonth)
          cbind(data["Imputed"], SE = 0)
        },
        extractor =  function(x){x},
        model.args = list()
      )
      store_model(
        object,
        base = analysis.path,
        project = "watervogels",
        overwrite = FALSE
      )
      data.frame(
        Fingerprint = get_file_fingerprint(object),
        Parent = aggregation[i, "FileFingerprint"],
        stringsAsFactors = FALSE
      )
    }
  )
  bind_rows(tot_max, monthly)
}
