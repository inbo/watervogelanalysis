#' Generate the models to apply on the aggregated sets
#' @inheritParams prepare_analysis
#' @param aggregation the output of \code{prepare_analysis_aggregate}
#' @export
#' @importFrom assertthat assert_that is.flag noNA
#' @importFrom n2kanalysis n2k_model_imputed get_file_fingerprint store_model
#' @importFrom dplyr %>% arrange left_join
#' @importFrom rlang .data
prepare_analysis_model <- function(aggregation, analysis_path, seed = 19790402,
                                   verbose = TRUE) {
  set.seed(seed)
  assert_that(inherits(aggregation, "data.frame"), is.flag(verbose),
              noNA(verbose))

  requireNamespace("INLA", quietly = TRUE)
  # yearly index
  if (verbose) {
    message("Yearly indices")
    aggregation <- arrange(aggregation, .data$FileFingerprint)
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
        model.type = "yearly imputed index: Total ~ Year + Month",
        formula =
          "~ f(cYear, model = \"rw1\", scale.model = TRUE,
  hyper = list(theta = list(prior = \"pc.prec\", param = c(0.1, 0.01)))
) +
f(Month, model = \"iid\", constr = TRUE,
  hyper = list(theta = list(prior = \"pc.prec\", param = c(0.6, 0.01)))
)",
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
        package = c("INLA", "dplyr"),
        extractor = function(model) {
          rbind(
            model$summary.lincomb.derived[, c("mean", "sd")],
            model$summary.random$cYear[, c("mean", "sd")]
          )
        },
        mutate = list(cYear = "Year - max(Year)"),
        prepare.model.args = list(
          function(model) {
            winters <- sort(unique(model@AggregatedImputed@Covariate$Year))
            lc1 <- inla.make.lincombs("(Intercept)" = rep(1, length(winters)),
                                      cYear = diag(length(winters)))
            names(lc1) <- paste("total:", winters)
            comb <- expand.grid(
              winter1 = factor(winters),
              winter2 = factor(winters)
            )
            comb <- comb[as.integer(comb$winter1) < as.integer(comb$winter2), ]
            comb$label <- sprintf("index: %s-%s", comb$winter2, comb$winter1)
            lc2 <- inla.make.lincombs(
              cYear = sparseMatrix(
                i = rep(seq_len(nrow(comb)), 2),
                j = c(as.integer(comb$winter2), as.integer(comb$winter1)),
                x = rep(c(1, -1), each = nrow(comb))
              )
            )
            names(lc2) <- comb$label
            return(list(lincomb = c(lc1, lc2)))
          }
        ),
        model.args = list(family = "nbinomial")
      )
      store_model(object, base = analysis_path, project = "watervogels",
                  overwrite = FALSE)
      tibble(Fingerprint = get_file_fingerprint(object),
             Parent = aggregation[i, "FileFingerprint"])
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
        model.type = "imputed trend: Total ~ Year + Month",
        formula = "~ cYear +
f(Month, model = \"iid\", constr = TRUE,
  hyper = list(theta = list(prior = \"pc.prec\", param = c(0.6, 0.01)))
)",
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
      store_model(object, base = analysis_path, project = "watervogels",
                  overwrite = FALSE)
      tibble(Fingerprint = get_file_fingerprint(object),
             Parent = aggregation[i, "FileFingerprint"])
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
        model.type = "imputed trend: Total ~ Year + Month",
        formula = "~ cYear +
f(Month, model = \"iid\", constr = TRUE,
  hyper = list(theta = list(prior = \"pc.prec\", param = c(0.6, 0.01)))
)",
        first.imported.year = aggregation[i, "FirstImportedYear"],
        last.imported.year = aggregation[i, "LastImportedYear"],
        duration = 12,
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
        filter = list("Year > max(Year) - 12"),
        mutate = list(cYear = "Year - max(Year)"),
        model.args = list(family = "nbinomial")
      )
      store_model(object, base = analysis_path, project = "watervogels",
                  overwrite = FALSE)
      tibble(Fingerprint = get_file_fingerprint(object),
             Parent = aggregation[i, "FileFingerprint"])
    }
  )
  bind_rows(yearly, longterm, shortterm)
}
