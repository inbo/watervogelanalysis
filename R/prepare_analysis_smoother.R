#' Fit a smoother to an aggregated set
#' @inheritParams prepare_analysis
#' @inheritParams prepare_analysis_index
#' @export
#' @importFrom assertthat assert_that
#' @importFrom dplyr arrange select left_join
#' @importFrom n2kanalysis n2k_model_imputed get_file_fingerprint store_model
#' @importFrom Matrix sparseMatrix
#' @importFrom rlang .data
prepare_analysis_smoother <- function(
  aggregation, analysis_path, month = TRUE, verbose = TRUE
) {
  assert_that(
    inherits(aggregation, "n2kAggregate"),
    requireNamespace("INLA", quietly = TRUE)
  )
  display(
    verbose, linefeed = FALSE,
    sprintf(
      "%s-%s%s ", aggregation@AnalysisMetadata$location_group_id,
      aggregation@AnalysisMetadata$species_group_id,
      ifelse(month, "-month", "")
    )
  )
  extractor_fun <- function(model) {
    rbind(
      model$summary.lincomb.derived[, c("mean", "sd")],
      model$summary.random$cyear[, c("mean", "sd")]
    )
  }
  c(
"~ f(cyear, model = \"rw2\", scale.model = TRUE,
  hyper = list(theta = list(prior = \"pc.prec\", param = c(0.1, 0.01)))
)",
"f(month, model = \"iid\", constr = TRUE,
  hyper = list(theta = list(prior = \"pc.prec\", param = c(0.6, 0.01)))
)")[c(TRUE, month)] |>
    paste(collapse = " +\n") -> formula
  prepare_model_args_fun <- function(model) {
    stopifnot(requireNamespace("INLA", quietly = TRUE))
    winters <- sort(unique(model@AggregatedImputed@Covariate$year))
    lc1 <- INLA::inla.make.lincombs(
      "(Intercept)" = rep(1, length(winters)), cyear = diag(length(winters))
    )
    names(lc1) <- paste("total:", winters)
    return(list(lincomb = lc1))
  }
  x <- n2k_model_imputed(
    result_datasource_id = aggregation@AnalysisMetadata$result_datasource_id,
    scheme_id = aggregation@AnalysisMetadata$scheme_id,
    species_group_id = aggregation@AnalysisMetadata$species_group_id,
    location_group_id = aggregation@AnalysisMetadata$location_group_id,
    model_type = c("smoothed imputed index: Total ~ Year", "Month"[month]) |>
      paste(collapse = " + "),
    formula = formula, model_fun = INLA::inla,
    first_imported_year = aggregation@AnalysisMetadata$first_imported_year,
    last_imported_year = aggregation@AnalysisMetadata$last_imported_year,
    duration = aggregation@AnalysisMetadata$duration,
    last_analysed_year = aggregation@AnalysisMetadata$last_analysed_year,
    analysis_date = aggregation@AnalysisMetadata$analysis_date,
    seed = aggregation@AnalysisMetadata$seed, package = c("INLA", "dplyr"),
    extractor = extractor_fun, mutate = list(cyear = "year - max(year)"),
    model_args = list(family = "nbinomial"),
    prepare_model_args = list(prepare_model_args_fun),
    parent = aggregation@AnalysisMetadata$file_fingerprint
  )
  store_model(
    x, base = analysis_path, project = "watervogels", overwrite = FALSE
  )
  x@AnalysisRelation |>
    select(fingerprint = "analysis", parent = "parent_analysis")
}
