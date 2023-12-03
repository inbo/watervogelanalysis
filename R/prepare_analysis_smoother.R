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
    "0", "month", "1",
"f(cyear, model = \"rw2\", scale.model = TRUE,
  hyper = list(theta = list(prior = \"pc.prec\", param = c(1, 0.01)))
)")[c(month, month, !month, TRUE)] |>
    paste(collapse = " +\n") |>
    sprintf(fmt = "~ %s") -> formula
  prepare_model_args_fun <- function(model) {
    if (nrow(model@AggregatedImputed@Covariate) == 0) {
      return(NULL)
    }
    stopifnot(requireNamespace("INLA", quietly = TRUE))
    winters <- sort(unique(model@AggregatedImputed@Covariate$year))
    if (
      "month" %in% colnames(model@AggregatedImputed@Covariate) &&
      length(unique(model@AggregatedImputed@Covariate$month)) > 1
    ) {
      months <- unique(model@AggregatedImputed@Covariate$month)
      (1 / length(months)) |>
        rep(length(winters)) |>
        list() |>
        rep(length(months)) |>
        setNames(paste0("month", months)) |>
        c(list(cyear = diag(length(winters)))) |>
        INLA::inla.make.lincombs() |>
        setNames(paste("total:", winters)) -> lc1
    } else {
      length(winters) |>
        diag() |>
        list() |>
        setNames("cyear") |>
        c("(Intercept)" = list(rep(1, length(winters)))) |>
        INLA::inla.make.lincombs() |>
        setNames(paste("total:", winters)) -> lc1
    }
    return(list(lincomb = lc1))
  }
  x <- n2k_model_imputed(
    result_datasource_id = aggregation@AnalysisMetadata$result_datasource_id,
    scheme_id = aggregation@AnalysisMetadata$scheme_id,
    species_group_id = aggregation@AnalysisMetadata$species_group_id,
    location_group_id = aggregation@AnalysisMetadata$location_group_id,
    model_type = c("smoothed imputed index: Total ~ Year", "Month"[month]) |>
      paste(collapse = " + "),
    formula = formula, model_fun = "INLA::inla",
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
