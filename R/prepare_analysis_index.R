#' Generate the models to apply on the aggregated sets
#' @inheritParams prepare_analysis
#' @param aggregation an `n2kAggregate` object
#' @param month Model the month effect.
#' Defaults to `TRUE`.
#' @export
#' @importFrom assertthat assert_that
#' @importFrom dplyr arrange left_join select
#' @importFrom n2kanalysis n2k_model_imputed get_file_fingerprint store_model
#' @importFrom Matrix sparseMatrix
#' @importFrom rlang .data
#' @importFrom stats setNames
prepare_analysis_index <- function(
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
  extractor_fun_month <- function(model) {
    rbind(
      model$summary.lincomb.derived[, c("mean", "sd")],
      model$summary.random$cyear[, c("mean", "sd")],
      model$summary.fixed[, c("mean", "sd")]
    )
  }
  extractor_fun <- function(model) {
    rbind(
      model$summary.lincomb.derived[, c("mean", "sd")],
      model$summary.random$cyear[, c("mean", "sd")]
    )
  }
  c(
    "0", "month", "1",
"f(cyear, model = \"rw1\", scale.model = TRUE,
  hyper = list(theta = list(prior = \"pc.prec\", param = c(2, 0.01)))
)"
)[c(month, month, !month, TRUE)] |>
    paste(collapse = " +\n") |>
    sprintf(fmt = "~ %s") -> formula
  prepare_model_args_fun <- function(model) {
    if (nrow(model@AggregatedImputed@Covariate) == 0) {
      return(NULL)
    }
    stopifnot(requireNamespace("INLA", quietly = TRUE))
    moving_trend <- function(n_year, trend_year, first_year) {
      trend_year <- min(n_year, trend_year)
      trend_coef <- seq_len(trend_year) - (trend_year + 1) / 2
      trend_coef <- trend_coef / sum(trend_coef ^ 2)
      lc <- vapply(
        seq_len(n_year - trend_year + 1),
        function(i) {
          c(rep(0, i - 1), trend_coef, rep(0, n_year - trend_year - i + 1))
        },
        numeric(n_year)
      )
      colnames(lc) <- sprintf(
        "trend_%.1f_%i",
        first_year + seq_len(ncol(lc)) - 1 + (trend_year - 1) / 2,
        trend_year
      )
      t(lc)
    }
    moving_average <- function(n_year, trend_year, first_year) {
      trend_year <- min(n_year, trend_year)
      vapply(
        seq_len(n_year - trend_year + 1) - 1,
        FUN.VALUE = vector(mode = "numeric", length = n_year),
        FUN = function(i, trend_coef, n_year) {
          c(rep(0, i), trend_coef, rep(0, n_year - length(trend_coef) - i))
        }, trend_coef = rep(1 / trend_year, trend_year), n_year = n_year
      ) |>
        `colnames<-`(
          sprintf(
            "average_%.1f_%i",
            first_year + seq_len(n_year - trend_year + 1) - 1 +
              (trend_year - 1) / 2,
            trend_year
          )
        ) |>
        t()
    }
    moving_difference <- function(n_year, trend_year, first_year) {
      trend_year <- min(floor(n_year / 2), trend_year)
      list(seq_len(n_year - 2 * trend_year + 1) - 1) |>
        rep(2) |>
        expand.grid() -> extra_zero
      extra_zero <- extra_zero[
        rowSums(extra_zero) <= n_year - 2 * trend_year,
      ]
      vapply(
        seq_len(nrow(extra_zero)),
        FUN.VALUE = vector(mode = "numeric", length = n_year),
        FUN = function(i, trend_coef, n_year, extra_zero) {
          c(
            rep(0, extra_zero[i, 1]), -trend_coef,
            rep(0, n_year - 2 * length(trend_coef) - sum(extra_zero[i, ])),
            trend_coef, rep(0, extra_zero[i, 2])
          )
        }, trend_coef = rep(1 / trend_year, trend_year), n_year = n_year,
        extra_zero = extra_zero
      ) |>
        `colnames<-`(
          sprintf(
            "difference_%.1f_%.1f_%i",
            first_year + extra_zero[, 1] + trend_year / 2,
            first_year + n_year - 1 - trend_year / 2 - extra_zero[, 2],
            trend_year
          )
        ) |>
        t()
    }
    apply(model@AggregatedImputed@Imputation, 1, max) |>
      aggregate(by = model@AggregatedImputed@Covariate["year"], FUN = max) -> mi
    mi <- mi[order(mi$year), ]
    winters <- mi$year[cumsum(mi$x) > 0 & rev(cumsum(rev(mi$x))) > 0]
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
      months <- character(0)
      length(winters) |>
        diag() |>
        list() |>
        setNames("cyear") |>
        c("(Intercept)" = list(rep(1, length(winters)))) |>
        INLA::inla.make.lincombs() |>
        setNames(paste("total:", winters)) -> lc1
    }
    comb <- expand.grid(
      winter1 = factor(winters), winter2 = factor(winters)
    )
    comb <- comb[as.integer(comb$winter1) < as.integer(comb$winter2), ]
    comb$label <- sprintf("index: %s-%s", comb$winter2, comb$winter1)
    nrow(comb) |>
      seq_len() |>
      rep(2) |>
      sparseMatrix(
        j = c(as.integer(comb$winter2), as.integer(comb$winter1)),
        x = rep(c(1, -1), each = nrow(comb))
      ) |>
      list() |>
      setNames("cyear") |>
      INLA::inla.make.lincombs() |>
      setNames(comb$label) -> lc2
    moving_trend(
      n_year = length(winters), trend_year = 10, first_year = min(winters)
    ) |>
      rbind(
        moving_trend(
          n_year = length(winters), trend_year = 12,
          first_year = min(winters)
        ),
        moving_trend(
          n_year = length(winters), trend_year = length(winters),
          first_year = min(winters)
        ),
        moving_difference(
          n_year = length(winters), trend_year = 10,
          first_year = min(winters)
        )
      ) |>
      unique() -> lc3
    INLA::inla.make.lincombs(cyear = lc3) |>
      setNames(rownames(lc3)) -> lc3
    moving_average(
      n_year = length(winters), trend_year = 5,
      first_year = min(winters)
    ) |>
      rbind(
        moving_average(
          n_year = length(winters), trend_year = 10,
          first_year = min(winters)
        )
      ) -> ma
    if (length(months) > 1) {
      (1 / length(months)) |>
        rep(nrow(ma)) |>
        list() |>
        rep(length(months)) |>
        setNames(paste0("month", months)) |>
        c(list(cyear = ma)) |>
        INLA::inla.make.lincombs() |>
        setNames(rownames(ma)) -> lc4
    } else {
      list("(Intercept)" = rep(1, nrow(ma)), cyear = ma) |>
        INLA::inla.make.lincombs() |>
        setNames(rownames(ma)) -> lc4
    }
    return(list(lincomb = c(lc1, lc2, lc3, lc4)))
  }
  x <- n2k_model_imputed(
    result_datasource_id = aggregation@AnalysisMetadata$result_datasource_id,
    scheme_id = aggregation@AnalysisMetadata$scheme_id,
    species_group_id = aggregation@AnalysisMetadata$species_group_id,
    location_group_id = aggregation@AnalysisMetadata$location_group_id,
    model_type = c("yearly imputed index: Total ~ Year", "Month"[month]) |>
      paste(collapse = " + "),
    formula = formula, model_fun = "INLA::inla",
    first_imported_year = aggregation@AnalysisMetadata$first_imported_year,
    last_imported_year = aggregation@AnalysisMetadata$last_imported_year,
    duration = aggregation@AnalysisMetadata$duration,
    last_analysed_year = aggregation@AnalysisMetadata$last_analysed_year,
    analysis_date = aggregation@AnalysisMetadata$analysis_date,
    seed = aggregation@AnalysisMetadata$seed,
    package = c("INLA", "dplyr"),
    extractor = ifelse(month, extractor_fun_month, extractor_fun),
    mutate = list(cyear = "year - max(year)"),
    filter = list(function(z) {
      z |>
        dplyr::group_by(.data$year) |>
        dplyr::summarise(Imputation_max = max(.data$Imputation_max)) |>
        dplyr::arrange(.data$year) |>
        dplyr::filter(
          cumsum(.data$Imputation_max) > 0,
          rev(cumsum(rev(.data$Imputation_max))) > 0
        ) |>
        dplyr::semi_join(x = z, by = "year")
    }),
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
