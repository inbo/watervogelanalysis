#' Prepare all datasets and a to do list of models
#' @inheritParams prepare_analysis_imputation
#' @inheritParams prepare_dataset
#' @export
#' @importFrom dplyr bind_rows distinct inner_join transmute
#' @importFrom fs path
#' @importFrom git2rdata verify_vc
#' @importFrom lubridate round_date year
#' @importFrom n2kanalysis display get_file_fingerprint manifest_yaml_to_bash
#' n2k_hurdle_imputed n2k_manifest store_manifest_yaml store_model
#' @importFrom methods slot
#' @importFrom purrr map_chr map_dfr
#' @importFrom rlang .data
#' @importFrom tidyr unnest
prepare_analysis <- function(
  analysis_path = ".", raw_repo, seed = 19790402, verbose = TRUE,
  knot_interval = 10
) {
  set.seed(seed)
  path("location", "location") |>
    verify_vc(root = raw_repo, variables = c("id", "start", "end")) |>
    transmute(
      .data$id,
      start_year = round_date(.data$start, unit = "year") |>
        year(),
      end_year = round_date(.data$end, unit = "year") |>
        year()
    ) -> location

  path("location", "locationgroup") |>
    verify_vc(root = raw_repo, variables = c("impute", "subset_month")) |>
    distinct(locationgroup = .data$impute, .data$subset_month) |>
    inner_join(
      path("location", "locationgroup_location") |>
        read_vc(root = raw_repo),
      by = "locationgroup"
    ) |>
    inner_join(location, by = c("location" = "id")) -> location

  display(verbose, "Prepare imputations")

  path("species", "speciesgroup_species") |>
    verify_vc(root = raw_repo, variables = c("speciesgroup", "species")) |>
    nest(.by = "speciesgroup") |>
    transmute(
      speciesgroup = map2(
        .data$speciesgroup, .data$data, ~mutate(.y, speciesgroup = .x)
      )
    ) |>
    pull("speciesgroup") |>
    map_dfr(
      prepare_analysis_imputation, location = location,
      seed = seed, analysis_path = analysis_path, raw_repo = raw_repo,
      verbose = verbose
    ) -> imputations
  imputations |>
    transmute(
      .data$count, fingerprint = map_chr(.data$count, get_file_fingerprint),
      parent = NA_character_
    ) -> manifest
  display(verbose, "\nDatasets without imputations")
  manifest |>
    transmute(
      no_impute = map(
        .data$count, prepare_analysis_aggregate_ni, verbose = verbose,
        analysis_path = analysis_path, raw_repo = raw_repo
      )
    ) |>
    unnest("no_impute") |>
    bind_rows(
      manifest |>
        select(-"count"),
      imputations |>
        transmute(
          fingerprint = map_chr(.data$presence, get_file_fingerprint),
          parent = NA_character_
        )
    ) -> manifest
  display(verbose, "\nHurdle model")
  imputations |>
    transmute(
      hurdle = map2(
        .data$presence, .data$count, n2k_hurdle_imputed, verbose = TRUE
      ),
      fingerprint = map_chr(
        .data$hurdle, store_model, base = analysis_path,
        project = "watervogels", overwrite = FALSE
      )
    ) -> relevant
  relevant |>
    transmute(
      parent = map(.data$hurdle, slot, "AnalysisRelation")
    ) |>
    unnest("parent") |>
    select(fingerprint = "analysis", parent = "parent_analysis") |>
    bind_rows(manifest) -> manifest
  display(verbose, "\nAggregations")
  relevant |>
    transmute(
      .data$hurdle,
      aggregated = map(
        .data$hurdle, prepare_analysis_aggregate,
        analysis_path = analysis_path, raw_repo = raw_repo, seed = seed,
        verbose = verbose
      )
    ) |>
    unnest("aggregated") -> relevant
  relevant |>
    transmute(
      parent = map(.data$aggregated, slot, "AnalysisRelation")
    ) |>
    unnest("parent") |>
    select(fingerprint = "analysis", parent = "parent_analysis") |>
    bind_rows(manifest) -> manifest
  display(verbose, "Trends")
  relevant |>
    transmute(
      fingerprint = map(
        .data$aggregated, prepare_analysis_index, month = TRUE,
        analysis_path = analysis_path, verbose = verbose
      )
    ) |>
    unnest("fingerprint") |>
    bind_rows(
      relevant |>
        transmute(
          fingerprint = map(
            .data$aggregated, prepare_analysis_smoother, month = TRUE,
            analysis_path = analysis_path, verbose = verbose
          )
        ) |>
        unnest("fingerprint"),
      manifest
    ) -> manifest
  display(verbose, "Wintermaxima aggregation")
  relevant |>
    transmute(
      aggregated = map(
        .data$aggregated, prepare_analysis_agg_max,
        analysis_path = analysis_path, verbose = verbose
      )
    ) -> wintermax
  display(verbose, "Wintermaxima trend")
  wintermax |>
    transmute(
      fingerprint = map(
        .data$aggregated, prepare_analysis_index, month = FALSE,
        analysis_path = analysis_path, verbose = verbose
      )
    ) |>
    unnest("fingerprint") |>
    bind_rows(
      wintermax |>
        transmute(
          fingerprint = map(.data$aggregated, slot, "AnalysisRelation")
        ) |>
        unnest("fingerprint") |>
        select(fingerprint = "analysis", parent = "parent_analysis"),
      wintermax |>
        transmute(
          fingerprint = map(
            .data$aggregated, prepare_analysis_smoother, month = FALSE,
            analysis_path = analysis_path, verbose = verbose
          )
        ) |>
        unnest("fingerprint"),
      manifest
    ) |>
    n2k_manifest() |>
    store_manifest_yaml(
      base = analysis_path, project = "watervogels",
      docker = "inbobmk/rn2k:0.9",
      dependencies = c(
        "inbo/n2khelper@v0.5.0", "inbo/multimput@v0.2.12",
        "inbo/n2kanalysis@v0.3.2"
      )
    ) |>
    map_chr("Key") |>
    basename() |>
    manifest_yaml_to_bash(
      base = analysis_path, project = "watervogels", shutdown = TRUE
    )
}
