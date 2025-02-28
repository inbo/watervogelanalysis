#' Prepare all datasets and a to do list of models
#' @inheritParams prepare_analysis_imputation
#' @inheritParams prepare_dataset
#' @export
#' @importFrom dplyr bind_rows distinct inner_join transmute
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
  docker = "inbobmk/rn2k:dev-0.10",
  dependencies = c(
    "inbo/multimput@v0.2.15", "inbo/n2khelper@v0.5.0",
    "inbo/n2kanalysis@v0.4.0"
  )
) {
  set.seed(seed)
  file.path("location", "location") |>
    verify_vc(root = raw_repo, variables = c("id", "start_date", "end_date")) |>
    transmute(
      .data$id,
      start_year = round_date(.data$start_date, unit = "year") |>
        year(),
      end_year = round_date(.data$end_date, unit = "year") |>
        year()
    ) -> location

  file.path("location", "locationgroup") |>
    verify_vc(root = raw_repo, variables = c("impute", "subset_months")) |>
    distinct(locationgroup = .data$impute, .data$subset_months) |>
    inner_join(
      file.path("location", "locationgroup_location") |>
        verify_vc(root = raw_repo, variables = c("locationgroup", "location")),
      by = "locationgroup"
    ) |>
    inner_join(location, by = c("location" = "id")) -> location

  display(verbose, "Prepare imputations")

  file.path("species", "speciesgroup_species") |>
    verify_vc(
      root = raw_repo, variables = c("speciesgroup", "species")
    ) -> speciesgroupspecies
  speciesgroupspecies |>
    filter(grepl("[0-9]{2,5}", .data$speciesgroup)) |>
    nest(.by = "speciesgroup") |>
    arrange(as.integer(.data$speciesgroup)) |>
    transmute(
      speciesgroup = map2(
        .data$speciesgroup, .data$data,
        ~data.frame(species = .y$species, speciesgroup = .x)
      )
    ) |>
    pull("speciesgroup") |>
    map_dfr(
      prepare_analysis_imputation, location = location,
      seed = seed, analysis_path = analysis_path, raw_repo = raw_repo,
      verbose = verbose
    ) |>
    mutate(
      month = map_lgl(
        .data$count,
        function(x) {
          if (is.null(x)) {
            return(NA)
          }
          slot(x, "AnalysisMetadata") |>
            pull(.data$formula) |>
            grepl(pattern = "\nmonth +")
        }
      )
    ) |>
    filter(!is.na(.data$month)) -> imputations
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
      ),
      .data$month
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
      .data$hurdle, .data$month,
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

  display(verbose, "\nTrends index")
  relevant |>
    transmute(
      fingerprint = map2(
        .data$aggregated, .data$month, prepare_analysis_index,
        analysis_path = analysis_path, verbose = verbose
      )
    ) |>
    unnest("fingerprint") -> trends

  display(verbose, "\nTrends smoother")
  relevant |>
    transmute(
      fingerprint = map2(
        .data$aggregated, .data$month, prepare_analysis_smoother,
        analysis_path = analysis_path, verbose = verbose
      )
    ) |>
    unnest("fingerprint") |>
    bind_rows(trends, manifest) -> manifest

  display(verbose, "\nWintermaxima aggregation")
  relevant |>
    transmute(
      aggregated = map(
        .data$aggregated, prepare_analysis_agg_max,
        analysis_path = analysis_path, verbose = verbose
      )
    ) -> wintermax

  display(verbose, "\nWintermaxima trend")
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
    ) -> manifest

  display(verbose, "\nComposite trends")
  trends |>
    inner_join(speciesgroupspecies, by = "speciesgroup") |>
    select(-"speciesgroup", -"parent", parent_analysis = "fingerprint") |>
    inner_join(
      speciesgroupspecies |>
        filter(!grepl("[0-9]{3,5}", .data$speciesgroup)),
      by = "species", relationship = "many-to-many"
    ) |>
    nest(.by = c("speciesgroup", "locationgroup")) |>
    transmute(
      fingerprint = pmap(
        list(
          species_group_id = .data$speciesgroup,
          location_group_id = .data$locationgroup,
          models = .data$data
        ),
        prepare_analysis_composite, base = analysis_path,
        verbose = verbose, project = "watervogels", seed = seed,
        scheme_id = "watervogels"
      )
    ) |>
    unnest("fingerprint") |>
    bind_rows(manifest) -> manifest

  display(verbose, "\nCreate manifest")
  manifest |>
    select("fingerprint", "parent") |>
    n2k_manifest() |>
    store_manifest_yaml(
      base = analysis_path, project = "watervogels", docker = docker,
      dependencies = dependencies
    )
}
