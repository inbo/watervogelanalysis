#' Prepare all datasets and a to do list of models
#' @inheritParams prepare_analysis_imputation
#' @inheritParams prepare_dataset
#' @export
#' @importFrom dplyr distinct inner_join transmute
#' @importFrom fs path
#' @importFrom git2rdata read_vc
#' @importFrom lubridate round_date year
#' @importFrom n2kanalysis display
#' @importFrom rlang .data
prepare_analysis <- function(
  analysis_path = ".", raw_repo, seed = 19790402, verbose = TRUE
) {
  set.seed(seed)
  path("location", "location") |>
    read_vc(root = raw_repo) |>
    transmute(
      .data$id,
      start_year = round_date(.data$start, unit = "year") |>
        year(),
      end_year = round_date(.data$end, unit = "year") |>
        year()
    ) -> location

  path("location", "locationgroup") |>
    read_vc(root = raw_repo) |>
    distinct(locationgroup = .data$impute, .data$subset_month) |>
    inner_join(
      path("location", "locationgroup_location") |>
        read_vc(root = raw_repo),
      by = "locationgroup"
    ) |>
    inner_join(location, by = c("location" = "id")) -> location

  display(verbose, "Prepare imputations")

  path("species", "speciesgroup_species") |>
    read_vc(root = raw_repo) |>
filter(speciesgroup == 65) |>
    nest(.by = "speciesgroup") |>
    transmute(
      speciesgroup = map2(
        .data$speciesgroup, .data$data, ~mutate(.y, speciesgroup = .x)
      )
    ) |>
    pull("speciesgroup") -> test |>
    map_dfr(
      prepare_analysis_imputation, location = location,
      seed = seed, analysis_path = analysis_path, raw_repo = raw_repo,
      verbose = verbose
    ) -> imputations
  imputations |>
    filter(.data$status != "insufficient_data") |>
    mutate(impute = as.integer(.data$impute)) |>
    inner_join(
      x = read_vc(file = "location/locationgroup", root = raw_repo) |>
        select("id", "impute"),
      by = "impute", relationship = "many-to-many"
    ) -> relevant
  relevant |>
    rename(location_group_id = "id") |>
    arrange(.data$file_fingerprint) |>
    nest(.by = "file_fingerprint", .key = "location_group") |>
    mutate(
      aggregation = map2(
        .data$location_group, .data$file_fingerprint,
        prepare_analysis_aggregate, verbose = verbose,
        analysis_path = analysis_path, seed = seed, raw_repo = raw_repo
      )
    ) -> aggregations

  for (fingerprint in sort(unique(relevant$file_fingerprint))) {
    analysis <- prepare_analysis_model(
      aggregation = aggregation, analysis_path = analysis_path,
      seed = seed, verbose = verbose
    )
    aggregation_wintermax <- prepare_analysis_agg_max(
      aggregation = aggregation, analysis_path = analysis_path, seed = seed,
      verbose = verbose)
    analysis_wintermax <- prepare_analysis_model_max(
      aggregation = aggregation_wintermax, analysis_path = analysis_path,
      seed = seed, verbose = verbose)
    imputations %>%
      select("Fingerprint") %>%
      filter(.data$Fingerprint == impute) %>%
      mutate(Imputation = .data$Fingerprint, Parent = NA_character_) %>%
      bind_rows(
        aggregation_wintermax %>%
          select(Fingerprint = "FileFingerprint", "Parent", "Imputation"),
        aggregation_wintermax %>%
          select("Imputation", Parent = "FileFingerprint") %>%
          inner_join(analysis_wintermax, by = "Parent"),
        aggregation %>%
          select(Fingerprint = "FileFingerprint", "Parent") %>%
          mutate(Imputation = .data$Parent),
        aggregation %>%
          select(Imputation = "Parent", Parent = "FileFingerprint") %>%
          inner_join(analysis, by = "Parent")
      ) %>%
      n2k_manifest() -> manifest
    store_manifest_yaml(
      manifest, base = analysis_path, project = "watervogels",
      docker = "inbobmk/rn2k:0.4",
      dependencies = c("inbo/n2khelper@v0.4.3", "inbo/n2kanalysis@v0.2.7",
                       "inbo/n2kupdate@v0.1.1")
    )
  }
}
