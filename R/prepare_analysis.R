#' Prepare all datasets and a to do list of models
#' @inheritParams prepare_analysis_imputation
#' @inheritParams prepare_dataset
#' @export
#' @importFrom git2rdata read_vc
#' @importFrom dplyr %>% mutate select distinct filter group_by do bind_rows inner_join
#' @importFrom tidyr unnest_
#' @importFrom rlang .data
#' @importFrom lubridate ymd year round_date
#' @importFrom n2kanalysis n2k_manifest store_manifest_yaml
prepare_analysis <- function(analysis_path = ".", raw_repo, seed = 19790402,
                             verbose = TRUE) {
  set.seed(seed)
  read_vc(file = "location", root = raw_repo) %>%
    mutate(
      StartYear = round_date(.data$StartDate, unit = "year") %>%
        year(),
      EndYear = round_date(.data$EndDate, unit = "year") %>%
        year()
    ) %>%
    select("ID", "StartYear", "EndYear") -> location
  read_vc(file = "locationgroup", root = raw_repo) %>%
filter(grepl("^4556c3a55", .data$Impute)) %>% # limit to Belgian data
    select(LocationGroupID = "Impute", "SubsetMonths") %>%
    distinct() %>%
    inner_join(
      read_vc(file = "locationgrouplocation", root = raw_repo),
      by = "LocationGroupID"
    ) %>%
    inner_join(location, by = c("LocationID" = "ID")) -> location

  if (verbose) {
    message("Prepare imputations")
  }

  read_vc(file = "speciesgroupspecies", root = raw_repo) %>%
    group_by(.data$speciesgroup) %>%
    do(
      Files = prepare_analysis_imputation(
        speciesgroupspecies = .data, location = location, seed = seed,
        analysis_path = analysis_path, raw_repo = raw_repo, verbose = verbose)
    ) %>%
    unnest(.data$Files) -> imputations
  imputations %>%
    filter(.data$Status != "insufficient_data") %>%
    inner_join(
      read_vc(file = "locationgroup.txt", root = raw_repo) %>%
        select(LocationGroup = "ID", "Impute"),
      by = "Impute"
    ) -> relevant
  for (impute in sort(unique(relevant$Fingerprint))) {
    aggregation <- prepare_analysis_aggregate(
      filter(relevant, .data$Fingerprint == impute), verbose = verbose,
      analysis_path = analysis_path, seed = seed, raw_repo = raw_repo)
    analysis <- prepare_analysis_model(
      aggregation = aggregation, analysis_path = analysis_path,
      seed = seed, verbose = verbose)
    aggregation_wintermax <- prepare_analysis_aggregate_wintermax(
      aggregation = aggregation, analysis_path = analysis_path, seed = seed,
      verbose = verbose)
    analysis_wintermax <- prepare_analysis_model_wintermax(
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
      docker = "inbobmk/rn2k:0.3",
      dependencies = c("inbo/n2khelper@v0.4.3", "inbo/n2kanalysis@v0.2.6",
                       "inbo/n2kupdate@v0.1.1")
    )
  }
}
