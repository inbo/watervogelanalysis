#' Prepare all datasets and a to do list of models
#' @inheritParams prepare_analysis_imputation
#' @inheritParams prepare_dataset
#' @export
#' @importFrom git2rdata read_vc
#' @importFrom dplyr %>% mutate_ select_ distinct_ filter group_by_ do_ bind_rows inner_join
#' @importFrom tidyr unnest_
#' @importFrom rlang .data
#' @importFrom lubridate ymd year round_date
#' @importFrom n2kanalysis n2k_manifest store_manifest_yaml
prepare_analysis <- function(
  analysis.path = ".",
  raw_repo,
  seed = 19790402,
  verbose = TRUE
){
  set.seed(seed)
  location <- read_vc(file = "location.txt", root = raw_repo) %>%
    mutate_(
      StartDate =  ~ymd(StartDate),
      EndDate = ~ymd(EndDate),
      StartYear = ~round_date(StartDate, unit = "year") %>%
        year(),
      EndYear = ~round_date(EndDate, unit = "year") %>%
        year()
    ) %>%
    select_(~ID, ~StartYear, ~EndYear)
  location <- read_vc(file = "locationgroup.txt", root = raw_repo) %>%
    select_(LocationGroupID = ~Impute, ~SubsetMonths) %>%
    distinct_() %>%
    inner_join(
      read_vc(
        file = "locationgrouplocation.txt",
        root = raw_repo
      ),
      by = "LocationGroupID"
    ) %>%
    inner_join(location, by = c("LocationID" = "ID"))

  imputations <- read_vc(
    file = "speciesgroupspecies.txt",
    root = raw_repo
  ) %>%
    group_by_(~SpeciesGroup) %>%
    do_(
      Files = ~prepare_analysis_imputation(
        speciesgroupspecies = .,
        location = location,
        analysis.path = analysis.path,
        raw_repo = raw_repo,
        seed = seed,
        verbose = verbose
      )
    ) %>%
    unnest_("Files")
  relevant <- imputations %>%
    filter(.data$Status != "insufficient_data") %>%
    inner_join(
      read_vc(file = "locationgroup.txt", root = raw_repo) %>%
        select_(LocationGroup = ~ID, ~Impute),
      by = "Impute"
    )
  for (impute in sort(unique(relevant$Fingerprint))) {
    aggregation <- prepare_analysis_aggregate(
        relevant %>%
          filter(.data$Fingerprint == impute),
        analysis.path = analysis.path,
        verbose = verbose,
        seed = seed,
        raw_repo = raw_repo
      )
    analysis <- prepare_analysis_model(
      aggregation = aggregation,
      analysis.path = analysis.path,
      seed = seed,
      verbose = verbose
    )
    aggregation_wintermax <- prepare_analysis_aggregate_wintermax(
      aggregation = aggregation,
      analysis.path = analysis.path,
      seed = seed,
      verbose = verbose
    )
    analysis_wintermax <- prepare_analysis_model_wintermax(
      aggregation = aggregation_wintermax,
      analysis.path = analysis.path,
      seed = seed,
      verbose = verbose
    )
    aggregation_ni <- prepare_analysis_aggregate_ni(
        relevant %>%
          filter(.data$Fingerprint == impute),
        analysis.path = analysis.path,
        verbose = verbose,
        seed = seed,
        raw_repo = raw_repo
      )
    analysis_ni <- prepare_analysis_model_ni(
      aggregation = aggregation_ni,
      analysis.path = analysis.path,
      seed = seed,
      verbose = verbose
    )
    manifest <- imputations %>%
      select_(~Fingerprint) %>%
      filter(.data$Fingerprint == impute) %>%
      mutate_(
        Parent = ~NA_character_,
        Imputation = ~Fingerprint
      ) %>%
      bind_rows(
        aggregation_wintermax %>%
          select(
            Fingerprint = .data$FileFingerprint,
            .data$Parent,
            .data$Imputation
          ),
        aggregation_wintermax %>%
          select_(
            ~Imputation,
            Parent = ~FileFingerprint
          ) %>%
          inner_join(analysis_wintermax, by = "Parent"),
        aggregation_ni %>%
          select_(Fingerprint = ~FileFingerprint, ~Parent) %>%
          mutate_(Imputation = ~Parent),
        aggregation_ni %>%
          select_(
            Imputation = ~Parent,
            Parent = ~FileFingerprint
          ) %>%
          inner_join(analysis_ni, by = "Parent"),
        aggregation %>%
          select_(Fingerprint = ~FileFingerprint, ~Parent) %>%
          mutate_(Imputation = ~Parent),
        aggregation %>%
          select_(
            Imputation = ~Parent,
            Parent = ~FileFingerprint
          ) %>%
          inner_join(analysis, by = "Parent")
      ) %>%
      n2k_manifest()
    store_manifest_yaml(manifest,
      base = analysis.path,
      project = "watervogels",
      docker = "inbobmk/rn2k:0.2",
      dependencies = c("inbo/n2khelper@v0.4.1.1", "inbo/n2kanalysis@v0.2.4.3")
    )
  }
}
