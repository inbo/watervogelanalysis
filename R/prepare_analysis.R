#' Prepare all datasets and a to do list of models
#' @export
#' @importFrom n2khelper read_delim_git list_files_git
#' @importFrom lubridate ymd year round_date
#' @inheritParams prepare_analysis_imputation
#' @inheritParams prepare_dataset
#' @importFrom dplyr %>% mutate_ select_ distinct_ select
#' @importFrom rlang .data
#' @importFrom lubridate ymd year round_date
#' @importFrom n2kanalysis n2k_manifest store_manifest_yaml
prepare_analysis <- function(
  analysis.path = ".",
  raw.connection,
  seed = 19790402,
  verbose = TRUE
){
  set.seed(seed)
  location <- read_delim_git(
    file = "location.txt",
    connection = raw.connection
  ) %>%
    mutate_(
      StartDate =  ~ymd(StartDate),
      EndDate = ~ymd(EndDate),
      StartYear = ~round_date(StartDate, unit = "year") %>%
        year(),
      EndYear = ~round_date(EndDate, unit = "year") %>%
        year()
    ) %>%
    select_(~ID, ~StartYear, ~EndYear)
  location <- read_delim_git(
    file = "locationgroup.txt",
    connection = raw.connection
  ) %>%
    select_(LocationGroupID = ~Impute, ~SubsetMonths) %>%
    distinct_() %>%
    inner_join(
      read_delim_git(
        file = "locationgrouplocation.txt",
        connection = raw.connection
      ),
      by = "LocationGroupID"
    ) %>%
    inner_join(location, by = c("LocationID" = "ID"))

  imputations <- read_delim_git(
    file = "speciesgroupspecies.txt",
    connection = raw.connection
  ) %>%
    semi_join(species_id, by = c("SpeciesGroup" = "fingerprint")) %>%
    group_by_(~SpeciesGroup) %>%
    do_(
      Files = ~prepare_analysis_imputation(
        speciesgroupspecies = .,
        location = location,
        analysis.path = analysis.path,
        raw.connection = raw.connection,
        seed = seed,
        verbose = verbose
      )
    ) %>%
    unnest_("Files")
  relevant <- imputations %>%
    filter_(~Status != "insufficient_data") %>%
    inner_join(
      read_delim_git(
        file = "locationgroup.txt",
        connection = raw.connection
      ) %>%
        select_(LocationGroup = ~ID, ~Impute),
      by = "Impute"
    )
  aggregation <- prepare_analysis_aggregate(
      relevant,
      analysis.path = analysis.path,
      verbose = verbose,
      seed = seed,
      raw.connection = raw.connection
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
      relevant,
      analysis.path = analysis.path,
      verbose = verbose,
      seed = seed,
      raw.connection = raw.connection
    )
  analysis_ni <- prepare_analysis_model_ni(
    aggregation = aggregation_ni,
    analysis.path = analysis.path,
    seed = seed,
    verbose = verbose
  )
  manifest <- imputations %>%
    select_(~Fingerprint) %>%
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
    group_by_(~Imputation) %>%
    do_(Manifest = ~n2k_manifest(.))
  lapply(
    manifest$Manifest,
    store_manifest_yaml,
    base = analysis.path,
    project = "watervogels",
    docker = "inbobmk/rn2k:0.1",
    dependencies = c("inbo/n2khelper@v0.4.1", "inbo/n2kanalysis@v0.2.3")
  )
}
