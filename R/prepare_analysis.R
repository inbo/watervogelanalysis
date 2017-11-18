#' Prepare all datasets and a to do list of models
#' @export
#' @importFrom n2khelper read_delim_git list_files_git
#' @importFrom lubridate ymd year round_date
#' @inheritParams prepare_analysis_imputation
#' @inheritParams prepare_dataset
#' @importFrom dplyr %>% mutate_ select_ distinct_
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
    # semi_join(species_id, by = c("SpeciesGroup" = "fingerprint")) %>%
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
  aggregation <- imputations %>%
    filter_(~Status != "insufficient_data") %>%
    inner_join(
      read_delim_git(
        file = "locationgroup.txt",
        connection = raw.connection
      ) %>%
        select_(LocationGroup = ~ID, ~Impute),
      by = "Impute"
    ) %>%
    prepare_analysis_aggregate(
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
  manifest <- imputations %>%
    select_(~Fingerprint) %>%
    mutate_(
      Parent = ~NA_character_,
      Imputation = ~Fingerprint
    ) %>%
    bind_rows(
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
