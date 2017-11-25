#' Create aggregation objects for imputed counts
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom aws.s3 s3readRDS
#' @importFrom dplyr %>% filter_ select_ bind_rows mutate_
#' @importFrom n2kanalysis n2k_aggregate
#' @inheritParams prepare_analysis_imputation
#' @inheritParams prepare_dataset
#' @param imputations a data.frame with the imputations per location group
prepare_analysis_aggregate <- function(
  analysis.path,
  imputations,
  raw.connection,
  seed = 19790402,
  verbose = TRUE
){
  set.seed(seed)
  assert_that(inherits(imputations, "data.frame"))
  assert_that(has_name(imputations, "SpeciesGroup"))
  assert_that(has_name(imputations, "Filename"))
  assert_that(has_name(imputations, "LocationGroup"))
  assert_that(has_name(imputations, "Scheme"))
  assert_that(has_name(imputations, "Fingerprint"))
  imputations <- imputations %>%
    arrange_(~Fingerprint, ~LocationGroup)

  location <- read_delim_git(
    file = "locationgrouplocation.txt",
    connection = raw.connection
  )
  assert_that(inherits(location, "data.frame"))
  assert_that(has_name(location, "LocationGroupID"))
  assert_that(has_name(location, "LocationID"))

  lapply(
    unique(imputations$Fingerprint),
    function(fingerprint) {
      if (verbose) {
        message("imputation: ", fingerprint)
      }
      locationgroups <- imputations %>%
        filter_(~Fingerprint == fingerprint) %>%
        "[["("LocationGroup")
      lapply(
        locationgroups,
        function(lg) {
          if (verbose) {
            message("  locationgroup: ", lg)
          }
          metadata <- imputations %>%
            filter_(~Fingerprint == fingerprint, ~LocationGroup == lg)
          analysis <- n2k_aggregate(
            status = "waiting",
            minimum = "Minimum",
            result.datasource.id = metadata$ResultDatasourceID,
            scheme.id = metadata$Scheme,
            species.group.id = metadata$SpeciesGroup,
            location.group.id = lg,
            seed = seed,
            model.type = "aggregate imputed: sum ~ Year + fMonth",
            formula = "~Year + fMonth",
            first.imported.year = metadata$FirstImportedYear,
            last.imported.year = metadata$LastImportedYear,
            analysis.date = metadata$AnalysisDate,
            join = location %>%
              filter_(~LocationGroupID == lg) %>%
              select_(~LocationID) %>%
              as.data.frame(stringsAsFactors = FALSE),
            fun = sum,
            parent = fingerprint
          )
          store_model(
            x = analysis,
            base = analysis.path,
            project = "watervogels",
            overwrite = FALSE
          )
          analysis@AnalysisMetadata %>%
            select_(
              ~SchemeID, ~SpeciesGroupID, ~LocationGroupID, ~FirstImportedYear,
              ~LastImportedYear, ~Duration, ~LastAnalysedYear, ~AnalysisDate,
              ~Status, ~StatusFingerprint, ~FileFingerprint, ~ResultDatasourceID
            )
        }
      ) %>%
        bind_rows() %>%
        mutate_(Parent = ~fingerprint)
    }
  ) %>%
    bind_rows()
}
