#' Create aggregation objects for imputed counts
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom aws.s3 s3readRDS
#' @importFrom dplyr %>% filter_ select_ bind_rows
#' @importFrom n2kanalysis n2k_aggregate
#' @inheritParams prepare_analysis_imputation
#' @inheritParams prepare_dataset
#' @param imputations a data.frame with the imputations per location group
prepare_analysis_aggregate <- function(
  analysis.path,
  imputations,
  verbose = TRUE
){
  assert_that(inherits(analysis.path, "s3_bucket"))

  assert_that(inherits(imputations, "data.frame"))
  assert_that(has_name(imputations, "SpeciesGroup"))
  assert_that(has_name(imputations, "Filename"))
  assert_that(has_name(imputations, "LocationGroup"))

  location <- read_delim_git(
    file = "locationgrouplocation.txt",
    connection = raw.connection
  )
  assert_that(inherits(location, "data.frame"))
  assert_that(has_name(location, "LocationGroupID"))
  assert_that(has_name(location, "LocationID"))

  lapply(
    unique(imputations$Filename),
    function(filename) {
      if (verbose) {
        message("imputation: ", filename)
      }
      locationgroups <- imputations %>%
        filter_(~Filename == filename) %>%
        "[["("LocationGroup")
      x <- s3readRDS(analysis.path, filename)
      lapply(
        locationgroups,
        function(lg) {
          if (verbose) {
            message("  locationgroup: ", lg)
          }
          analysis <- n2k_aggregate(
            minimum = "Minimum",
            scheme.id = x@AnalysisMetadata$SchemeID,
            species.group.id = x@AnalysisMetadata$SpeciesGroupID,
            location.group.id = lg,
            model.type = "aggregate imputed: sum ~ fYear + fMonth",
            formula = "~fYear + fMonth",
            first.imported.year = x@AnalysisMetadata$FirstImportedYear,
            last.imported.year = x@AnalysisMetadata$LastImportedYear,
            duration = x@AnalysisMetadata$Duration,
            last.analysed.year = x@AnalysisMetadata$LastImportedYear,
            analysis.date = x@AnalysisMetadata$AnalysisDate,
            join = location %>%
              filter_(~LocationGroupID == lg) %>%
              select_(~LocationID) %>%
              as.data.frame(),
            fun = sum,
            parent = get_file_fingerprint(x)
          )
          store_model(
            x = analysis,
            base = analysis.path,
            project = "watervogels"
          )
          analysis@AnalysisMetadata %>%
            select_(
              ~SchemeID, ~SpeciesGroupID, ~LocationGroupID, ~FirstImportedYear,
              ~LastImportedYear, ~Duration, ~LastAnalysedYear, ~AnalysisDate,
              ~Status, ~StatusFingerprint, ~FileFingerprint
            )
        }
      ) %>%
        bind_rows()
    }
  ) %>%
    bind_rows()
}
