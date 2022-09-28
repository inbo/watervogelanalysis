#' Create aggregation objects for imputed counts while ignoring the missing data
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom dplyr %>% filter select bind_rows mutate arrange
#' @importFrom n2kanalysis n2k_aggregate store_model
#' @importFrom git2rdata read_vc
#' @inheritParams prepare_analysis_imputation
#' @inheritParams prepare_dataset
#' @param imputations a data.frame with the imputations per location group
prepare_analysis_aggregate_ni <- function(
  analysis_path, imputations, raw_repo, seed = 19790402, verbose = TRUE
) {
  set.seed(seed)
  assert_that(inherits(imputations, "data.frame"))
  assert_that(has_name(imputations, "SpeciesGroup"))
  assert_that(has_name(imputations, "Filename"))
  assert_that(has_name(imputations, "LocationGroup"))
  assert_that(has_name(imputations, "Scheme"))
  assert_that(has_name(imputations, "Fingerprint"))
  imputations <- imputations %>%
    arrange(.data$Fingerprint, .data$LocationGroup)

  location <- read_vc(file = "locationgrouplocation.txt", root = raw_repo)
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
        filter(.data$Fingerprint == fingerprint) %>%
        "[["("LocationGroup")
      lapply(
        locationgroups,
        function(lg) {
          if (verbose) {
            message("  locationgroup: ", lg)
          }
          metadata <- imputations %>%
            filter(.data$Fingerprint == fingerprint, .data$LocationGroup == lg)
          form <- ifelse(
            grepl("fMonth", metadata$Formula),
            "~Year + fMonth",
            "~Year"
          )
          analysis <- n2k_aggregate(
            status = "waiting",
            minimum = "Minimum",
            result.datasource.id = metadata$ResultDatasourceID,
            scheme.id = metadata$Scheme,
            species.group.id = metadata$SpeciesGroup,
            location.group.id = lg,
            seed = seed,
            model.type = "aggregate imputed: sum ~ Year + fMonth",
            formula = form,
            first.imported.year = metadata$FirstImportedYear,
            last.imported.year = metadata$LastImportedYear,
            analysis.date = metadata$AnalysisDate,
            join = location %>%
              filter(.data$LocationGroupID == lg) %>%
              select("LocationID") %>%
              as.data.frame(stringsAsFactors = FALSE),
            filter = list("!Missing"),
            fun = sum,
            parent = fingerprint
          )
          store_model(
            x = analysis,
            base = analysis_path,
            project = "watervogels",
            overwrite = FALSE
          )
          analysis@AnalysisMetadata %>%
            select(
              "SchemeID", "SpeciesGroupID", "LocationGroupID",
              "FirstImportedYear", "LastImportedYear", "Duration",
              "LastAnalysedYear", "AnalysisDate", "Status", "StatusFingerprint",
              "FileFingerprint", "ResultDatasourceID"
            )
        }
      ) %>%
        bind_rows() %>%
        mutate(Parent = .data$fingerprint)
    }
  ) %>%
    bind_rows()
}
