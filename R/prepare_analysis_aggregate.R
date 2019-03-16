#' Create aggregation objects for imputed counts
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom dplyr %>% filter select bind_rows mutate arrange
#' @importFrom n2kanalysis n2k_aggregate store_model
#' @importFrom git2rdata read_vc
#' @inheritParams prepare_analysis_imputation
#' @inheritParams prepare_dataset
#' @param imputations a data.frame with the imputations per location group
prepare_analysis_aggregate <- function(analysis_path, imputations, raw_repo,
                                       seed = 19790402, verbose = TRUE) {
  set.seed(seed)
  assert_that(
    inherits(imputations, "data.frame"), has_name(imputations, "speciesgroup"),
    has_name(imputations, "Filename"), has_name(imputations, "LocationGroup"),
    has_name(imputations, "Scheme"), has_name(imputations, "Fingerprint"),
    length(unique(imputations$Fingerprint)) == 1)
  imputations <- imputations %>%
    arrange(.data$Fingerprint, .data$LocationGroup)

  location <- read_vc(file = "locationgrouplocation.txt", root = raw_repo)
  assert_that(
    inherits(location, "data.frame"), has_name(location, "LocationGroupID"),
    has_name(location, "LocationID"))

  if (verbose) {
    message("imputation: ", imputations$Fingerprint[1])
  }
  lapply(
    imputations$LocationGroup,
    function(lg) {
      if (verbose) {
        message("  locationgroup: ", lg)
      }
      metadata <- imputations %>%
        filter(.data$LocationGroup == lg)
      form <- ifelse(grepl("Month", metadata$Formula), "~Year + Month", "~Year")
      analysis <- n2k_aggregate(
        status = "waiting",
        minimum = "Minimum",
        result.datasource.id = metadata$ResultDatasourceID,
        scheme.id = metadata$Scheme,
        species.group.id = metadata$speciesgroup,
        location.group.id = lg,
        seed = seed,
        model.type = "aggregate imputed: sum ~ Year + Month",
        formula = form,
        first.imported.year = metadata$FirstImportedYear,
        last.imported.year = metadata$LastImportedYear,
        analysis.date = metadata$AnalysisDate,
        join = location %>%
          filter(.data$LocationGroupID == lg) %>%
          select("LocationID") %>%
          as.data.frame(stringsAsFactors = FALSE),
        fun = sum,
        parent = imputations$Fingerprint[1]
      )
      store_model(x = analysis, base = analysis_path, project = "watervogels",
                  overwrite = FALSE)
      analysis@AnalysisMetadata %>%
        select(
          "SchemeID", "SpeciesGroupID", "LocationGroupID", "FirstImportedYear",
          "LastImportedYear", "Duration", "LastAnalysedYear", "AnalysisDate",
          "Status", "StatusFingerprint", "FileFingerprint", "ResultDatasourceID"
        )
    }
  ) %>%
    bind_rows() %>%
    mutate(Parent = imputations$Fingerprint[1])
}
