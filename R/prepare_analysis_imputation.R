#' Create the analysis dataset based on the available raw data
#'
#' The analysis dataset is saved to a rda file with the SHA-1 as name.
#' @param speciesgroupspecies a data.frame with the current species group and the associates species
#' @param location a data.frame with the locations and location groups
#' @param analysis.path Path to store the analysis files. Must be either an existing local file path or an object of the \code{s3_bucket} class.
#' @inheritParams prepare_dataset
#' @return A data.frame with the species id number of rows in the analysis dataset, number of precenses in the analysis datset and SHA-1 of the analysis dataset or NULL if not enough data.
#' @importFrom n2khelper read_delim_git git_recent
#' @importFrom n2kanalysis select_factor_treshold n2k_inla_nbinomial get_file_fingerprint store_model
#' @importFrom assertthat assert_that has_name noNA is.flag
#' @importFrom dplyr %>% inner_join left_join mutate_ select_ filter_ ungroup
#' @importFrom stats na.omit
#' @export
prepare_analysis_imputation <- function(
  speciesgroupspecies,
  location,
  analysis.path,
  raw.connection,
  verbose = TRUE
){
  assert_that(inherits(location, "data.frame"))
  assert_that(has_name(location, "LocationID"))
  assert_that(has_name(location, "LocationGroupID"))
  assert_that(has_name(location, "SubsetMonths"))
  assert_that(has_name(location, "StartYear"))
  assert_that(has_name(location, "EndYear"))
  assert_that(noNA(location$LocationID))
  assert_that(noNA(location$LocationGroupID))
  assert_that(noNA(location$SubsetMonths))

  assert_that(inherits(speciesgroupspecies, "data.frame"))
  assert_that(has_name(speciesgroupspecies, "SpeciesGroup"))
  assert_that(has_name(speciesgroupspecies, "Species"))
  assert_that(noNA(speciesgroupspecies))
  assert_that(anyDuplicated(speciesgroupspecies$SpeciesGroup) == 0)

  assert_that(is.flag(verbose))
  assert_that(noNA(verbose))
  if (verbose) {
    message(unique(speciesgroupspecies[["SpeciesGroup"]]), "\n")
  }

  if (nrow(speciesgroupspecies) > 1) {
    stop("Speciesgroups with multiple species not yet handled")
  }

  metadata <- read_delim_git(
    file = "metadata.txt",
    connection = raw.connection
  ) %>%
    inner_join(speciesgroupspecies, by = c("SpeciesID" = "Species")) %>%
    inner_join(
      read_delim_git(file = "import.txt", connection = raw.connection),
      by = c("SpeciesGroup" = "SpeciesGroupID")
    )
  assert_that(has_name(metadata, "FirstImportedYear"))
  assert_that(has_name(metadata, "LastImportedYear"))
  assert_that(has_name(metadata, "ImportAnalysis"))

  rawdata.file <- paste0(metadata$SpeciesGroup, ".txt")
  rawdata <- read_delim_git(rawdata.file, connection = raw.connection)
  assert_that(has_name(rawdata, "LocationID"))
  assert_that(has_name(rawdata, "Year"))
  assert_that(has_name(rawdata, "fMonth"))
  assert_that(has_name(rawdata, "ObservationID"))
  assert_that(has_name(rawdata, "Complete"))
  assert_that(has_name(rawdata, "Count"))
  assert_that(
    noNA(select_(rawdata, ~LocationID, ~Year, ~fMonth, ~ObservationID))
  )
  available_data <- table(rawdata$LocationID, rawdata$Year, rawdata$fMonth) %>%
    range()
  if (!all.equal(available_data, c(1, 1))) {
    stop(
"Each combination of LocationID, Year and fMonth must have exactly one
observation"
    )
  }

  analysis.date <- git_recent(
    file = rawdata.file,
    connection = raw.connection
  )$Date
  model.type <- "inla nbinomial: Year * (Month + Location)"

  rawdata <- rawdata %>%
    inner_join(
      location %>%
        select_(
          ~LocationID, ~LocationGroupID, ~SubsetMonths, ~StartYear, ~EndYear
        ),
      by = "LocationID"
    ) %>%
    filter_(~!SubsetMonths | fMonth %in% c("11", "12", "1", "2")) %>%
    filter_(
      ~is.na(StartYear) | StartYear <= Year,
      ~is.na(EndYear) | Year <= EndYear
    ) %>%
    select_(
      ~LocationGroupID,
      ~ObservationID,
      ~LocationID,
      ~Year,
      ~fMonth,
      ~Complete,
      ~Count
    ) %>%
    mutate_(
      Minimum = ~pmax(0, Count),
      Count = ~ifelse(Complete == 1, Minimum, NA)
    ) %>%
    group_by_(~LocationGroupID)

  selected <- rawdata %>%
    arrange_(~LocationID, ~Year, ~fMonth) %>%
    do_(
      Dataset = ~ungroup(.) %>%
        select_(~-LocationGroupID),
      Relevant = ~select_relevant_analysis(.) %>%
        ungroup() %>%
        mutate_(
          fMonth = ~factor(fMonth),
          fYear = ~factor(Year),
          cYear = ~Year - max(Year),
          fYearMonth = ~interaction(fYear, fMonth, drop = TRUE),
          LocationID = ~factor(LocationID),
          fYearLocation = ~interaction(fYear, LocationID, drop = TRUE)
        ) %>%
        select_(~-LocationGroupID)
    )
  lapply(
    seq_along(selected$LocationGroupID),
    function(i) {
      dataset <- selected$Relevant[[i]]
      if (nrow(dataset) == 0) {
        model <- n2k_inla_nbinomial(
          data = selected$Dataset[[i]],
          result.datasource.id = metadata$ResultDatasourceID,
          scheme.id = metadata$SchemeID,
          species.group.id = metadata$SpeciesGroup,
          location.group.id = selected$LocationGroupID[i],
          model.type = model.type,
          formula = "Count ~ 1",
          first.imported.year = metadata$FirstImportedYear,
          last.imported.year = metadata$LastImportedYear,
          analysis.date = analysis.date,
          parent = metadata$ImportAnalysis,
          status = "insufficient_data"
        )
        filename <- store_model(
          model,
          base = analysis.path,
          project = "watervogels"
        )
        return(
          data.frame(
            Scheme = metadata$SchemeID,
            Impute = selected$LocationGroupID[i],
            Fingerprint = get_file_fingerprint(model),
            FirstImportedYear = metadata$FirstImportedYear,
            LastImportedYear = metadata$LastImportedYear,
            AnalysisDate = analysis.date,
            Filename = filename,
            Status = "insufficient_data",
            stringsAsFactors = FALSE
          )
        )
      }
      if (length(levels(dataset$fYear)) < 2) {
        stop("Single year datasets not handled")
      }
      covariate <- "Year"
      form <- "f(
  Year,
  model = \"rw1\",
  scale.model = TRUE,
  hyper = list(theta = list(prior = \"pc.prec\", param = c(1, 0.01)))
)"
      if (length(levels(dataset$fMonth)) > 1) {
        covariate <- c(covariate, "fMonth", "fYearMonth")
        form <- c(form, "fMonth + f(fYearMonth, model = 'iid')")
      }
      if (length(levels(dataset$LocationID)) > 1) {
        form <- c(
          form,
          "f(LocationID, model = 'iid') + f(fYearLocation, model = 'iid')"
        )
        covariate <- c(covariate, "LocationID", "fYearLocation")
      }
      relevant <- c(
        covariate, "ObservationID", "Count", "Minimum"
      )

      model <- dataset %>%
        select_(.dots = relevant) %>%
        arrange_(.dots = relevant) %>%
        n2k_inla_nbinomial(
          result.datasource.id = metadata$ResultDatasourceID,
          scheme.id = metadata$SchemeID,
          species.group.id = metadata$SpeciesGroup,
          location.group.id = selected$LocationGroupID[i],
          model.type = model.type,
          formula = paste(form, collapse = "+") %>%
            sprintf(fmt = "Count~%s"),
          first.imported.year = metadata$FirstImportedYear,
          last.imported.year = metadata$LastImportedYear,
          imputation.size = 100,
          minimum = "Minimum",
          parent = metadata$ImportAnalysis,
          analysis.date = analysis.date
        )
      filename <- store_model(
        model,
        base = analysis.path,
        project = "watervogels"
      )
      return(
        data.frame(
          ResultDatasourceID = metadata$ResultDatasourceID,
          Scheme = metadata$SchemeID,
          Impute = selected$LocationGroupID[i],
          Fingerprint = get_file_fingerprint(model),
          FirstImportedYear = metadata$FirstImportedYear,
          LastImportedYear = metadata$LastImportedYear,
          AnalysisDate = analysis.date,
          Filename = filename,
          Status = "new",
          stringsAsFactors = FALSE
        )
      )
    }
  ) %>%
    bind_rows()
}
