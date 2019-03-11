#' Create the analysis dataset based on the available raw data
#'
#' The analysis dataset is saved to a rda file with the SHA-1 as name.
#' @param speciesgroupspecies a data.frame with the current species group and the associates species
#' @param location a data.frame with the locations and location groups
#' @param analysis.path Path to store the analysis files. Must be either an existing local file path or an object of the \code{s3_bucket} class.
#' @param seed the seed for the random number generator. Defaults to 19790402 which refers to "Council Directive 79/409/EEC of 2 April 1979 on the conservation of wild birds".
#' @inheritParams prepare_dataset
#' @return A data.frame with the species id number of rows in the analysis dataset, number of precenses in the analysis datset and SHA-1 of the analysis dataset or NULL if not enough data.
#' @importFrom git2rdata read_vc recent_commit
#' @importFrom n2kanalysis n2k_inla get_file_fingerprint store_model
#' @importFrom assertthat assert_that has_name noNA is.flag
#' @importFrom dplyr %>% inner_join mutate_ select_ filter_ ungroup arrange_ bind_rows do_ group_by_
#' @export
prepare_analysis_imputation <- function(
  speciesgroupspecies,
  location,
  analysis.path,
  raw_repo,
  seed = 19790402,
  verbose = TRUE
){
  set.seed(seed)
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

  metadata <- read_vc(
    file = "metadata.txt",
    root = raw_repo
  ) %>%
    inner_join(speciesgroupspecies, by = c("SpeciesID" = "Species")) %>%
    inner_join(
      read_vc(file = "import.txt", root = raw_repo),
      by = c("SpeciesGroup" = "SpeciesGroupID")
    )
  assert_that(has_name(metadata, "FirstImportedYear"))
  assert_that(has_name(metadata, "LastImportedYear"))
  assert_that(has_name(metadata, "ImportAnalysis"))

  rawdata.file <- paste0(metadata$SpeciesGroup, ".txt")
  rawdata <- read_vc(rawdata.file, root = raw_repo)
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

  analysis.date <- recent_commit(
    file = rawdata.file,
    root = raw_repo
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
          Missing = ~is.na(Count),
          fMonth = ~factor(fMonth),
          fYear = ~factor(Year),
          cYear = ~Year - max(Year),
          fYearMonth = ~interaction(fYear, fMonth, drop = TRUE),
          fYearLocation = ~interaction(fYear, factor(LocationID), drop = TRUE)
        ) %>%
        select_(~-LocationGroupID)
    )
  lapply(
    seq_along(selected$LocationGroupID),
    function(i) {
      dataset <- selected$Relevant[[i]]
      if (nrow(dataset) == 0) {
        model <- n2k_inla(
          data = selected$Dataset[[i]],
          result.datasource.id = metadata$ResultDatasourceID,
          scheme.id = metadata$SchemeID,
          species.group.id = metadata$SpeciesGroup,
          location.group.id = selected$LocationGroupID[i],
          model.type = model.type,
          formula = "Count ~ 1",
          family = "nbinomial",
          first.imported.year = metadata$FirstImportedYear,
          last.imported.year = metadata$LastImportedYear,
          analysis.date = analysis.date,
          parent = metadata$ImportAnalysis,
          seed = seed,
          status = "insufficient_data"
        )
        filename <- store_model(
          model,
          base = analysis.path,
          project = "watervogels",
          overwrite = FALSE
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
      n.eff <- dataset %>%
        filter_(~Count > 0) %>%
        nrow()
      n.used <- length(table(dataset$Year))
      n.month <- length(table(dataset$fMonth))
      n.location <- length(table(dataset$LocationID))
      covariate <- "Year"
      form <- "f(
  Year,
  model = \"rw1\",
  scale.model = TRUE,
  hyper = list(theta = list(prior = \"pc.prec\", param = c(1, 0.01)))
)"
      if (n.month > 1) {
        covariate <- c(covariate, "fMonth")
        form <- c(form, "fMonth")
        n.used <- n.used + n.month
      }
      if (n.location > 1) {
        form <- c(
          form,
          "f(LocationID, model = 'iid')"
        )
        covariate <- c(covariate, "LocationID")
        n.used <- n.used + n.location
      }
      if (n.month > 1) {
        n.extra <- length(table(dataset$fYearMonth))
        if (n.used + n.extra <= n.eff / 5) {
          covariate <- c(covariate, "fYearMonth")
          form <- c(form, "f(fYearMonth, model = 'iid')")
          n.used <- n.used + n.extra
        }
      }
      if (n.location > 1) {
        n.extra <- length(table(dataset$fYearLocation))
        if (n.used + n.extra <= n.eff / 5) {
          covariate <- c(covariate, "fYearLocation")
          form <- c(form, "f(fYearLocation, model = 'iid')")
          n.used <- n.used + n.extra
        }
      }
      relevant <- c(
        covariate, "ObservationID", "Count", "Minimum", "Missing"
      )

      model <- dataset %>%
        select_(.dots = relevant) %>%
        arrange_(.dots = relevant) %>%
        n2k_inla(
          result.datasource.id = metadata$ResultDatasourceID,
          scheme.id = metadata$SchemeID,
          species.group.id = metadata$SpeciesGroup,
          location.group.id = selected$LocationGroupID[i],
          model.type = model.type,
          formula = paste(form, collapse = "+") %>%
            sprintf(fmt = "Count~%s"),
          family = "nbinomial",
          first.imported.year = metadata$FirstImportedYear,
          last.imported.year = metadata$LastImportedYear,
          imputation.size = 100,
          minimum = "Minimum",
          parent = metadata$ImportAnalysis,
          seed = seed,
          analysis.date = analysis.date
        )
      filename <- store_model(
        model,
        base = analysis.path,
        project = "watervogels",
        overwrite = FALSE
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
          Formula = paste(form, collapse = "+"),
          Status = "new",
          stringsAsFactors = FALSE
        )
      )
    }
  ) %>%
    bind_rows()
}
