#' Create the analysis dataset based on the available raw data
#'
#' The analysis dataset is saved to a rda file with the SHA-1 as name.
#' @param speciesgroupspecies a data.frame with the current species group and the associates species
#' @param location a data.frame with the locations and location groups
#' @param analysis_path Path to store the analysis files. Must be either an existing local file path or an object of the \code{s3_bucket} class.
#' @param seed the seed for the random number generator. Defaults to 19790402 which refers to "Council Directive 79/409/EEC of 2 April 1979 on the conservation of wild birds".
#' @inheritParams prepare_dataset
#' @return A data.frame with the species id number of rows in the analysis dataset, number of precenses in the analysis datset and SHA-1 of the analysis dataset or NULL if not enough data.
#' @importFrom git2rdata read_vc recent_commit
#' @importFrom n2kanalysis n2k_inla get_file_fingerprint store_model
#' @importFrom assertthat assert_that has_name noNA is.flag
#' @importFrom dplyr %>% inner_join mutate select filter ungroup arrange bind_rows do group_by
#' @export
prepare_analysis_imputation <- function(
  speciesgroupspecies, location, analysis_path, raw_repo, seed = 19790402,
  verbose = TRUE
) {
  set.seed(seed)
  assert_that(
    inherits(location, "data.frame"), has_name(location, "LocationID"),
    has_name(location, "LocationGroupID"), has_name(location, "SubsetMonths"),
    has_name(location, "StartYear"), has_name(location, "EndYear"),
    noNA(location$LocationID), noNA(location$LocationGroupID),
    noNA(location$SubsetMonths),
    inherits(speciesgroupspecies, "data.frame"),
    has_name(speciesgroupspecies, "speciesgroup"),
    has_name(speciesgroupspecies, "species"), noNA(speciesgroupspecies),
    anyDuplicated(speciesgroupspecies$speciesgroup) == 0,
    is.flag(verbose), noNA(verbose))
  if (verbose) {
    message(unique(speciesgroupspecies[["speciesgroup"]]), "\n")
  }

  if (nrow(speciesgroupspecies) > 1) {
    stop("Speciesgroups with multiple species not yet handled")
  }

  read_vc(file = "metadata", root = raw_repo) %>%
    inner_join(speciesgroupspecies, by = c("species_id" = "species")) %>%
    inner_join(
      read_vc(file = "import", root = raw_repo),
      by = c("speciesgroup" = "species_group_id")
    ) -> metadata
  assert_that(has_name(metadata, "first_imported_year"),
              has_name(metadata, "last_imported_year"),
              has_name(metadata, "import_analysis"),
              has_name(metadata, "scheme_id"))

  rawdata <- try(read_vc(metadata$speciesgroup, root = raw_repo), silent = TRUE)
  if ("try-error" %in% class(rawdata)) {
    return(tibble())
  }
  assert_that(
    has_name(rawdata, "LocationID"), has_name(rawdata, "Year"),
    has_name(rawdata, "Month"), has_name(rawdata, "ObservationID"),
    has_name(rawdata, "Complete"), has_name(rawdata, "Count"),
    has_name(rawdata, "DataFieldID"),
    noNA(select(rawdata, "DataFieldID", "LocationID", "Year", "Month",
                "ObservationID"))
  )
  rawdata %>%
    count(.data$LocationID, .data$Year, .data$Month) %>%
    filter(.data$n > 1) %>%
    nrow() -> duplicates
  if (duplicates > 0) {
    stop(
"Each combination of LocationID, Year and Month must have exactly one
observation"
    )
  }

  analysis_date <- recent_commit(metadata$speciesgroup, raw_repo, TRUE)$when
  model_type <- "inla nbinomial: Year * (Month + Location)"

  rawdata %>%
    mutate_at("LocationID", as.character) %>%
    inner_join(
      location %>%
        select("LocationID", "LocationGroupID", "SubsetMonths", "StartYear",
               "EndYear"),
      by = "LocationID"
    ) %>%
    filter(
      !.data$SubsetMonths |
        .data$Month %in% c("November", "December", "Januari", "Februari"),
      is.na(.data$StartYear) | .data$StartYear <= .data$Year,
      is.na(.data$EndYear) | .data$Year <= .data$EndYear
    ) %>%
    select("DataFieldID", "LocationGroupID", "ObservationID", "LocationID",
           "Year", "Month", "Complete", "Count") %>%
    mutate(Minimum = pmax(0, .data$Count),
           Count = ifelse(.data$Complete == 1, .data$Minimum, NA),
           Month = factor(.data$Month)) %>%
    group_by(.data$LocationGroupID) -> rawdata

  rawdata %>%
    arrange(.data$LocationID, .data$Year, .data$Month) %>%
    do(
      Dataset = ungroup(.data) %>%
        select(-"LocationGroupID"),
      Relevant = select_relevant_analysis(.data)
    ) %>%
    ungroup() -> selected
  lapply(
    seq_along(selected$LocationGroupID),
    function(i) {
      dataset <- selected$Relevant[[i]]
      if (nrow(dataset) == 0) {
        model <- n2k_inla(
          data = selected$Dataset[[i]],
          result.datasource.id = metadata$results_datasource_id,
          scheme.id = metadata$scheme_id,
          species.group.id = metadata$speciesgroup,
          location.group.id = selected$LocationGroupID[i],
          model.type = model_type,
          formula = "Count ~ 1",
          family = "nbinomial",
          first.imported.year = metadata$first_imported_year,
          last.imported.year = metadata$last_imported_year,
          analysis.date = analysis_date,
          parent = metadata$import_analysis,
          seed = seed,
          status = "insufficient_data"
        )
        filename <- store_model(model, base = analysis_path,
                                project = "watervogels", overwrite = FALSE)
        return(tibble(
          Scheme = metadata$scheme_id, Impute = selected$LocationGroupID[i],
          Fingerprint = get_file_fingerprint(model),
          FirstImportedYear = metadata$first_imported_year,
          LastImportedYear = metadata$last_imported_year,
          AnalysisDate = analysis_date, Filename = filename,
          Status = "insufficient_data"))
      }
      if (length(levels(dataset$fYear)) < 2) {
        stop("Single year datasets not handled")
      }
      n.eff <- dataset %>%
        filter(.data$Count > 0) %>%
        nrow()

      n.used <- length(unique(dataset$Year))
      n.month <- length(levels(dataset$Month))
      n.location <- length(unique(dataset$LocationID))
      covariate <- "Year"
      form <- "f(Year, model = \"rw1\", scale.model = TRUE,
  hyper = list(theta = list(prior = \"pc.prec\", param = c(0.1, 0.01)))
)"
      if (n.month > 1) {
        covariate <- c(covariate, "Month")
        form <- c(form, "Month")
        n.used <- n.used + n.month
      }
      if (n.location > 1) {
        form <- c(form,
          "f(LocationID, model = \"iid\", constr = TRUE,
  hyper = list(theta = list(prior = \"pc.prec\", param = c(1, 0.01)))
)"
        )
        covariate <- c(covariate, "LocationID")
        n.used <- n.used + n.location
      }
      if (n.month > 1) {
        n.extra <- length(levels(dataset$fYearMonth))
        if (n.used + n.extra <= n.eff / 5) {
          covariate <- c(covariate, "fYearMonth")
          form <- c(form, "f(fYearMonth, model = \"iid\", constr = TRUE,
  hyper = list(theta = list(prior = \"pc.prec\", param = c(0.25, 0.01)))
)")
          n.used <- n.used + n.extra
        }
      }
      if (n.location > 1) {
        n.extra <- length(levels(dataset$fYearLocation))
        if (n.used + n.extra <= n.eff / 5) {
          covariate <- c(covariate, "fYearLocation")
          form <- c(form, "f(fYearLocation, model = \"iid\", constr = TRUE,
  hyper = list(theta = list(prior = \"pc.prec\", param = c(0.25, 0.01)))
)")
          n.used <- n.used + n.extra
        }
      }
      relevant <- c("DataFieldID", "ObservationID", covariate, "Count",
                    "Minimum", "Missing")

      model <- dataset %>%
        select(relevant) %>%
        arrange(.data$DataFieldID, .data$ObservationID) %>%
        n2k_inla(
          result.datasource.id = metadata$results_datasource_id,
          scheme.id = metadata$scheme_id,
          species.group.id = metadata$speciesgroup,
          location.group.id = selected$LocationGroupID[i],
          model.type = model_type,
          formula = paste(form, collapse = "+") %>%
            sprintf(fmt = "Count~%s"),
          family = "nbinomial",
          first.imported.year = metadata$first_imported_year,
          last.imported.year = metadata$last_imported_year,
          imputation.size = 100,
          minimum = "Minimum",
          parent = metadata$import_analysis,
          seed = seed,
          analysis.date = analysis_date
        )
      filename <- store_model(model, base = analysis_path,
                              project = "watervogels", overwrite = FALSE)
      return(tibble(
        ResultDatasourceID = metadata$results_datasource_id,
        Scheme = metadata$scheme_id, Impute = selected$LocationGroupID[i],
        Fingerprint = get_file_fingerprint(model),
        FirstImportedYear = metadata$first_imported_year,
        LastImportedYear = metadata$last_imported_year,
        AnalysisDate = analysis_date, Filename = filename,
        Formula = paste(form, collapse = "+"), Status = "new"))
    }
  ) %>%
    bind_rows()
}
