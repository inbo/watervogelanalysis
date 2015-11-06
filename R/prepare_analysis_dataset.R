#' Create the analysis dataset based on the available raw data
#'
#' The analysis dataset is saved to a rda file with the SHA-1 as name.
#' @param rawdata.file Name of the rawdata file
#' @param location a data.frame with the locations
#' @param analysis.path Path to store the analysis files
#' @inheritParams prepare_dataset
#' @return A data.frame with the species id number of rows in the analysis dataset, number of precenses in the analysis datset and SHA-1 of the analysis dataset or NULL if not enough data.
#' @importFrom n2khelper check_single_character check_dataframe_variable read_delim_git git_recent check_single_strictly_positive_integer
#' @importFrom n2kanalysis select_factor_threshold n2k_inla_nbinomial get_file_fingerprint
#' @importFrom plyr d_ply
#' @importFrom digest digest
#' @importFrom assertthat assert_that is.count
#' @export
prepare_analysis_dataset <- function(
  rawdata.file,
  location,
  analysis.path,
  raw.connection
){
  analysis.path <- check_path(paste0(analysis.path, "/"), type = "directory")
  check_dataframe_variable(
    df = location,
    variable = c(
      "LocationID", "LocationGroupID", "SubsetMonths", "StartYear", "EndYear"
    ),
    name = "location"
  )

  speciesgroup.id <- as.integer(gsub("\\.txt", "", rawdata.file))
  metadata <- read_delim_git(file = "metadata.txt", connection = raw.connection)
  metadata <- metadata[metadata$SpeciesGroupID == speciesgroup.id, ]

  assert_that(is.count(metadata$SchemeID))
  scheme.id <- as.integer(metadata$SchemeID)
  assert_that(is.count(first.year))
  first.year <- as.integer(first.year)
  assert_that(is.count(last.year))
  last.year <- as.integer(last.year)

  rawdata <- read_delim_git(file = rawdata.file, connection = raw.connection)
  if (class(rawdata) != "data.frame") {
    stop(rawdata.file, " not available")
  }
  analysis.date <- git_recent(
    file = rawdata.file,
    connection = raw.connection
  )$Date

  rawdata$Complete <- rawdata$Complete == 1

  full.combination <- expand.grid(
    Year = unique(rawdata$Year),
    fMonth = unique(rawdata$fMonth),
    LocationID = unique(rawdata$LocationID)
  )
  full.combination <- merge(
    full.combination,
    location[
      ,
      c("LocationID", "LocationGroupID", "SubsetMonths", "StartYear", "EndYear")
    ]
  )
  rm(location)

  remove.month <- full.combination$SubsetMonths &
    full.combination$fMonth %in% c("3", "10")
  full.combination <- full.combination[!remove.month, ]

  relevant.start <- is.na(full.combination$StartYear) |
    full.combination$StartYear <= full.combination$Year
  relevant.end <- is.na(full.combination$EndYear) |
    full.combination$EndYear >= full.combination$Year
  full.combination <- full.combination[relevant.start & relevant.end, ]
  rm(relevant.start, relevant.end)

  rawdata <- merge(
    full.combination[, c("LocationGroupID", "LocationID", "Year", "fMonth")],
    rawdata,
    all.x = TRUE
  )
  rawdata$Minimum <- ifelse(is.na(rawdata$Count), 0, rawdata$Count)
  rawdata$Count[!is.na(rawdata$Complete) & !rawdata$Complete] <- 0
  rawdata$Complete <- NULL

  model.type <- "inla nbinomial: Year * (Month + Location)"
  d_ply(rawdata, "LocationGroupID", function(dataset){
    locationgroup.id <- dataset$LocationGroupID[1]
    dataset$LocationGroupID <- NULL
    dataset$fMonth <- factor(dataset$fMonth)
    dataset <- select_relevant_analysis(dataset)

    if (is.null(dataset)) {
      analysis <- n2k_inla_nbinomial(
        data = rawdata,
        scheme.id = scheme.id,
        speciesgroup.id = speciesgroup.id,
        locationgroup.id = locationgroup.id,
        model.type = model.type,
        covariate = "fYear + fMonth + fLocation",
        first.imported.year = first.year,
        last.imported.year = last.year,
        analysis.date = analysis.date,
        status = "insufficient data"
      )
      filename <- paste0(
        analysis.path,
        "/",
        get_file_fingerprint(analysis),
        ".rda"
      )
      if (!file.exists(filename)) {
        save(analysis, file = filename)
      }
      return(invisible(NULL))
    }
    if (length(unique(dataset$Year)) > 1) {
      dataset$fYear <- factor(dataset$Year)
      covariate <- "0 + fYear"
    } else {
      covariate <- "1"
    }
    dataset$Year <- NULL

    if (length(levels(dataset$fMonth)) == 1) {
      dataset$fMonth <- NULL
    } else {
      covariate <- c(covariate, "fMonth")
      if ("fYear" %in% colnames(dataset)) {
        dataset$fYearMonth <- interaction(
          dataset$fYear,
          dataset$fMonth,
          drop = TRUE
        )
        covariate <- c(covariate, "f(fYearMonth, model = 'iid')")
      }
    }

    if (length(unique(dataset$LocationID)) > 1) {
      dataset$fLocation <- factor(dataset$LocationID)
      covariate <- c(covariate, "f(fLocation, model = 'iid')")
      if ("fYear" %in% colnames(dataset)) {
        dataset$fYearLocation <- interaction(
          dataset$fYear,
          dataset$fLocation,
          drop = TRUE
        )
        covariate <- c(covariate, "f(fYearLocation, model = 'iid')")
      }
    }
    dataset$LocationID <- NULL
    sort.column <- na.omit(
      match(c("fLocation", "fYear", "fMonth"), colnames(dataset))
    )
    dataset <- dataset[do.call(order, dataset[, sort.column]), ]

    order.column <- c(
      "fLocation", "fYear", "fMonth", "Count", "Minimum", "ObservationID",
      "fYearLocation", "fYearMonth"
    )
    order.column <- order.column[order.column %in% colnames(dataset)]
    data <- dataset[, order.column]

    covariate <- paste(covariate, collapse = " + ")
    analysis <- n2k_inla_nbinomial(
      data = data,
      scheme.id = scheme.id,
      speciesgroup.id = speciesgroup.id,
      locationgroup.id = locationgroup.id,
      model.type = model.type,
      covariate = covariate,
      first.imported.year = first.year,
      last.imported.year = last.year,
      analysis.date = analysis.date
    )
    filename <- paste0(
      analysis.path,
      "/",
      get_file_fingerprint(analysis),
      ".rda"
    )
    if (!file.exists(filename)) {
      save(analysis, file = filename)
    }
    return(invisible(NULL))
  })
  return(invisible(NULL))
}
