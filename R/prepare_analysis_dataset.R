#' Create the analysis dataset based on the available raw data
#' 
#' The analysis dataset is saved to a rda file with the SHA-1 as name.
#' @param rawdata.file Name of the rawdata file
#' @param location a data.frame with the locations
#' @param path Path to store the analysis files
#' @return A data.frame with the species id number of rows in the analysis dataset, number of precenses in the analysis datset and SHA-1 of the analysis dataset or NULL if not enough data.
#' @importFrom n2khelper check_single_character check_dataframe_variable read_delim_git
#' @importFrom n2kanalysis select_factor_threshold
#' @importFrom plyr ddply
#' @importFrom digest digest
#' @export
prepare_analysis_dataset <- function(rawdata.file, location, path = "."){
  rawdata.file <- check_single_character(rawdata.file, name = "rawdata.file")
  path <- check_single_character(path, name = "path")
  check_dataframe_variable(
    df = location, 
    variable = c("LocationID", "LocationGroupID", "SubsetMonths", "StartYear", "EndYear"), 
    name = "location"
  )
  
  if(!file_test("-d", path)){
    dir.create(path, recursive = TRUE)
  }
  species.group.id <- as.integer(gsub("\\.txt", "", rawdata.file))
  
  rawdata <- read_delim_git(file = rawdata.file, path = "watervogel")
  if(class(rawdata) != "data.frame"){
    stop(rawdata.file, " not available")
  }
  
  rawdata$Complete <- rawdata$Complete == 1
  
  full.combination <- expand.grid(
    Year = unique(rawdata$Year),
    fMonth = unique(rawdata$fMonth),
    LocationID = unique(rawdata$LocationID)
  )
  full.combination <- merge(
    full.combination, 
    location[, c("LocationID", "LocationGroupID", "SubsetMonths", "StartYear", "EndYear")]
  )
  rm(location)
  
  remove.month <- full.combination$SubsetMonths & full.combination$fMonth %in% c("3", "10")
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

#   dataset <- subset(rawdata, LocationGroupID == 8)
  output <- ddply(rawdata, "LocationGroupID", function(dataset){
    location.group.id <- dataset$LocationGroupID[1]
    dataset$LocationGroupID <- NULL
    dataset$fMonth <- factor(dataset$fMonth)
    dataset <- select_relevant_analysis(dataset)
    
    if(length(unique(dataset$Year)) > 1){
      dataset$fYear <- factor(dataset$Year)
      covariate <- "0 + fYear"
    } else {
      covariate <- "1"
    }
    dataset$Year <- NULL

    if(length(levels(dataset$fMonth)) == 1){
      dataset$fMonth <- NULL
    } else {
      covariate <- c(covariate, "fMonth")
      if("fYear" %in% colnames(dataset)){
        dataset$fYearMonth <- interaction(dataset$fYear, dataset$fMonth, drop = TRUE)
        covariate <- c(covariate, "f(fYearMonth, model = 'iid')")
      }
    }
    
    if(length(unique(dataset$LocationID)) == 1){
      n.location <- 1
    } else {
      dataset$fLocation <- factor(dataset$LocationID)
      n.location <- length(levels(dataset$fLocation))
      covariate <- c(covariate, "f(fLocation, model = 'iid')")
      if("fYear" %in% colnames(dataset)){
        dataset$fYearLocation <- interaction(dataset$fYear, dataset$fLocation, drop = TRUE)
        covariate <- c(covariate, "f(fYearLocation, model = 'iid')")
      }
    }
    dataset$LocationID <- NULL
    sort.column <- na.omit(match(c("fLocation", "fYear", "fMonth"), colnames(dataset)))
    dataset <- dataset[do.call(order, dataset[, sort.column]), ]
    
    order.column <- c("fLocation", "fYear", "fMonth", "Count", "Minimum", "ObservationID", "fYearLocation", "fYearMonth")
    order.column <- order.column[order.column %in% colnames(dataset)]
    dataset <- dataset[, order.column]
    
    covariate <- paste(covariate, collapse = " + ")
    modeltype <- "inla_nbinomial: Year * (Month + Location)"
    data.fingerprint <- digest(dataset, algo = "sha1")
    file.fingerprint <- digest(
      list(
        species.group.id, location.group.id, data, covariate, modeltype, data.fingerprint
      ),
      algo = "sha1"
    )
    save(
      species.group.id, location.group.id, data, covariate, modeltype, data.fingerprint,
      file = paste0(path, "/", file.fingerprint, ".rda")
    )
    data.frame(
      ModelType = modeltype,
      Covariate = covariate,
      Fingerprint = file.fingerprint,
      NObs = nrow(dataset),
      NLocation = n.location
    )
  })
  output$SpeciesGroupID <- species.group.id  

  return(output)
}
