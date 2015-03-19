#' Create the analysis dataset based on the available raw data
#' 
#' The analysis dataset is saved to a rda file with the SHA-1 as name.
#' @param rawdata.file Name of the rawdata file
#' @param path Path to store the analysis files
#' @return A data.frame with the species id number of rows in the analysis dataset, number of precenses in the analysis datset and SHA-1 of the analysis dataset or NULL if not enough data.
#' @importFrom n2khelper read_delim_git
#' @importFrom lubridate ymd round_date year month 
#' @importFrom digest digest
#' @export
prepare_analysis_dataset <- function(rawdata.file, path = "."){
  if(!file_test("-d", path)){
    dir.create(path, recursive = TRUE)
  }
  species.id <- as.integer(gsub("_.*", "", rawdata.file))
  region <- gsub(
    ".*_", 
    "",
    gsub("\\.txt$", "", rawdata.file)
  )
  
  analysis <- read_delim_git(file = rawdata.file, path = "watervogel")
  if(class(analysis) != "data.frame"){
    stop(rawdata.file, " not available")
  }
  location <- read_delim_git(file = "location.txt", path = "watervogel")
  
  analysis$Date <- ymd(analysis$Date)
  location$StartDate <- ymd(location$StartDate)
  location$EndDate <- ymd(location$EndDate)
  
  analysis$Year <- year(round_date(analysis$Date, unit = "year"))
  location$StartYear <- year(round_date(location$StartDate, unit = "year"))
  location$EndYear <- year(round_date(location$EndDate, unit = "year"))

  analysis$fMonth <- factor(month(analysis$Date))
  analysis$Date <- NULL
  
  full.combination <- expand.grid(
    Year = unique(analysis$Year),
    fMonth = unique(analysis$fMonth),
    LocationID = unique(analysis$LocationID)
  )
  full.combination <- merge(
    full.combination, 
    location[, c("LocationID", "StartYear", "EndYear")]
  )
  rm(location)
  
  relevant.start <- is.na(full.combination$StartYear) | 
    full.combination$StartYear <= full.combination$Year
  relevant.end <- is.na(full.combination$EndYear) | 
    full.combination$EndYear >= full.combination$Year
  full.combination <- full.combination[relevant.start & relevant.end, ]
  rm(relevant.start, relevant.end)
  
  analysis <- merge(
    full.combination[, c("LocationID", "Year", "fMonth")], 
    analysis, 
    all.x = TRUE
  )
  analysis$Minimum <- pmax(0, analysis$Count, na.rm = TRUE)
  analysis$Count[!is.na(analysis$Count) & analysis$Complete == 0] <- NA
  analysis$Complete <- NULL
  
  analysis$fYear <- factor(analysis$Year)
  base.variable <- c("ObservationID", "Count", "Minimum", "fYear")
  base.model.set <- c("0 + fYear")
  
  if(length(levels(analysis$fMonth)) > 1){
    base.variable <- c(base.variable, "fMonth")
    base.model.set <- c(base.model.set, "fMonth")
  }

  analysis$fLocation <- factor(analysis$LocationID)
  if(length(levels(analysis$fLocation)) > 1){
    base.variable <- c(base.variable, "fLocation")
    if(length(levels(analysis$fLocation)) > 10){
      base.model.set <- c(base.model.set, "f(fLocation, model = 'iid')")
    } else {
      base.model.set <- c(base.model.set, "fLocation")
    }
  }
  
  base.model.set <- paste(base.model.set, collapse = " + ")

  analysis$fLocationYear <- interaction(analysis$fLocation, analysis$fYear)
  extra.variable <- "fLocationYear"
  extra.model.set <- "f(fLocationYear, model = 'iid')"

  analysis$cYear <- analysis$Year - min(analysis$Year)
  extra.variable <- c(extra.variable, "cYear")
  extra.model.set <- c(extra.model.set, "f(cYear, model = 'rw1', replicate = as.integer(fLocation))")
  
  model.type <- "inla nbinomial"
  data <- analysis[, base.variable]
  model.set <- base.model.set
  sha <- digest(
    list(species.id, region, model.type, model.set, data), 
    algo = "sha1"
  )
  save(species.id, region, model.type, model.set, data, file = paste0(path, "/", sha, ".rda"))
  
  for(i in seq_along(extra.model.set)){
    data <- analysis[, c(base.variable, extra.variable[i])]
    model.set <- paste(base.model.set, extra.model.set[i], sep = " + ")
    sha <- digest(
      list(species.id, region, model.type, model.set, data), 
      algo = "sha1"
    )
    save(
      species.id, region, model.type, model.set, data, 
      file = paste0(path, "/", sha, ".rda")
    )
  }
  
  return(analysis)
}
