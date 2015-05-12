#' Read the observations for the raw datasource, save them to the git repository and the results database
#' @param this.constraint the constraints for this species group
#' @param location a data frame with the full list of locations
#' @inheritParams read_specieslist
#' @inheritParams prepare_dataset
#' @inheritParams connect_flemish_source
#' @export
#' @importFrom n2khelper check_dataframe_variable odbc_get_id odbc_get_multi_id check_id read_delim_git check_single_strictly_positive_integer
#' @importFrom lubridate round_date year month
#' @importFrom n2kanalysis mark_obsolete_dataset
#' @importFrom digest digest
prepare_dataset_observation <- function(
  this.constraint, location, flemish.channel, walloon.connection, result.channel, raw.connection, scheme.id
){
  check_dataframe_variable(
    df = this.constraint, 
    variable = c("Firstyear", "ExternalCode", "DatasourceID", "SpeciesCovered", "SpeciesGroupID"),
    name = "this.constraint"
  )
  check_dataframe_variable(
    df = location, 
    variable = c("ID", "ExternalCode", "DatasourceID"),
    name = "location"
  )
  external <- unique(this.constraint[, c("ExternalCode", "DatasourceID")])
  if(any(table(external$ExternalCode, external$DatasourceID) > 1)){
    stop("Each datasource must use just one ExternalCode")
  }
  if(nrow(unique(this.constraint[, c("SpeciesGroupID", "Firstyear")])) != 1){
    stop("this.constraint must contain just one SpeciesGroupID and one Firstyear")
  }
  scheme.id <- check_single_strictly_positive_integer(scheme.id, name = "scheme.txt")
  
  import.date <- Sys.time()
  flanders.id <- datasource_id_flanders(result.channel = result.channel)
  observation.flemish <- read_observation(
    species.id = as.integer(
      this.constraint$ExternalCode[this.constraint$DatasourceID == flanders.id][1]
    ), 
    first.winter = this.constraint$Firstyear[1], 
    last.winter = this.constraint$Lastyear[1],
    species.covered = unique(this.constraint$SpeciesCovered),
    flemish.channel = flemish.channel
  )
  observation.flemish$DatasourceID <- flanders.id
  
  wallonia.id <- datasource_id_wallonia(result.channel = result.channel)
  if(any(this.constraint$DatasourceID == wallonia.id)){
    observation.walloon <- read_observation_wallonia(
      species.id = this.constraint$ExternalCode[this.constraint$DatasourceID == wallonia.id][1], 
      first.winter = this.constraint$Firstyear[1],
      last.winter = this.constraint$Lastyear[1],
      walloon.connection = walloon.connection
    )
  } else {
    observation.walloon <- NULL
  }
  if(is.null(observation.walloon)){
    observation <- observation.flemish
  } else {
    observation.walloon$DatasourceID <- wallonia.id
    observation <- rbind(observation.flemish, observation.walloon)
  }

  observation$Year <- year(round_date(observation$Date, unit = "year"))
  observation$fMonth <- factor(month(observation$Date))
  observation$Date <- NULL
  
  observation <- merge(
    observation, 
    location[, c("ID", "ExternalCode", "DatasourceID")], 
    by.x = c("LocationID", "DatasourceID"), 
    by.y = c("ExternalCode", "DatasourceID")
  )
  observation$LocationID <- observation$ID
  
  observation <- select_relevant_import(observation)
  
  result <- remove_duplicate_observation(observation)
  
  observation <- result$Observation[
    order(
      result$Observation$LocationID, 
      result$Observation$Year, 
      result$Observation$fMonth
    ),
    c("LocationID", "Year", "fMonth", "ObservationID", "Complete", "Count")
  ]
  
  filename <- paste0(this.constraint$SpeciesGroupID[1], ".txt")
  if(is.null(observation)){
    observation.sha <- NA
    status.id <- odbc_get_id(
      table = "AnalysisStatus", 
      variable = "Description",
      value = "No data",
      channel = result.channel
    )
  } else {
    observation.sha <- write_delim_git(
      x = observation, 
      file = filename, 
      connection = raw.connection
    )
    status.id <- odbc_get_id(
      table = "AnalysisStatus", 
      variable = "Description",
      value = "Working",
      channel = result.channel
    )
  }
  
  import.id <- odbc_get_id(
    table = "ModelType", 
    variable = "Description", 
    value = "import", 
    channel = result.channel
  )
  model.set <- data.frame(
    ModelTypeID = import.id,
    FirstYear = this.constraint$Firstyear[1],
    LastYear = this.constraint$Lastyear[1],
    Duration = this.constraint$Lastyear[1] - this.constraint$Firstyear[1] + 1
  )
  
  model.set.id <- odbc_get_multi_id(
    data = model.set,
    id.field = "ID", merge.field = c("ModelTypeID", "FirstYear", "LastYear", "Duration"), 
    table = "ModelSet",
    channel = result.channel, create = TRUE
  )$ID

  version <- data.frame(
    Description = paste(
      "watervogelanalyis version", 
      sessionInfo()$otherPkgs$watervogelanalysis$Version
    ),
    Obsolete = FALSE
  )
  version.id <- odbc_get_multi_id(
    data = version,
    id.field = "ID", 
    merge.field = "Description", 
    table = "AnalysisVersion",
    channel = result.channel, 
    create = TRUE
  )$ID
  
  sql <- paste0("
    SELECT
      ID, FileName, PathName, Fingerprint
    FROM
      Dataset
    WHERE
      PathName = '", raw.connection@LocalPath, "' AND
      Filename IN ('location.txt', 'locationgroup.txt', 'locationgrouplocation.txt') AND
      Obsolete = 0
  ")
  location.ds <- sqlQuery(channel = result.channel, query = sql, stringsAsFactors = FALSE)
  sha <- sort(c(location.ds$Fingerprint, observation.sha))
  
  analysis <- data.frame(
    ModelSetID = model.set.id,
    LocationGroupID = odbc_get_id(
      table = "LocationGroup",
      variable = c("Description", "SchemeID"),
      value = c("Belgi\\u0137", scheme.id),
      channel = result.channel
    ),
    SpeciesGroupID = this.constraint$SpeciesGroupID[1],
    AnalysisVersionID = version.id,
    AnalysisDate = import.date,
    StatusID = status.id,
    Fingerprint = digest(sha, algo = "sha1")
  )
  analysis.id <- odbc_get_multi_id(
    data = analysis,
    id.field = "ID", 
    merge.field = c("ModelSetID", "LocationGroupID", "SpeciesGroupID", "AnalysisVersionID", "Fingerprint"),
    table = "Analysis",
    channel = result.channel,
    create = TRUE
  )$ID

  if(!is.na(observation.sha)){
    dataset <- data.frame(
      FileName = filename,
      PathName = raw.connection@LocalPath,
      Fingerprint = observation.sha,
      ImportDate = import.date,
      Obsolete = FALSE
    )
    location.ds <- rbind(
      location.ds,
      odbc_get_multi_id(
        data = dataset,
        id.field = "ID", merge.field = c("FileName", "PathName", "Fingerprint"), 
        table = "Dataset", 
        channel = result.channel, create = TRUE
      )
    )
  }
  analysis.dataset <- data.frame(
    AnalysisID = analysis.id,
    DatasetID = location.ds$ID
  )
  database.id <- odbc_get_multi_id(
    data = analysis.dataset,
    id.field = "ID", merge.field = c("AnalysisID", "DatasetID"), 
    table = "AnalysisDataset", 
    channel = result.channel, create = TRUE
  )

  if(nrow(result$Duplicate) > 0){
    duplicate <- result$Duplicate
    duplicate$TableName <- ifelse(
      duplicate$DatasourceID == flanders.id, "tblWaarneming", "visit.txt"
    )
    duplicate$Column1Name <- ifelse(
      duplicate$DatasourceID == flanders.id, "ID", "ObservationID"
    )
    duplicate$Column1Value <- duplicate$ObservationID
    duplicate$ObservationID <- NULL
    duplicate$Column2Name <- NA
    duplicate$Column2Value <- NA
    
    duplicate$TypeID <- odbc_get_id(
      table = "AnomalyType", 
      variable = "Description", 
      value = "Duplicate record", 
      channel = result.channel
    )
    duplicate$AnalysisID <- analysis.id
    database.id <- odbc_get_multi_id(
      data = duplicate,
      id.field = "ID", 
      merge.field = colnames(duplicate),
      table = "Anomaly",
      channel = result.channel,
      create = TRUE
    )
  }  
}
