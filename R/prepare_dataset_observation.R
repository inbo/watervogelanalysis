#' Read the observations for the raw datasource, save them to the git repository and the results database
#' @param this.constraint the constraints for this species group
#' @param location a data frame with the full list of locations
#' @inheritParams n2khelper::odbc_connect
#' @inheritParams prepare_dataset
#' @export
#' @importFrom n2khelper check_dataframe_variable odbc_get_id odbc_get_multi_id check_id
#' @importFrom lubridate round_date year month
#' @importFrom n2kanalysis mark_obsolete_dataset
prepare_dataset_observation <- function(
  this.constraint, 
  location, 
  scheme.id = odbc_get_id(
    table = "Scheme", variable = "Description", value = "Watervogels", develop = develop
  ), 
  develop = TRUE
){
  check_dataframe_variable(
    df = this.constraint, 
    variable = c("Firstyear", "ExternalCode", "SpeciesCovered", "SpeciesGroupID"),
    name = "this.constraint"
  )
  check_dataframe_variable(
    df = location, 
    variable = c("ID", "ExternalCode", "DatasourceID"),
    name = "location"
  )
  if(nrow(unique(this.constraint[, c("ExternalCode", "SpeciesGroupID", "Firstyear")])) != 1){
    stop("this.constraint must contain just one ExternalCode, one SpeciesGroupID and one Firstyear")
  }
  
  import.date <- Sys.time()
  observation.flemish <- read_observation(
    species.id = this.constraint$ExternalCode[1], 
    first.winter = this.constraint$Firstyear[1], 
    species.covered = this.constraint$SpeciesCovered,
    develop = develop
  )
  flanders.id <- odbc_get_id(
    table = "Datasource", 
    variable = "Description", 
    value = "Raw data watervogels Flanders", 
    develop = develop
  )
  observation.flemish$DatasourceID <- flanders.id
  
  observation.walloon <- read_observation_wallonia(
    species.id = this.constraint$ExternalCode[1], 
    first.winter = this.constraint$Firstyear[1]
  )
  if(is.null(observation.walloon)){
    observation <- observation.flemish
  } else {
    observation.walloon$DatasourceID <- odbc_get_id(
      table = "Datasource", 
      variable = "Description", 
      value = "Raw data watervogels Wallonia", 
      develop = develop
    )
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
  pathname <- "watervogel"
  if(is.null(observation)){
    observation.sha <- NA
    status.id <- odbc_get_id(
      table = "AnalysisStatus", 
      variable = "Description",
      value = "No data",
      develop = develop
    )
  } else {
    observation.sha <- write_delim_git(
      x = observation, 
      file = filename, 
      path = pathname
    )
    status.id <- odbc_get_id(
      table = "AnalysisStatus", 
      variable = "Description",
      value = "Working",
      develop = develop
    )
  }
  
  import.id <- odbc_get_id(
    table = "ModelType", 
    variable = "Description", 
    value = "import", 
    develop = develop
  )
  model.set <- data.frame(
    ModelTypeID = import.id,
    FirstYear = this.constraint$Firstyear[1],
    LastYear = NA,
    Duration = NA
  )
  
  channel <- connect_result(develop = develop)
  model.set.id <- odbc_get_multi_id(
    data = model.set,
    id.field = "ID", merge.field = c("ModelTypeID", "FirstYear", "LastYear", "Duration"), 
    table = "ModelSet",
    channel = channel, create = TRUE
  )$ID

#   version <- paste(
#     "watervogelanalyis version", 
#     sessionInfo()$otherPkgs$watervogelanalysis$Version
#   )
#   version.id <- odbc_get_id(
#     table = "AnalysisVersion", 
#     variable = "Description", 
#     value = version, 
#     develop = develop
#   )
  version.id <- 4
  test <- check_id(value = version.id, field = "ID", table = "AnalysisVersion", channel = channel)
  if(!test){
    stop("Unknown version id")
  }
  warning("update code when version description is nchar instead of int")
  
  analysis <- data.frame(
    ModelSetID = model.set.id,
    LocationGroupID = odbc_get_id(
      table = "LocationGroup",
      variable = c("Description", "SchemeID"),
      value = c("Belgi\\u0137", scheme.id),
      develop = develop
    ),
    SpeciesGroupID = this.constraint$SpeciesGroupID[1],
    AnalysisVersionID = version.id,
    AnalysisDate = import.date,
    StatusID = status.id
  )
  warning("add fingerprint to code when available in database")
  analysis.id <- odbc_get_multi_id(
    data = analysis,
    id.field = "ID", 
    merge.field = c("ModelSetID", "LocationGroupID", "SpeciesGroupID", "AnalysisVersionID"),
    table = "Analysis",
    channel = channel,
    create = TRUE
  )$ID

  sql <- "
    SELECT
      ID, FileName, PathName, Fingerprint
    FROM
      Dataset
    WHERE
      PathName = 'watervogel' AND
      Filename IN ('location.txt', 'locationgroup.txt', 'locationgrouplocation.txt') AND
      Obsolete = 0
  "
  location.ds <- sqlQuery(channel = channel, query = sql)

  if(!is.na(observation.sha)){
    dataset <- data.frame(
      FileName = filename,
      PathName = "watervogel",
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
        channel = channel, create = TRUE
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
    channel = channel, create = TRUE
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
      develop = develop
    )
    duplicate$AnalysisID <- analysis.id
    database.id <- odbc_get_multi_id(
      data = duplicate,
      id.field = "ID", 
      merge.field = colnames(duplicate),
      table = "Anomaly",
      channel = channel,
      create = TRUE
    )
  }  

  odbcClose(channel)
}
