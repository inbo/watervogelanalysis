#' Read the locations for the raw datasource, save them to the git repository and the results database
#' @return a data.frame with the locations
#' @inheritParams n2khelper::odbc_connect
#' @inheritParams prepare_dataset
#' @export
#' @importFrom n2khelper odbc_get_multi_id write_delim_git connect_result
#' @importFrom n2kanalysis mark_obsolete_dataset
#' @importFrom RODBC odbcClose
prepare_dataset_location <- function(
    scheme.id = odbc_get_id(
    table = "Scheme", variable = "Description", value = "Watervogels", develop = develop
  ), 
  develop = develop
){
  import.date <- Sys.time()

  # read the locations
  location <- read_location(develop = develop)
  
  # store the locations
  channel <- connect_result(develop = develop)
  database.id <- odbc_get_multi_id(
    data = location[, c("ExternalCode", "DatasourceID", "Description")],
    id.field = "ID", merge.field = c("ExternalCode", "DatasourceID"), table = "Location",
    channel = channel, create = TRUE
  )
  location <- merge(database.id, location)
  location$Description <- NULL
  
  # define and save location groups
  # \\u0137 is the ASCII code for e umlaut
  location.group <- data.frame(
    Description = c("Vlaanderen", "Walloni\\u0137", "Belgi\\u0137", "Vogelrichtlijn Vlaanderen", "Vogelrichtlijn Walloni\\u0137", "Vogelrichtlijn Belgi\\u0137"),
    Flanders = c(TRUE, FALSE, TRUE, TRUE, FALSE, TRUE),
    Wallonia = c(FALSE, TRUE, TRUE, FALSE, TRUE, TRUE),
    SPA = c(FALSE, FALSE, FALSE, TRUE, TRUE, TRUE),
    Impute = c("Vlaanderen", "Walloni\\u0137", "Belgi\\u0137", "Vlaanderen", "Walloni\\u0137", "Belgi\\u0137"),
    SubsetMonths = c(FALSE, TRUE, TRUE, FALSE, TRUE, TRUE),
    SchemeID = scheme.id
  )
  database.id <- odbc_get_multi_id(
    data = location.group[, c("SchemeID", "Description")],
    id.field = "ID", merge.field = c("SchemeID", "Description"), table = "LocationGroup",
    channel = channel, create = TRUE
  )
  location.group <- merge(database.id, location.group)
  location.group <- merge(
    location.group,
    location.group[, c("ID", "Description")],
    by.x = "Impute",
    by.y = "Description",
    suffixes = c("", ".y")
  )
  location.group$Impute <- location.group$ID.y
  
  location.group$ID.y <- NULL
  location.group$SchemeID <- NULL
  location.group$Description <- NULL
  
  # get the locations per location group
  flanders <- odbc_get_id(
    table = "Datasource", 
    variable = "Description", 
    value = "Raw data watervogels Flanders", 
    develop = develop
  )
  wallonia <- odbc_get_id(
    table = "Datasource", 
    variable = "Description", 
    value = "Raw data watervogels Wallonia", 
    develop = develop
  )
  location.group.location <- do.call(rbind, lapply(
    seq_along(location.group$ID),
    function(i){
      if(location.group$SPA[i]){
        selection <- location$SPA == 1
      } else {
        selection <- rep(TRUE, nrow(location))
      }
      if(location.group$Flanders[i]){
        if(!location.group$Wallonia[i]){
          selection <- selection & location$DatasourceID == flanders
        }
      } else {
        selection <- selection & location$DatasourceID == wallonia
      }
      data.frame(
        LocationGroupID = location.group$ID[i],
        LocationID = location$ID[selection]
      )
    }
  ))
  database.id <- odbc_get_multi_id(
    data = location.group.location[, c("LocationGroupID", "LocationID")],
    id.field = "ID", merge.field = c("LocationGroupID", "LocationID"), 
    table = "LocationGroupLocation",
    channel = channel, create = TRUE
  )
  
  location <- location[
    order(location$ID), 
    c("ID", "StartDate", "EndDate", "ExternalCode", "DatasourceID")
    ]
  location.group <- location.group[
    order(location.group$ID), 
    c("ID", "Impute", "SubsetMonths")
  ]
  location.group.location <- location.group.location[
    order(
      location.group.location$LocationGroupID,
      location.group.location$LocationID
    ), 
    c("LocationGroupID", "LocationID")
  ]
  
  
  # store the datasets in the git repository
  location.sha <- write_delim_git(
    x = location[, c("ID", "StartDate", "EndDate")], 
    file = "location.txt", path = "watervogel"
  )
  location.group.sha <- write_delim_git(
    x = location.group, file = "locationgroup.txt", path = "watervogel"
  )
  location.group.location.sha <- write_delim_git(
    x = location.group.location, file = "locationgrouplocation.txt", path = "watervogel"
  )
  
  dataset <- data.frame(
    FileName = c("location.txt", "locationgroup.txt", "locationgrouplocation.txt"),
    PathName = "watervogel",
    Fingerprint = c(location.sha, location.group.sha, location.group.location.sha),
    ImportDate = import.date,
    Obsolete = FALSE
  )
  database.id <- odbc_get_multi_id(
    data = dataset,
    id.field = "ID", merge.field = c("FileName", "PathName", "Fingerprint"), 
    table = "Dataset", 
    channel = channel, create = TRUE
  )
  mark_obsolete_dataset(develop = develop)
  
  odbcClose(channel)
  
  return(location)
}
