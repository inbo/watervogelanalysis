#' read the dataset of locations from the database
#' @inheritParams n2khelper::odbc_connect
#' @export
#' @importFrom n2khelper check_single_logical odbc_connect get_datasource_id git_connect
#' @importFrom RODBC sqlQuery odbcClose
#' @examples
#' location <- read_location()
#' head(location)
read_location <- function(develop = TRUE){
  
  # read Flemisch data from the database
  data.source.name <- "Raw data watervogels Flanders"
  data.source.id <- get_datasource_id(data.source.name = data.source.name)
  channel <- odbc_connect(data.source.name = data.source.name, develop = develop)
  sql <- "
    SELECT
      Code AS ExternalCode,
      Gebiedsnaam AS Description,
      BeginDatum AS StartDate,
      EindDatum AS EndDate,
      ABS(EgVogelrichtlijngebied) AS SPA
    FROM
      tblGebied
    WHERE
      Actief = 1
    ORDER BY
      Code
  "
  location <- sqlQuery(channel = channel, query = sql, stringsAsFactors = FALSE)
  odbcClose(channel)
  location$DatasourceID <- data.source.id
  location$SPA[is.na(location$SPA)] <- 0
  
  # Read Walloon data from the git repository
  data.source.name <- "Raw data watervogels Wallonia"
  data.source.id <- get_datasource_id(data.source.name = data.source.name)
  path <- git_connect(data.source.name = data.source.name, develop = develop)
  walloon.location <- read_delim_git(file = "location.txt", path = path)
  if(class(walloon.location) == "logical"){
    Encoding(location$Description) <- "UTF-8"
    return(location)
  }
  walloon.location <- walloon.location[, c("LocationID", "LocationName", "SPA")]
  colnames(walloon.location) <- c("ExternalCode", "Description", "SPA")  
  walloon.location$SPA[is.na(walloon.location$SPA)] <- 0
  walloon.location$StartDate <- NA
  walloon.location$EndDate <- NA
  walloon.location$DatasourceID <- data.source.id
  
  location <- rbind(location, walloon.location)
  Encoding(location$Description) <- "UTF-8"

  return(location)
}
