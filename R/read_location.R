#' read the dataset of locations from the database
#' @export
#' @importFrom RODBC sqlQuery odbcClose
#' @examples
#' location <- read_location()
#' head(location)
read_location <- function(){
  channel <- connect_watervogel()
  sql <- "
    SELECT
      Code AS LocationID,
      Gebiedsnaam AS LocationName,
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
  location$SPA[is.na(location$SPA)] <- 0
  
  walloon.location <- read_delim_git(file = "location.txt", path = "watervogel/wallonia")
  if(class(walloon.location) == "logical"){
    return(location)
  }
  if(any(location$LocationID %in% walloon.location$LocationID)){
    stop("Identical id's in Flemish and Walloon locations")
  }
  
  walloon.location <- walloon.location[, c("LocationID", "LocationName", "SPA")]
  walloon.location$StartDate <- NA
  walloon.location$EndDate <- NA
  
  location <- rbind(location, walloon.location)
  location <- location[order(location$LocationID), ]
  
  return(location)
}
