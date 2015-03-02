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
      EgVogelrichtlijngebied AS SPA
    FROM
      tblGebied
    WHERE
      Actief = 1
  "
  location <- sqlQuery(channel = channel, query = sql, stringsAsFactors = FALSE)
  odbcClose(channel)
  location$SPA[is.na(location$SPA)] <- 0
  location$SPA <- location$SPA == -1
  return(location)
}
