#' read the dataset of locations from the database
#' @inheritParams read_specieslist
#' @inheritParams connect_flemish_source
#' @inheritParams prepare_dataset
#' @export
#' @importFrom n2khelper odbc_get_id odbc_connect git_connect
#' @importFrom git2rdata read_vc
#' @importFrom RODBC sqlQuery
#' @examples
#' \dontrun{
#' result.channel <- n2khelper::connect_result()
#' flemish.channel <- connect_flemish_source(result.channel = result.channel)
#' walloon.connection <- connect_walloon_source(
#'   result.channel = result.channel,
#'   username = "Someone",
#'   password = "xxxx",
#'   commit.user = "Someone",
#'   commit.email = "some\\u0040one.com"
#' )
#' location <- read_location(
#'   result.channel = result.channel,
#'   flemish.channel = flemish.channel,
#'   walloon.connection = walloon.connection
#' )
#' head(location)
#' }
read_location <- function(result.channel, flemish.channel, walloon.connection){

  # read Flemish data from the database
  datasource.id <- datasource_id_flanders(result.channel = result.channel)
  sql <- "
    SELECT
      Code AS external_code,
      Gebiedsnaam AS description,
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
  location <- sqlQuery(
    channel = flemish.channel,
    query = sql,
    stringsAsFactors = FALSE
  )
  location$datasource <- datasource.id
  location$SPA[is.na(location$SPA)] <- 0
  location$Region <- "Flanders"

  # Read Walloon data from the git repository
  datasource.id <- datasource_id_wallonia(result.channel = result.channel)
  walloon.location <- read_vc(file = "location.txt", root = walloon.connection)
  if (class(walloon.location) == "logical") {
    Encoding(location$Description) <- "UTF-8"
    return(location)
  }
  walloon.location <- walloon.location[, c("LocationID", "LocationName", "SPA")]
  colnames(walloon.location) <- c("external_code", "description", "SPA")
  walloon.location$SPA[is.na(walloon.location$SPA)] <- 0
  walloon.location$StartDate <- NA
  walloon.location$EndDate <- NA
  walloon.location$datasource <- datasource.id
  walloon.location$Region <- "Wallonia"
  location <- rbind(location, walloon.location)
  Encoding(location$description) <- "UTF-8"

  return(location)
}
