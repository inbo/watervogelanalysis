#' Read the species list and their constraints
#' @param limit Return only species with explicit constraints (default = TRUE). Otherwise return all species in the database
#' @param flemish.channel An open ODBC connection to the source database
#' @param attribute.connection a git-connection object to the attributes
#' @inheritParams connect_flemish_source
#' @export
#' @importFrom n2khelper check_single_logical odbc_connect read_delim_git
#' @importFrom RODBC sqlQuery odbcClose
#' @importFrom assertthat assert_that is.flag noNA
#' @examples
#' result.channel <- n2khelper::connect_result()
#' flemish.channel <- connect_flemish_source(result.channel = result.channel)
#' attribute.connection <- connect_attribute(
#'   result.channel = result.channel,
#'   username = "Someone",
#'   password = "xxxx",
#'   commit.user = "Someone",
#'   commit.email = "some\\u0040one.com"
#' )
#' species.list <- read_specieslist(
#'   result.channel = result.channel,
#'   flemish.channel = flemish.channel,
#'   attribute.connection = attribute.connection
#' )
#' head(species.list$species)
#' head(species.list$species.constraint)
read_specieslist <- function(
  result.channel,
  flemish.channel,
  attribute.connection,
  limit = TRUE
){
  assert_that(is.flag(limit))
  assert_that(noNA(limit))

  sql <- "
    SELECT
      EuringCode AS ExternalCode,
      NaamWetenschappelijk AS ScientificName,
      NaamNederlands AS DutchName,
      NaamEngels AS EnglishName,
      NaamFrans AS FrenchName
    FROM
      tblSoort
  "
  species <- sqlQuery(
    channel = flemish.channel,
    query = sql,
    stringsAsFactors = FALSE
  )
  species$DatasourceID <- datasource_id_flanders(
    result.channel = result.channel
  )
  species$TableName <- "tblSoort"
  species$ColumnName <- "EuringCode"

  # restrict the species list to the species with constraints
  species.constraint <- read_delim_git(
    file = "soorttelling.txt",
    connection = attribute.connection
  )
  colnames(species.constraint)[1] <- "DutchName"

  species.constraint <- merge(
    species[, c("DutchName", "ExternalCode")],
    species.constraint
  )
  species.constraint$DutchName <- NULL

  species.constraint <- merge(
    species.constraint,
    species[, c("ExternalCode", "DutchName")]
  )
  species.constraint$DutchName <- NULL

  if (limit) {
    species <- species[
      species$ExternalCode %in% species.constraint$ExternalCode,
    ]
  }

  return(
    list(
      species = species,
      species.constraint = species.constraint[
        ,
        c("ExternalCode", "Firstyear", "SpeciesCovered")
      ]
    )
  )
}
