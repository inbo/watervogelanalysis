#' Read the species list and their constraints
#' @param limit Return only species with explicit constraints (default = TRUE). Otherwise return all species in the database
#' @inheritParams n2khelper::odbc_connect
#' @export
#'@importFrom n2khelper check_single_logical odbc_connect read_delim_git
#'@importFrom RODBC sqlQuery odbcClose
#'@examples
#'species.list <- read_specieslist()
#'head(species.list$species)
#'head(species.list$species.constraint)
read_specieslist <- function(limit = TRUE, develop = TRUE){
  limit <- check_single_logical(limit)
  
  channel <- odbc_connect(
    data.source.name = "Raw data watervogels Flanders", develop = develop
  )
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
  species <- sqlQuery(channel = channel, query = sql, stringsAsFactors = FALSE)
  odbcClose(channel)
  
  # restrict the species list to the species with constraints
  species.constraint <- read_delim_git(
    file = "soorttelling.txt", path = "watervogel/attribuut"
  )
  colnames(species.constraint)[1] <- "DutchName"

  species.constraint <- merge(species.constraint, species[, c("ExternalCode", "DutchName")])
  species.constraint$DutchName <- NULL
  
  if(limit){
    species <- species[species$ExternalCode %in% species.constraint$ExternalCode, ]
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
