#' Read the species list and their constraints
#' @param limit Return only species with explicit constraints (default = TRUE). Otherwise return all species in the database
#' @inheritParams n2khelper::odbc_connect
#' @export
#'@importFrom n2khelper read_delim_git
#'@importFrom RODBC sqlQuery odbcClose
#'@examples
#'species.list <- read_specieslist()
#'head(species.list$species)
#'head(species.list$species.constraint)
read_specieslist <- function(limit = TRUE, develop = TRUE){
  species.constraint <- read_delim_git(file = "soorttelling.txt", path = "watervogel/attribuut")
  channel <- odbc_connect(data.source.name = "Raw data watervogels Flanders", develop = develop)
  sql <- "
    SELECT
      EuringCode AS SpeciesID,
      NaamWetenschappelijk AS Species,
      NaamNederlands AS SpeciesNL,
      NaamEngels AS SpeciesEN
    FROM
      tblSoort
  "
  species <- sqlQuery(channel = channel, query = sql, stringsAsFactors = FALSE)
  odbcClose(channel)
  # restrict the species list to the species with constraints
  species.constraint <- merge(species.constraint, species[, c("SpeciesID", "SpeciesNL")])
  species.constraint$SpeciesNL <- NULL
  species.constraint <- species.constraint[
    order(species.constraint$SpeciesID, species.constraint$SpeciesCovered), 
  ]
  
  if(limit){
    species <- species[species$SpeciesID %in% species.constraint$SpeciesID, ]
  }
  species <- species[order(species$SpeciesID), ]

  return(
    list(
      species = species,
      species.constraint = species.constraint[, c("SpeciesID", "Firstyear", "SpeciesCovered")]
    )
  )
}
