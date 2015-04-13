#' Read the species for the raw datasource, save them to the results database
#' @return the species group constraint information
#' @inheritParams n2khelper::odbc_connect
#' @inheritParams prepare_dataset
#' @export
#' @importFrom n2khelper odbc_get_multi_id connect_result
#' @importFrom RODBC odbcClose
prepare_dataset_species <- function(
  scheme.id = odbc_get_id(
    table = "Scheme", variable = "Description", value = "Watervogels", develop = develop
  ), 
  develop = develop
){
  
  # read and save the species list
  species.list <- read_specieslist(limit = TRUE, develop = develop)
  channel <- connect_result(develop = develop)
  database.id <- odbc_get_multi_id(
    data = species.list$species,
    id.field = "ID", merge.field = "ExternalCode", table = "Species",
    channel = channel, create = TRUE
  )
  species <- merge(database.id, species.list$species[, c("ExternalCode", "DutchName")])
  colnames(species)[2] <- "SpeciesID"
  
  #define each species as a species group
  species.group <- data.frame(
    Description = species$DutchName,
    SchemeID = scheme.id
  )
  database.id <- odbc_get_multi_id(
    data = species.group,
    id.field = "ID", merge.field = c("Description", "SchemeID"), table = "SpeciesGroup",
    channel = channel, create = TRUE
  )
  species.group <- merge(database.id, species.group)
  colnames(species.group)[3] <- "SpeciesGroupID"
  
  species.group.species <- merge(
    species.group[, c("Description", "SpeciesGroupID")],
    species[, c("DutchName", "SpeciesID")],
    by.x = "Description",
    by.y = "DutchName"
  )
  species.group.species$Description <- NULL
  database.id <- odbc_get_multi_id(
    data = species.group.species,
    id.field = "ID", merge.field = c("SpeciesGroupID", "SpeciesID"), 
    table = "SpeciesGroupSpecies",
    channel = channel, create = TRUE
  )
  
  species.constraint <- merge(
    species.list$species.constraint,
    species[, c("ExternalCode", "SpeciesID")]
  )
  species.constraint <- merge(
    species.constraint,
    species.group.species
  )
  
  odbcClose(channel)
  
  return(species.constraint)
}
