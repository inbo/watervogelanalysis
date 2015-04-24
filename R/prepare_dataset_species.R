#' Read the species for the raw datasource, save them to the results database
#' @return the species group constraint information
#' @inheritParams read_specieslist
#' @inheritParams connect_flemish_source
#' @inheritParams prepare_dataset
#' @export
#' @importFrom n2khelper odbc_get_multi_id connect_result
#' @importFrom RODBC odbcClose
prepare_dataset_species <- function(
  scheme.id, 
  result.channel, 
  flemish.channel, 
  attribute.connection
){
  
  # read and save the species list
  species.list <- read_specieslist(
    flemish.channel = flemish.channel,
    attribute.connection = attribute.connection,
    limit = TRUE
  )
  database.id <- odbc_get_multi_id(
    data = species.list$species,
    id.field = "ID", merge.field = "ExternalCode", table = "Species",
    channel = result.channel, create = TRUE
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
    channel = result.channel, create = TRUE
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
    channel = result.channel, create = TRUE
  )
  
  species.constraint <- merge(
    species.list$species.constraint,
    species[, c("ExternalCode", "SpeciesID")]
  )
  species.constraint <- merge(
    species.constraint,
    species.group.species
  )
  
  return(species.constraint)
}
