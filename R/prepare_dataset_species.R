#' Read the species for the raw datasource, save them to the results database
#' @return the species group constraint information
#' @inheritParams read_specieslist
#' @inheritParams connect_flemish_source
#' @inheritParams prepare_dataset
#' @export
#' @importFrom n2khelper odbc_get_multi_id get_nbn_key_multi get_nbn_name
#' @importFrom RODBC odbcClose
#' @importFrom reshape2 dcast
prepare_dataset_species <- function(
  scheme.id, 
  result.channel, 
  flemish.channel, 
  attribute.connection,
  walloon.connection
){
  
  # read Flemish species list
  species.list <- read_specieslist(
    flemish.channel = flemish.channel,
    attribute.connection = attribute.connection,
    limit = TRUE
  )
  
  species <- get_nbn_key_multi(species.list$species, orders = c("la", "nl", "en"))
  species$DatasourceID <- datasource_id_flanders(result.channel = result.channel)
  species$Description <- species$DutchName
  
  species.constraint <- merge(
    species[, c("ExternalCode", "NBNID")],
    species.list$species.constraint
  )
  species.constraint$ExternalCode <- NULL
  
  # read Walloon species list
  walloon.species <- read_delim_git(file = "species.txt", connection = walloon.connection)
  walloon.species <- walloon.species[walloon.species$NBNID %in% species.constraint$NBNID, ]
  walloon.species$ExternalCode <- walloon.species$NBNID
  walloon.species$Description <- walloon.species$ScientificName
  walloon.species$DatasourceID <- datasource_id_wallonia(result.channel = result.channel)
  species <- rbind(
    species[, c("ExternalCode", "DatasourceID", "Description", "NBNID")],
    walloon.species[, c("ExternalCode", "DatasourceID", "Description", "NBNID")]
  )
  rm(walloon.species)
  
  database.id <- odbc_get_multi_id(
    data = species[, c("ExternalCode", "DatasourceID", "Description")],
    id.field = "ID", merge.field = c("ExternalCode", "DatasourceID"), 
    table = "Sourcespecies",
    channel = result.channel, 
    create = TRUE
  )
  species <- merge(database.id, species)
  colnames(species) <- gsub("^ID$", "SourcespeciesID", colnames(species))
  species$Description <- NULL
  
  nbn.species <- dcast(
    get_nbn_name(species$NBNID), 
    formula = NBNID ~ Language, 
    value.var = "Name"
  )
  colnames(nbn.species) <- gsub("^en$", "EnglishName", colnames(nbn.species))
  colnames(nbn.species) <- gsub("^la$", "ScientificName", colnames(nbn.species))
  colnames(nbn.species) <- gsub("^nl$", "DutchName", colnames(nbn.species))
  colnames(nbn.species) <- gsub("^fr$", "FrenchName", colnames(nbn.species))

  database.id <- odbc_get_multi_id(
    data = nbn.species,
    id.field = "ID", merge.field = "NBNID", 
    table = "Species",
    channel = result.channel, 
    create = TRUE
  )
  colnames(database.id) <- gsub("^ID$", "SpeciesID", colnames(database.id))
  nbn.species <- merge(nbn.species, database.id)

  species <- merge(species, database.id)
  database.id <- odbc_get_multi_id(
    data = species[, c("SpeciesID", "SourcespeciesID")],
    id.field = "ID", merge.field = c("SpeciesID", "SourcespeciesID"), 
    table = "SpeciesSourcespecies",
    channel = result.channel, 
    create = TRUE
  )
  
  #define each species as a species group
  
  species.group <- data.frame(
    Description = nbn.species$DutchName,
    SchemeID = scheme.id
  )
  if(anyNA(nbn.species$DutchName)){
    stop("At least one species without DutchName. NA is not acceptable as Speciesgroup Description")
  }
  database.id <- odbc_get_multi_id(
    data = species.group,
    id.field = "ID", 
    merge.field = c("Description", "SchemeID"), 
    table = "SpeciesGroup",
    channel = result.channel, 
    create = TRUE
  )
  species.group <- merge(database.id, species.group)
  colnames(species.group) <- gsub("^ID", "SpeciesGroupID", colnames(species.group))
  
  species.group.species <- merge(
    species.group[, c("Description", "SpeciesGroupID")],
    nbn.species[, c("DutchName", "SpeciesID")],
    by.x = "Description",
    by.y = "DutchName"
  )
  species.group.species$Description <- NULL
  database.id <- odbc_get_multi_id(
    data = species.group.species,
    id.field = "ID", 
    merge.field = c("SpeciesGroupID", "SpeciesID"), 
    table = "SpeciesGroupSpecies",
    channel = result.channel, create = TRUE
  )
  
  species.constraint <- merge(
    species.constraint,
    species[, c("NBNID", "SpeciesID", "ExternalCode", "DatasourceID")]
  )
  species.constraint$NBNID <- NULL
  species.constraint <- merge(
    species.constraint,
    species.group.species
  )
  species.constraint$SpeciesID <- NULL
  
  return(species.constraint)
}
