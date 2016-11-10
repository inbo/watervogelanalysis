#' Read the species for the raw datasource, save them to the results database
#' @return the species group constraint information
#' @inheritParams read_specieslist
#' @inheritParams connect_flemish_source
#' @inheritParams prepare_dataset
#' @export
#' @importFrom assertthat assert_that is.string
#' @importFrom n2khelper get_nbn_key_multi read_delim_git write_delim_git
#' @importFrom dplyr %>% mutate_ select_ inner_join filter_ bind_rows transmute_ distinct_ arrange_
#' @importFrom n2kupdate store_species_group_species
prepare_dataset_species <- function(
  raw.connection,
  result.channel,
  flemish.channel,
  attribute.connection,
  walloon.connection,
  nbn.channel,
  scheme.id
){
  assert_that(is.string(scheme.id))

  # read Flemish species list
  species.list <- read_specieslist(
    result.channel = result.channel,
    flemish.channel = flemish.channel,
    attribute.connection = attribute.connection,
    limit = TRUE
  )

  species <- get_nbn_key_multi(
    species = species.list$species,
    orders = c("la", "nl", "en"),
    channel = nbn.channel
  ) %>%
    mutate_(
      Description = ~DutchName
    )

  species.constraint <- species %>%
    select_(~ExternalCode, ~NBNKey) %>%
    inner_join(
      species.list$species.constraint,
      by = "ExternalCode"
    ) %>%
    select_(~-ExternalCode)

  # read Walloon species list
  source.species <- read_delim_git(
    file = "species.txt",
    connection = walloon.connection
  ) %>%
    filter_(~NBNKey %in% species.constraint$NBNKey) %>%
    mutate_(
      ExternalCode = ~NBNKey,
      Description = ~ScientificName,
      DatasourceID = ~datasource_id_wallonia(
        result.channel = result.channel
      ),
      TableName = ~"species.txt",
      ColumnName = ~"NBNKey",
      Datatype = ~"character"
    ) %>%
  bind_rows(
    species %>%
      transmute_(
        ExternalCode = ~as.character(ExternalCode),
        ~DatasourceID,
        ~Description,
        ~NBNKey,
        ~TableName,
        ~ColumnName,
        ~Datatype
      )
  )
  export_datafield <- source.species %>%
    distinct_(~DatasourceID, ~TableName, ~ColumnName, ~Datatype) %>%
    transmute_(
      local_id = ~DatasourceID,
      datasource = ~DatasourceID,
      table_name = ~TableName,
      primary_key = ~ColumnName,
      datafield_type = ~Datatype
    )

  export_sourcespecies <- source.species %>%
    transmute_(
      local_id = ~ExternalCode,
      description = ~Description,
      datafield_local_id = ~DatasourceID,
      external_code = ~ExternalCode
    )

  export_species <- species %>%
    transmute_(
      local_id = ~NBNKey,
      scientific_name = ~ScientificName,
      nbn_key = ~NBNKey,
      nl = ~DutchName,
      en = ~EnglishName,
      fr = ~FrenchName
    )

  # \u00E7 is the UTF-8 code for c with cedilla
  export_language <- data.frame(
    code = c("nl", "en", "fr"),
    description = c("Nederland", "English", "Fran\u00E7ais")
  )

  export_speciesgroup <- species %>%
    transmute_(
      local_id = ~NBNKey,
      description = ~DutchName,
      scheme = ~scheme.id
    )

  export_speciesgroupspecies <- species %>%
    transmute_(
      species_local_id = ~NBNKey,
      species_group_local_id = ~NBNKey
    )

  export_sourcespeciesspecies <- source.species %>%
    select_(
      species_local_id = ~NBNKey,
      source_species_local_id = ~ExternalCode
    )

  stored <- store_species_group_species(
    species = export_species,
    language = export_language,
    source_species = export_sourcespecies,
    source_species_species = export_sourcespeciesspecies,
    datafield = export_datafield,
    species_group = export_speciesgroup,
    species_group_species = export_speciesgroupspecies,
    conn = result.channel$con
  )


  speciesgroupspecies.hash <- export_species %>%
    select_(~nbn_key, species_local_id = ~local_id) %>%
    inner_join(
      stored$species %>%
        select_(Species = ~fingerprint, ~nbn_key),
      by = "nbn_key"
    ) %>%
    inner_join(
      export_speciesgroupspecies,
      by = "species_local_id"
    ) %>%
    inner_join(
      stored$species_group %>%
        select_(
          species_group_local_id = ~local_id,
          SpeciesGroup = ~fingerprint
        ),
      by = "species_group_local_id"
    ) %>%
    select_(~SpeciesGroup, ~Species) %>%
    arrange_(~SpeciesGroup, ~Species) %>%
    write_delim_git(
      file = "speciesgroupspecies.txt",
      connection = raw.connection
    )

  species.constraint <- source.species %>%
    select_(~NBNKey, ~ExternalCode, ~DatasourceID) %>%
    inner_join(
      stored$species %>%
        select_(~nbn_key, SpeciesID = ~fingerprint),
      by = c("NBNKey" = "nbn_key")
    ) %>%
    inner_join(
      species.constraint,
      by = "NBNKey"
    ) %>%
    inner_join(
      export_speciesgroupspecies,
      by = c("NBNKey" = "species_local_id")
    ) %>%
    inner_join(
      stored$species_group %>%
        select_(species_group_local_id = ~local_id, SpeciesGroupID = ~fingerprint),
      by = "species_group_local_id"
    ) %>%
    select_(~-NBNKey, ~-species_group_local_id)

  attr(species.constraint, "speciesgroupspecies.hash") <- speciesgroupspecies.hash
  return(species.constraint)
}
