#' Read the species for the raw datasource, save them to the results database
#' @return the species group constraint information
#' @inheritParams prepare_dataset
#' @inheritParams connect_flemish_source
#' @inheritParams prepare_dataset
#' @param first_date earliest records to take into account
#' @param latest_date until which date should we import the species list
#' @export
#' @importFrom assertthat assert_that is.string
#' @importFrom git2rdata write_vc
#' @importFrom dplyr %>% mutate select inner_join bind_rows transmute distinct
#' @importFrom n2kupdate store_species_group_species
#' @importFrom rlang .data
#' @importFrom tibble tibble
prepare_dataset_species <- function(raw_repo, result_channel, flemish_channel,
  walloon_repo, scheme_id, first_date, latest_date) {
  assert_that(is.string(scheme_id))

  species_list <- read_specieslist(
    result_channel = result_channel, flemish_channel = flemish_channel,
    walloon_repo = walloon_repo, first_date = first_date,
    latest_date = latest_date)

  species_list$flanders %>%
    transmute(
      ExternalCode = .data$TaxonWVKey,
      Description = .data$ScientificName,
      .data$NBNKey, DatasourceID = datasource_id_flanders(result_channel),
      TableName = "DimTaxonWV", ColumnName = "TaxonWVKey", Datatype = "integer",
      local_id = paste("f", .data$TaxonWVKey), .data$First
    ) %>%
    bind_rows(
      species_list$wallonia %>%
        transmute(
          ExternalCode = .data$euringcode, Description = .data$ScientificName,
          .data$NBNKey, DatasourceID = datasource_id_wallonia(result_channel),
          TableName = "species", ColumnName = "NBNKey", Datatype = "character",
          local_id = paste("w", .data$euringcode), .data$First
        )
    ) -> source_species

  source_species %>%
    distinct(
      .data$DatasourceID, .data$TableName, .data$ColumnName, .data$Datatype
    ) %>%
    transmute(
      local_id = .data$DatasourceID, datasource = .data$DatasourceID,
      table_name = .data$TableName, primary_key = .data$ColumnName,
      datafield_type = .data$Datatype
    ) -> export_datafield
  source_species %>%
    select("local_id", description = "Description",
           datafield_local_id = "DatasourceID", external_code = "ExternalCode"
    ) -> export_sourcespecies

  species_list$flanders %>%
    select("NBNKey", scientific_name = "ScientificName", "nl") -> species
  species %>%
    transmute(
      local_id = .data$NBNKey, .data$scientific_name,
      nbn_key = .data$NBNKey, .data$nl
    ) -> export_species

  # \u00E7 is the UTF-8 code for c with cedilla
  export_language <- tibble(
    code = c("nl", "en", "fr"),
    description = c("Nederlands", "English", "Fran\u00E7ais")
  )

  species %>%
    select(local_id = "NBNKey", description = "nl") %>%
    mutate(scheme = scheme_id) -> export_speciesgroup

  export_speciesgroupspecies <- species %>%
    transmute(species_local_id = .data$NBNKey,
              species_group_local_id = .data$NBNKey)

  export_sourcespeciesspecies <- source_species %>%
    select(
      species_local_id = "NBNKey", source_species_local_id = "local_id"
    )

  stored <- store_species_group_species(
    species = export_species, language = export_language,
    source_species = export_sourcespecies,
    source_species_species = export_sourcespeciesspecies,
    datafield = export_datafield, species_group = export_speciesgroup,
    species_group_species = export_speciesgroupspecies,
    conn = result_channel$con
  )

  export_species %>%
    select("nbn_key", species_local_id = "local_id") %>%
    inner_join(
      stored$species %>%
        select(species = "fingerprint", "nbn_key"),
      by = "nbn_key"
    ) %>%
    inner_join(export_speciesgroupspecies, by = "species_local_id") %>%
    inner_join(
      stored$species_group %>%
        select(
          species_group_local_id = "local_id", speciesgroup = "fingerprint"
        ),
      by = "species_group_local_id"
    ) %>%
    select("speciesgroup", "species") %>%
    write_vc(
      file = "speciesgroupspecies", sorting = c("speciesgroup", "species"),
      stage = TRUE, root = raw_repo
    ) -> speciesgroupspecies_hash

  source_species %>%
    select("NBNKey", "ExternalCode", "DatasourceID", "First") %>%
    inner_join(
      stored$species %>%
        select("nbn_key", species_id = "fingerprint"),
      by = c("NBNKey" = "nbn_key")
    ) %>%
    inner_join(
      export_speciesgroupspecies,
      by = c("NBNKey" = "species_local_id")
    ) %>%
    inner_join(
      stored$species_group %>%
        select(species_group_local_id = "local_id",
               species_group_id = "fingerprint"),
      by = "species_group_local_id"
    ) %>%
    select(-"NBNKey", -"species_group_local_id") -> final_list

  attr(final_list, "speciesgroupspecies_hash") <- speciesgroupspecies_hash
  return(final_list)
}
