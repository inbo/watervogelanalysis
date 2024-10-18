#' Read the species for the raw data source, save them to the results database
#' @return the species group constraint information
#' @inheritParams prepare_dataset
#' @param first_date earliest records to take into account
#' @param latest_date until which date should we import the species list
#' @export
#' @importFrom assertthat assert_that is.string
#' @importFrom git2rdata update_metadata write_vc
#' @importFrom dplyr bind_rows distinct inner_join mutate select transmute
#' @importFrom rlang .data
prepare_dataset_species <- function(
  raw_repo, flemish_channel, walloon_repo, first_date, latest_date
) {
  species <- read_specieslist(
    flemish_channel = flemish_channel, walloon_repo = walloon_repo,
    first_date = first_date, latest_date = latest_date, raw_repo = raw_repo
  )
  species |>
    select(-"first") |>
    write_vc(
      file = "species/species", root = raw_repo, stage = TRUE,
      sorting = "euring"
    )
  update_metadata(
    file = "species/species", root = raw_repo, stage = TRUE,
    name = "species", title = "List of species",
    field_description = c(
      euring = "The European bird ringing code.",
      scientific = "The scientific name of the species.",
      external_code_fl =
        "Identifier of the species in the Flemish data source.",
      datafield_fl = "The matching id in the datafield table.",
      external_code_wal =
        "Identifier of the species in the Walloon data source.",
      datafield_wal = "The matching id in the datafield table.",
      nl = "The Dutch name of the species.",
      fr = "The French name of the species."
    )
  )

  "SELECT
  tl.TaxonListCode AS external_code, tl.TaxonListBeschrijving AS description,
  t.euringcode AS euring
FROM FactTaxonList tl
INNER JOIN DimTaxonWV t ON t.Taxon_id = tl.TaxonWVKey
WHERE tl.TaxonListCode like 'WI-%'
ORDER BY tl.TaxonListCode, t.euringcode" |>
    dbGetQuery(conn = flemish_channel) -> speciesgroup_species
  speciesgroup_species |>
    distinct(.data$external_code, .data$description) |>
    mutate(
      datafield = get_datafield_id(
        table = "FactTaxonList", field = "TaxonListCode", root = raw_repo,
        datasource = "W0004_00_Waterbirds database", stage = TRUE
      )
    ) |>
    bind_rows(
      species |>
        transmute(
          external_code = as.character(.data$euring),
          description = .data$scientific,
          datafield = get_datafield_id(
            table = "species", field = "euring", root = raw_repo,
            datasource = "raw repo", stage = TRUE
          )
        )
    ) |>
    write_vc(
      file = "species/speciesgroup", root = raw_repo, stage = TRUE,
      sorting = "external_code"
    )
  update_metadata(
    file = "species/speciesgroup", root = raw_repo, stage = TRUE,
    name = "speciesgroup", title = "List of species groups",
    field_description = c(
      external_code =
        "Identifier of the species group in the original data source.",
      description = "Name of the species group.",
      datafield = "The matching id in the datafield table."
    )
  )

  speciesgroup_species |>
    transmute(
      speciesgroup = .data$external_code,
      species = as.integer(.data$euring)
    ) |>
    bind_rows(
      species |>
        transmute(
          speciesgroup = as.character(.data$euring), species = .data$euring
        )
    ) -> final_list
  write_vc(
    final_list, file = "species/speciesgroup_species", root = raw_repo,
    stage = TRUE, sorting = c("speciesgroup", "species")
  )
  update_metadata(
    file = "species/speciesgroup_species", root = raw_repo, stage = TRUE,
    name = "speciesgroup_species", title = "List of species in species groups",
    field_description = c(
      speciesgroup = "Identifier of the species group.",
      species = "Identifier of the species."
    )
  )

  return(species)
}
