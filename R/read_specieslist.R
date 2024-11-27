#' Read the relevant species list
#' @inheritParams prepare_dataset
#' @inheritParams prepare_dataset_species
#' @export
#' @importFrom dplyr count filter full_join group_by inner_join
#' mutate n pull select semi_join summarise ungroup
#' @importFrom DBI dbQuoteString dbGetQuery
#' @importFrom git2rdata read_vc
#' @importFrom rlang .data
#' @importFrom tidyr replace_na
read_specieslist <- function(
  raw_repo, flemish_channel, walloon_repo, first_date, latest_date
) {
  datafield <- get_datafield_id(
    table = "DimTaxonWV", field = "TaxonWVKey", root = raw_repo, stage = TRUE,
    datasource = "W0004_00_Waterbirds database"
  )
  sprintf(
    "WITH cte_survey AS (
      SELECT
        f.TaxonWVKey, COUNT(f.TaxonCount) AS n_fl, MIN(f.SampleDate) AS first
      FROM FactAnalyseSetOccurrence AS f
      INNER JOIN DimAnalyseSet AS a ON  f.analysesetkey = a.analysesetkey
      INNER JOIN DimSample AS s ON f.samplekey = s.samplekey
      WHERE %s <= f.SampleDate AND f.SampleDate <= %s AND f.TaxonCount > 0 AND
            a.AnalysesetCode LIKE 'MIDMA%%' AND s.CoverageCode IN ('V', 'O')
      GROUP BY f.TaxonWVKey
    )

    SELECT
      t.TaxonWVKey AS external_code_fl, CAST(t.euringcode AS int) AS euring,
      t.scientificname AS scientific_fl, t.commonname AS nl, c.n_fl, c.first
    FROM cte_survey AS c
    INNER JOIN DimTaxonWV AS t ON c.TaxonWVKey = t.TaxonWVKey",
    format(first_date, "%Y-%m-%d") |>
      dbQuoteString(conn = flemish_channel),
    format(latest_date, "%Y-%m-%d") |>
      dbQuoteString(conn = flemish_channel)
  ) |>
    dbGetQuery(conn = flemish_channel) |>
    mutate(
      first_fl = round_date(.data$first, unit = "year") |>
        year(),
      datafield_fl = datafield
    ) -> species_flanders

  if (any(is.na(species_flanders$euring))) {
    species_flanders |>
      filter(is.na(.data$euring)) |>
      summarise(problem = paste(.data$scientific_name, collapse = ", ")) |>
      sprintf(fmt = "Species in Flemish dataset without euringcode: %s") |>
      warning(call. = FALSE)
    species_flanders |>
      filter(!is.na(.data$euring)) -> species_flanders
  }

  datafield <- get_datafield_id(
    table = "DimTaxonWV", field = "TaxonWVKey", root = raw_repo, stage = TRUE,
    datasource = "Wallonia waterbirds repo"
  )
  read_vc("visit", walloon_repo) |>
    filter(first_date <= .data$date, .data$date <= latest_date) |>
    inner_join(x = read_vc("data", walloon_repo), by = c("visit" = "hash")) |>
    group_by(.data$species) |>
    summarise(
      first_wal = min(.data$date) |>
        round_date(unit = "year") |>
        year(),
      n_wal = n(), .groups = "drop"
    ) |>
    inner_join(
      x = read_vc("species", walloon_repo), by = c("scientific" = "species")
    ) |>
    filter(!is.na(.data$euring)) |>
    transmute(
      .data$euring, external_code_wal = .data$euring,
      scientific_wal = .data$scientific, fr = .data$french_name,
      .data$first_wal, .data$n_wal, datafield_wal = datafield
    ) -> species_wallonia
  species_flanders |>
    full_join(species_wallonia, by = "euring") |>
    filter(replace_na(.data$n_fl, 0) + replace_na(.data$n_wal, 0) >= 100) |>
    transmute(
      .data$euring, scientific = ifelse(
        is.na(.data$scientific_fl), .data$scientific_wal, .data$scientific_fl
      ),
      .data$external_code_fl, .data$datafield_fl, .data$external_code_wal,
      .data$datafield_wal, .data$nl, .data$fr,
      first = round_date(.data$first, unit = "year") |>
        year(),
      first = ifelse(is.na(.data$first), .data$first_wal, .data$first)
    )
}
