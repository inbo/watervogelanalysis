#' Read the relevant species list
#' @inheritParams connect_flemish_source
#' @inheritParams prepare_dataset
#' @inheritParams prepare_dataset_species
#' @export
#' @importFrom dplyr %>% filter semi_join count group_by summarise full_join anti_join inner_join mutate pull select ungroup
#' @importFrom DBI dbQuoteString dbGetQuery
#' @importFrom git2rdata read_vc
#' @importFrom rlang .data
#' @importFrom stats na.omit
read_specieslist <- function(result_channel, flemish_channel, walloon_repo,
                             first_date, latest_date){
  sprintf(
    "WITH cte_survey AS (
      SELECT f.TaxonWVKey, f.RecommendedTaxonTLI_Key AS NBNKey,
             COUNT(f.TaxonCount) AS n, MIN(f.SampleDate) AS First
      FROM FactAnalyseSetOccurrence AS f
      INNER JOIN DimAnalyseSet AS a ON  f.analysesetkey = a.analysesetkey
      INNER JOIN DimSample AS s ON f.samplekey = s.samplekey
      WHERE %s <= f.SampleDate AND f.SampleDate <= %s AND f.TaxonCount > 0 AND
            a.AnalysesetCode LIKE 'MIDMA%%' AND s.CoverageCode IN ('V', 'O')
      GROUP BY f.TaxonWVKey, f.RecommendedTaxonTLI_Key
    )

    SELECT
      CASE WHEN c.NBNKey = '-               ' THEN NULL
           ELSE c.NBNKey END AS NBNKey,
      t.TaxonWVKey, CAST(t.euringcode AS int) AS euringcode,
      t.scientificname AS ScientificName, t.commonname AS nl, c.n, c.First
    FROM cte_survey AS c
    INNER JOIN DimTaxonWV AS t ON c.TaxonWVKey = t.TaxonWVKey",
    format(first_date, "%Y-%m-%d") %>%
      dbQuoteString(conn = flemish_channel),
    format(latest_date, "%Y-%m-%d") %>%
      dbQuoteString(conn = flemish_channel)
  ) %>%
    dbGetQuery(conn = flemish_channel) %>%
    mutate(First = round_date(.data$First, unit = "year") %>%
             year()) %>%
    group_by(.data$euringcode, .data$TaxonWVKey, .data$ScientificName,
             .data$nl) %>%
    summarise(NBNKey = sort(.data$NBNKey)[1], n = sum(.data$n),
              First = min(.data$First)) %>%
    ungroup() -> species_flanders

  if (any(is.na(species_flanders$NBNKey))) {
    species_flanders %>%
      filter(is.na(.data$NBNKey)) %>%
      summarise(problem = paste(.data$ScientificName, collapse = ", ")) %>%
      sprintf(fmt = "Species in Flemish dataset without NBN key: %s") %>%
      warning(call. = FALSE)
    species_flanders %>%
      filter(!is.na(.data$NBNKey)) -> species_flanders
  }
  if (any(is.na(species_flanders$euringcode))) {
    species_flanders %>%
      filter(is.na(.data$euringcode)) %>%
      summarise(problem = paste(.data$ScientificName, collapse = ", ")) %>%
      sprintf(fmt = "Species in Flemish dataset without euringcode: %s") %>%
      warning(call. = FALSE)
    species_flanders %>%
      filter(!is.na(.data$euringcode)) -> species_flanders
  }

  read_vc("visit", walloon_repo) %>%
    filter(.data$Date <= latest_date) %>%
    mutate(Winter = round_date(.data$Date, unit = "year") %>%
             year()) %>%
    inner_join(read_vc("data", walloon_repo), by = "OriginalObservationID") %>%
    inner_join(species_flanders, by = "euringcode") %>%
    filter(.data$First <= .data$Winter) %>%
    count(.data$euringcode, .data$First) %>%
    inner_join(read_vc("species", walloon_repo), by = "euringcode") ->
    species_wallonia
  species_wallonia %>%
    select("euringcode", nw = "n") %>%
    full_join(species_flanders, by = "euringcode") %>%
    mutate(n = ifelse(is.na(.data$n), 0, .data$n) +
             ifelse(is.na(.data$nw), 0, .data$nw)) %>%
    filter(.data$n >= 100) %>%
    select("euringcode", "NBNKey", "nl") -> relevant
  if (any(is.na(relevant$NBNKey))) {
    relevant %>%
      filter(is.na(relevant$NBNKey)) %>%
      semi_join(x = species_wallonia, by = "euringcode") %>%
      dplyr::pull("ScientificName") %>%
      paste(collapse = ", ") %>%
      sprintf(fmt = "Walloon species without NBN key: %s") %>%
      warning(call. = FALSE)
    relevant %>%
      filter(!is.na(relevant$NBNKey)) -> relevant
  }
  return(
    list(
      flanders = semi_join(species_flanders, relevant, by = "euringcode"),
      wallonia = inner_join(species_wallonia, relevant, by = "euringcode")
    )
  )
}
