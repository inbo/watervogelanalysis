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
                             latest_date){
  format(latest_date, "%Y-%m-%d") %>%
    dbQuoteString(conn = flemish_channel) %>%
    sprintf(
      fmt = "WITH cte_survey AS (
        SELECT TaxonWVKey, RecommendedTaxonTLI_Key AS NBNKey,
               COUNT(TaxonCount) AS N
        FROM FactAnalyseSetOccurrence
        WHERE SampleDate <= %s AND TaxonCount > 0
        GROUP BY TaxonWVKey, RecommendedTaxonTLI_Key
      )

      SELECT
        CASE WHEN c.NBNKey = '-               ' THEN NULL
             ELSE c.NBNKey END AS NBNKey,
        t.TaxonWVKey, CAST(t.euringcode AS int) AS euringcode,
        t.scientificname AS ScientificName, t.commonname AS nl, c.n
      FROM cte_survey AS c
      INNER JOIN DimTaxonWV AS t ON c.TaxonWVKey = t.TaxonWVKey"
    ) %>%
    dbGetQuery(conn = flemish_channel) %>%
    group_by(.data$euringcode, .data$TaxonWVKey, .data$ScientificName,
             .data$nl) %>%
    summarise(NBNKey = na.omit(.data$NBNKey), n = sum(.data$n)) %>%
    ungroup() -> species_flanders
  if (any(is.na(species_flanders$NBNKey))) {
    species_flanders %>%
      filter(is.na(.data$NBNKey)) %>%
      anti_join(species_flanders, by = "euringcode")
  }
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
    semi_join(
      x = read_vc("data", walloon_repo),
      by = "OriginalObservationID"
    ) %>%
    count(.data$euringcode) %>%
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
