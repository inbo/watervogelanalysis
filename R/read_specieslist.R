#' Read the relevant species list
#' @inheritParams connect_flemish_source
#' @inheritParams prepare_dataset
#' @export
#' @importFrom dplyr %>% filter semi_join count group_by summarise
#' @importFrom DBI dbQuoteString dbGetQuery
#' @importFrom n2khelper get_nbn_key_multi
#' @importFrom git2rdata read_vc
#' @importFrom rlang .data
read_specieslist <- function(
  result_channel,
  flemish_channel,
  walloon_repo,
  import_date
){
  format(import_date, "%Y-%m-%d") %>%
    dbQuoteString(conn = flemish_channel) %>%
    sprintf(
      fmt = "WITH cte_survey AS (
        SELECT TaxonWVKey, COUNT(TaxonCount) AS N
        FROM FactAnalyseSetOccurrence
        WHERE SampleDate <= %s AND TaxonCount > 0
        GROUP BY TaxonWVKey
      )
      SELECT
        t.TaxonWVKey,
        t.scientificname AS ScientificName,
        c.n
      FROM cte_survey AS c
      INNER JOIN DimTaxonWV AS t ON c.TaxonWVKey = t.TaxonWVKey"
    ) %>%
    dbGetQuery(conn = flemish_channel) %>%
    get_nbn_key_multi(orders = "la", nbn_channel) -> species_flanders
  read_vc("visit", walloon_repo) %>%
    filter(.data$Date <= import_date) %>%
    semi_join(x = read_vc("data", walloon_repo), by = "ObservationID") %>%
    count(.data$NBNKey) -> species_wallonia
  bind_rows(species_wallonia, species_flanders) %>%
    group_by(.data$NBNKey) %>%
    summarise(n = sum(.data$n)) %>%
    filter(.data$n >= 100) -> relevant
  return(
    list(
      flanders = semi_join(species_flanders, relevant, by = "NBNKey"),
      wallonia = semi_join(species_wallonia, relevant, by = "NBNKey")
    )
  )
}
