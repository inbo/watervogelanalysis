#' Make an ODBC connection to the Flemish source data
#' @export
#' @param result_channel An open RODBC connection to the results database
#' @importFrom n2khelper odbc_connect
connect_flemish_source <- function(result_channel) {
  odbc_connect(
    data_source_name = "Datawarehouse watervogels Flanders",
    channel = result_channel
  )
}
