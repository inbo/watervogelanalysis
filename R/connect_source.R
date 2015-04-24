#' Make an ODBC connection to the source data
#' @export
#' @param result.channel An open RODBC connection to the results database
#' @importFrom n2khelper odbc_connect
connect_source <- function(result.channel){
  odbc_connect(
    data.source.name = "Source data watervogels Flanders", channel = result.channel
  )
}
