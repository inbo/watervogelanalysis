#' Get the id of the data source for the Walloon source data
#' @inheritParams connect_flemish_source
#' @importFrom n2khelper odbc_get_id
#' @export
datasource_id_wallonia <- function(result.channel){
  odbc_get_id(
    table = "datasource",
    variable = "description",
    value = "Source data watervogels Wallonia",
    channel = result.channel
  )
}

#' Get the id of the data source for the Flemish source data
#' @inheritParams connect_flemish_source
#' @importFrom n2khelper odbc_get_id
#' @export
datasource_id_flanders <- function(result.channel){
  odbc_get_id(
    table = "datasource",
    variable = "description",
    value = "Source data watervogels Flanders",
    channel = result.channel
  )
}

#' Get the id of the data source for the raw data
#'
#' The raw data is the combination of the Flemish and Walloon source data
#' @inheritParams connect_flemish_source
#' @importFrom n2khelper odbc_get_id
#' @export
datasource_id_raw <- function(result.channel){
  odbc_get_id(
    table = "datasource",
    variable = "description",
    value = "Raw data watervogels",
    channel = result.channel
  )
}
