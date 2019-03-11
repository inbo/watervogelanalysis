#' Get the id of the data source for the Walloon source data
#' @inheritParams connect_flemish_source
#' @importFrom n2khelper odbc_get_id
#' @export
datasource_id_wallonia <- function(result_channel){
  odbc_get_id(table = "datasource", variable = "description",
              value = "Source data watervogels Wallonia",
              id_variable = "fingerprint", channel = result_channel)
}

#' Get the id of the data source for the Flemish source data
#' @inheritParams connect_flemish_source
#' @importFrom n2khelper odbc_get_id
#' @export
datasource_id_flanders <- function(result_channel){
  odbc_get_id(table = "datasource", variable = "description",
    value = "Datawarehouse watervogels Flanders", id_variable = "fingerprint",
    channel = result_channel)
}

#' Get the id of the data source for the raw data
#'
#' The raw data is the combination of the Flemish and Walloon source data
#' @inheritParams connect_flemish_source
#' @importFrom n2khelper odbc_get_id
#' @importFrom dplyr %>%
#' @export
datasource_id_raw <- function(result_channel){
  odbc_get_id(table = "datasource", variable = "description",
              value = "Raw data watervogel", id_variable = "fingerprint",
              channel = result_channel) %>%
    unname()
}


#' Get the id of the data source for the raw data
#'
#' The raw data is the combination of the Flemish and Walloon source data
#' @inheritParams connect_flemish_source
#' @param develop logical. Indicate whether the use the development database TRUE or the production database
#' @importFrom n2khelper odbc_get_id
#' @importFrom dplyr %>%
#' @export
datasource_id_result <- function(result_channel, develop = FALSE){
  if (develop) {
    odbc_get_id(table = "datasource", variable = "description",
                value = "n2kresult_develop", id_variable = "fingerprint",
                channel = result_channel) %>%
      unname()
  } else {
    odbc_get_id(table = "datasource", variable = "description",
                value = "n2kresult", id_variable = "fingerprint",
                channel = result_channel) %>%
      unname()
  }
}
