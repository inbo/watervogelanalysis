#' Get the id of the scheme
#' @inheritParams connect_flemish_source
#' @importFrom n2khelper odbc_get_id
#' @export
scheme_id <- function(result_channel) {
  odbc_get_id(table = "scheme", variable = "description", value = "Watervogels",
    id_variable = "fingerprint", channel = result_channel)
}
