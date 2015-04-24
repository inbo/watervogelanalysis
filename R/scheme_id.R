#' Get the id of the scheme
#' @inheritParams connect_flemish_source
#' @importFrom n2khelper odbc_get_id
#' @export
scheme_id <- function(result.channel){
  odbc_get_id(
    table = "Scheme",
    variable = "Description",
    value = "Watervogels",
    channel = result.channel
  )
}
