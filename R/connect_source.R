#' Make an ODBC connection to the Flemish source data
#' @export
#' @param result.channel An open RODBC connection to the results database
#' @importFrom n2khelper odbc_connect
connect_flemish_source <- function(result.channel){
  odbc_connect(
    data.source.name = "Source data watervogels Flanders", channel = result.channel
  )
}

#' Make a git connection to the Walloon source data
#' @export
#' @inheritParams connect_flemish_source
#' @inheritParams n2khelper::git_connection
#' @importFrom n2khelper git_connect
connect_walloon_source <- function(result.channel, username, password){
  git_connect(
    data.source.name = "Source data watervogels Wallonia", 
    channel = result.channel,
    username = username,
    password = password
  )
}