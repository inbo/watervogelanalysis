#' Make an ODBC connection to the Flemish source data
#' @export
#' @param result.channel An open RODBC connection to the results database
#' @importFrom n2khelper odbc_connect
connect_flemish_source <- function(result.channel){
  odbc_connect(
    data.source.name = "Datawarehouse watervogels Flanders",
    channel = result.channel
  )
}

#' Make an ODBC connection to the NBN database
#' @export
#' @inheritParams connect_flemish_source
#' @importFrom n2khelper odbc_connect
connect_nbn <- function(result.channel){
  odbc_connect(
    data.source.name = "NBN data",
    channel = result.channel
  )
}

#' Make a git connection to the Walloon source data
#' @export
#' @inheritParams connect_flemish_source
#' @inheritParams n2khelper::git_connection
#' @importFrom n2khelper git_connect
connect_walloon_source <- function(
  result.channel,
  username,
  password,
  commit.user = "watervogelanalysis",
  commit.email = "bmk@inbo.be"
){
  git_connect(
    data.source.name = "Source data watervogels Wallonia",
    channel = result.channel,
    username = username,
    password = password,
    commit.user = commit.user,
    commit.email = commit.email
  )
}

#' Make a git connection to the attribute
#' @export
#' @inheritParams connect_flemish_source
#' @inheritParams n2khelper::git_connection
#' @importFrom n2khelper git_connect
connect_attribute <- function(
  result.channel,
  username,
  password,
  commit.user = "watervogelanalysis",
  commit.email = "bmk@inbo.be"
){
  git_connect(
    data.source.name = "Attributes watervogels",
    channel = result.channel,
    username = username,
    password = password,
    commit.user = commit.user,
    commit.email = commit.email
  )
}

#' Make a git connection to the raw data
#' @export
#' @inheritParams connect_flemish_source
#' @inheritParams n2khelper::git_connection
#' @importFrom n2khelper git_connect
connect_raw <- function(
  result.channel,
  username,
  password,
  commit.user = "watervogelanalysis",
  commit.email = "bmk@inbo.be"
){
  git_connect(
    data.source.name = "Raw data watervogel",
    channel = result.channel,
    username = username,
    password = password,
    commit.user = commit.user,
    commit.email = commit.email
  )
}
