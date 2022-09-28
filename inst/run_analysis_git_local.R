library(fs)
library(watervogelanalysis)
prepare_analysis(
  analysis.path = path("~", "analysis"),
  raw.connection = n2khelper::git_connection(
    repo.path = "~/n2k/ssh/rawdata", #nolint
    local.path = "watervogel",
    key = "~/.ssh/id_rsa_n2kreadonly", #nolint
    commit.user = "watervogelanalysis",
    commit.email = "bmk@inbo.be"
  )
)
