library(watervogelanalysis)
prepare_analysis(
  analysis.path = "~/analysis",  #nolint
  raw.connection = n2khelper::git_connection(
    repo.path = "~/rawdata", #nolint
    local.path = "watervogel",
    key = "~/.ssh/id_rsa_n2kreadonly" #nolint
  )
)
