library(watervogelanalysis)
prepare_analysis(
  analysis.path = "~/analysis", 
  raw.connection = n2khelper::git_connection(
    repo.path = "~/rawdata",
    local.path = "watervogel",
    key = "~/.ssh/id_rsa_n2kreadonly"
  )
)
