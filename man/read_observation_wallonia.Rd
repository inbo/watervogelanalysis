% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/read_observation_wallonia.R
\name{read_observation_wallonia}
\alias{read_observation_wallonia}
\title{Read the Walloon observations from a species}
\usage{
read_observation_wallonia(species_id, first_year, latest_year, walloon_repo)
}
\arguments{
\item{species_id}{The id of the species}

\item{first_year}{first winter to import. defaults to 1992}

\item{latest_year}{latest winter to import.
Winter 2019 is defined as 2018-10-01 until 2019-03-31.
Defaults the winter prior to last firth of July.
2019-06-30 becomes 2018, 2019-07-01 becomes 2019.}

\item{walloon_repo}{a git_repository object to the Walloon source data}
}
\description{
All available imported data is used.
}
