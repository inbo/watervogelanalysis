% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/prepare_analysis_smoother.R
\name{prepare_analysis_smoother}
\alias{prepare_analysis_smoother}
\title{Fit a smoother to an aggregated set}
\usage{
prepare_analysis_smoother(
  aggregation,
  analysis_path,
  month = TRUE,
  verbose = TRUE
)
}
\arguments{
\item{aggregation}{an \code{n2kAggregate} object}

\item{analysis_path}{Path to store the analysis files.
Must be either an existing local file path or an object of the \code{s3_bucket}
class.}

\item{month}{Model the month effect.
Defaults to \code{TRUE}.}

\item{verbose}{Display a progress bar when TRUE (default)}
}
\description{
Fit a smoother to an aggregated set
}
