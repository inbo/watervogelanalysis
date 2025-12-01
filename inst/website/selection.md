---
title: "Selection of the raw data for the imputation model"
---

In order to get stable models, we have to restrict the data to only those sites and periods which are relevant for the species.
Keeping so-called structural zero's (e.g. a site in which the species is always absent) would flatten the overall trend.
The rules mentioned below might feel somewhat strange a first.
Note that they are the end point of a process of trial and error.
We started by a minimal set of rules and added more rules when we noticed that some models were not stable.
Whenever a model failed to run, we inspected the data to find out what would be the problem.
Then we added a rule to exclude such cases.
Because we apply the same set of rules to all species - region combination, this implies that we might need to refit stable model when the new set of rules resulted in a changed dataset.

First we select the relevant time period.
We calculate the oldest and most recent winter for which the species was observed at least 5 times during that winter in the imputation region.
We limit the time period of the analysis to this range.
This rule ignores the time period before the arrival of new species and the time period after the disappearance of species.
If the remaining time period is less than 5 winters, we will not analyse the species.

Next we further refine the dataset.
We keep the data in the dataset when it matches with the rules below.

1. Only keep sites where we observe the species during at least 5 different winters.
1. Calculate the number of sites where the species was observed for every month and winter.
  Calculate the median of this number for every month.
  Keep the months where the median is at least 1.
  This implies that within the relevant time period, we keep the months for which in at least half of the winter the species was observed in at least one site.
  Thus excluding months in which the species is rarely observed.
1. Keep months where the average counts are at least 5% of the month with the largest average counts.
1. When the data still spans multiple months, remove sites only observed during a single month.
  Then count the number of winters per observations for every combination of site and month.
  Keep only sites that have a least 2 months observed during at least 3 different winters.
1. Some locations exhibit strong changes over time.
  We calculate the geometric mean per year for every site and select the five largest values in decreasing order.
  We retain the site when the ratio between first and last value is less than 10.
  This excludes sites with extreme changes from the imputation model, but not from the totals.
1. Calculate the fraction of surveyed month per site and per winter.
  Note the previous rules determine the number of relevant months.
  Keep only sites in which at least 2 winter have a survey fraction of at least 50%.
  
The resulting dataset needs to span at least 6 sites.
If the dataset is too small, we will not analyse the species in this region.

As mentioned before we use the relevant data to create an impute model.
And we use that imputation model to impute the missing data.
In order to avoid strong extrapolations, we restrict the missing data of a site to within 5 years of the nearest observed data.
Suppose a site was surveyed from 2005 to 2010 and from 2023 to until now.
Then we will impute missing data in the period 2000 to 2015 and 2018 until now.
We don't impute prior to 2000 because it is more than 5 year from 2005.
We don't impute 2016 and 2017 because they are more than 5 year from 2010 or 2023.
