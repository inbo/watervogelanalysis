---
title: "Workflow for the statistical analysis"
---

The goal of the analysis is to estimate the trend of the total wintering population of waterbirds for several regions.
In an ideal world, we would have a complete dataset with the number of birds for every site, winter and month.
Note that we define a winter as the period from October to March and we refer to a winter by the year of January 1st.
Winter 2001 goes from October 2000 to March 2001.
In reality, the dataset is incomplete.
Summing the observed birds would underestimate the total wintering population.
Therefore we have to impute the missing observations.
We use a model to predict the number of birds at a given site, winter and month.
Then we use this model to generate plausible values for the missing observations.

In order to get stable models, we only define the imputation models at a higher level.
We define only three imputation models per species: one at the Belgian level, a second for the Flemish region and a third for the Brussels and Walloon region.
The analyses bases on the monthly population totals for these region use the matching imputation model.
The analyses for the smaller regions in Flanders reuse the imputations from the model for the Flemish region.
We don't analyse subsets for the Brussels and Walloon region.
Note that we still we analyse the data at the Belgian level in this case.
The results will be different from the Flemish data, because at the Belgian level we only take into account the data from November to February, while in Flanders we take into account the data from October to March.

Before running the imputation model, we have to [select](selection.html) the relevant data in order to get stable imputation models.
The rules split the data into two datasets: the relevant dataset and the rare dataset.
The relevant dataset contains the data that is used in the [imputation model](imputation.html).
It also defines which missing observations are imputed.
In case there are too few relevant data of a species for a given imputation region, we ignore that species within this region.
Suppose that a species is only observed in Flanders.
Then we will not analyse the species in the Brussels and Walloon region.

The rare dataset contains the data that is not used in the imputation model.
Hence we don't impute the missing observations in the rare dataset.
However we do take the rare dataset into account when we aggregate the results of the imputation model.

The monthly population total is the sum of the relevant, rare and imputed counts for a specific region, winter and month.
This monthly population total is the input for the [analysis](../analysis.html) of the trends per region.
