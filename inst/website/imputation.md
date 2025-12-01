---
title: "Imputing missing observations"
---

The final analysis is on the trend of the total wintering population.
A simple summation of all observed birds underestimates this total population when some of the data is missing.
To solve this problem, we impute the missing counts.
First we fit a model to the available observations.
This model allows us to predict the number of birds at given site, winter and month.
Then we use this model to generate plausible values for the missing observations.
The augmented data (observations + imputed values) has counts for every combination of site, winter and month.
We aggregate this augmented data into a monthly population total $T_{wm}$ by summation of the counts of every site $s$ for each combination of winter $w$ and month $m$ (@eq-total).
All further analyses use these monthly population totals.

$$T_{wm} = \sum^S_{s = 1}Y_{swm}$${#eq-total}

## Imputation model

The model estimates the number of birds $Y_{swm}$ at site $s$, winter $w$ and month $m$ (@eq-hurdle).
We assume that the number of birds follows a hurdle model.
The hurdle model is a two-part model.
The first part models the presence of birds.
The second part models the number of birds given that they are present.
The presence of birds follows a Bernoulli distribution with probability $\pi_{swm}$.
The number of birds given that they are present follows a zero-truncated negative binomial distribution with mean $\mu_{swm}$ and overdispersion parameter $n$.

$$ProbY_{swm} \sim \mathcal{Binom}(\pi_{swm})\times \mathcal{TruncNegBin}(\mu_{swm} n)$${#eq-hurdle}

### Presence of birds

The probability $\pi_{swm}$ of the binomial distribution is linked to the linear predictor by @eq-link-presence

$$\log\left(\frac{\pi_{swm}}{1 - \pi_{swm}}\right) = \zeta_{swm}$${#eq-link-presence}

The linear predictor $\zeta_{swm}$ depends on five terms (@eq-zeta):

1. $\alpha_0$ is the global average at the reference month.
1. $\alpha_m$ is the global effect of a given month $m$ relative to the reference month.
   There are $m - 1$ parameters, since one month is used as the reference.
   We only use this term when the dataset consists of multiple months.
   This term describes the average seasonal pattern.
1. $a_w$ is the global trend over winters.
   We use a first order random walk in order to take into account the temporal autocorrelation.
   A first order random walk is defined by the difference in consecutive winters.
   $\Delta_{a_w} = a_{w + 1} - a_{w}$
   The difference follows a zero mean Gaussian distribution with variance $\sigma^2_{wp}$.
1. $a_s$ is the relative effect of site $s$ (@eq-presence-site).
   The site effect follows a zero mean Gaussian distribution with variance $\sigma^2_{sp}$.
1. $a_{mw}$ is the difference in seasonal pattern in a given winter.
   It models deviations from the global seasonal pattern in a given year $w$.
   It follows a zero mean Gaussian distribution with variance $\sigma^2_{mwp}$.
1. $a_{ws}$ is the difference in trend per location $s$ (@eq-presence-winter).
   We use a second order random walk in order to take into account the temporal autocorrelation.
   The second order random walk is defined by the difference of consecutive first order differences.
   $\Delta(\Delta_{a_ws}) = \Delta_{a_{ws}} - \Delta_{a_{(w-1)s}} = (a_{(w + 1)s} - a_{ws}) - (a_{ws} - a_{(w - 1)s}) = a_{(w + 1)s} - 2 a_{ws} + a_{(w - 1)s}$
   The second order difference follows a zero mean Gaussian distribution with variance $\sigma^2_{wp}$.

$$\zeta{swm} = \alpha_0 + \alpha_m + a_s + a_w + a_{mw} + a_{ws}$${#eq-zeta}

$$a_s \sim \mathcal{N}(0, \sigma^2_{sp})$${#eq-presence-site}

$$a_{(w + 1)s} - 2 a_{ws} + a_{(w - 1)s} = \Delta(\Delta_{a_{ws}}) \sim \mathcal{N}(0, \sigma^2_{wsp})$${#eq-presence-winter}

### Number of birds given that they are present

The mean $\mu_{swm}$ is the link to the linear predictor by @eq-link-count

$$\log(\mu_{swm}) = \eta_{swm}$${#eq-link-count}

The linear predictor $\eta_{swm}$ depends on six terms (@eq-eta):

1. $\beta_0$ is the global average at the reference month.
1. $\beta_m$ is the global effect of a given month $m$ relative to the reference month.
There are $m - 1$ parameters, since one month is used as the reference.
We only use this term when the data consists of multiple months.
1. $b_s$ is the relative effect of site $s$ (@eq-count-site).
It follows a zero mean Gaussian distribution with variance $\sigma^2_{s}$.
1. $b_w$ is the relative effect of winter $w$ (@eq-count-winter). 
We use a first order random walk in order to take into account the temporal autocorrelation.
The difference between two consecutive year follows a zero mean Gaussian distribution with variance $\sigma^2_w$.
1. $b_{wm}$ is the relative effect of the combination of winter $w$ and month $m$ (@eq-count-winter-site). 
This effect models deviations from the global seasonal pattern in a given year $w$.
It follows a zero mean Gaussian distribution with variance $\sigma^2_{wm}$.

$$\eta_{swm} = \beta_0 + \beta_m + b_s + b_w + b_{wm}$${#eq-eta}

$$b_{s} \sim \mathcal{N}(0, \sigma^2_{s})$${#eq-count-site}

$$b_w - b_{w - 1} = \Delta_{b_w} \sim \mathcal{N}(0, \sigma^2_w)$${#eq-count-winter}

$$b_{wm} \sim \mathcal{N}(0, \sigma^2_{wm})$${#eq-count-winter-site}

### Fitting the models

We fit the models in R [@R] using INLA [@INLA].
As INLA using a Bayesian approach, we need to specify priors for the (hyper-)parameters.

- $\alpha_0$, $\alpha_m$, $\beta_0$ and $\beta_m$ get a Gaussian prior $\mathcal{N}(0, 1000)$.
- $\sigma^2_{sp}$ gets a penalised complexity prior so that $Prob(\sigma_{sp} > 1) = 0.01$.
- $\sigma^2_{wp}$ gets a penalised complexity prior so that $Prob(\sigma_{wp} > 1) = 0.01$.
- $\sigma^2_s$ gets a penalised complexity prior so that $Prob(\sigma_s > 1) = 0.01$.
- $\sigma^2_w$ gets a penalised complexity prior so that $Prob(\sigma_w > 2) = 0.01$.
- $\sigma^2_{wm}$ gets a penalised complexity prior so that $Prob(\sigma_{wm} > 1) = 0.01$.
- $n$ gets a penalised complexity prior so that $n \sim \Gamma(1/7, 1/7)$.

## Multiple imputation

We use the imputation model to generate plausible values for the missing observations.
Although the predictions of the imputation model are the most plausible values, we cannot use them as this would reduce the variability in the data.
Then more missing values, would lead to less variability and thus smaller credible intervals compared to a complete data set.
This is of course non sense.

Instead we impute the missing data with a random value based on the prediction distribution of the imputation model.
This takes both the natural variability and the model uncertainty into account.
Then the variability is the data will be larger than the variability in the observed data.
The increase of the variability depends on a) the number of missing observations and b) the model uncertainty of the imputation model.
More missing data or a poorer model fit (more uncertainty) will lead to more variability and thus wider credible intervals.

With the missing values replaced by imputation set $l$, the dataset is complete. 
So we can apply the [analysis](../analysis.html) that we wanted to do in the first place.
The analysis results in a set of coefficients ${\gamma_a}_l$ and their standard error ${\sigma_a}_l$. 
Of course, this set will depend on the imputed values of the imputation set $l$. 
Another imputation set has different imputed values and will hence lead to different coefficients.

Therefore the imputation, aggregation and analysis is repeated for $L = 100$ different imputation sets, resulting in $L$ sets of coefficients and their standard errors. 
They are aggregated by the formulas below [@Rubin1987]. 
The coefficient will be the average of the coefficient in all imputation sets (@eq-mean). 
The standard error of a coefficient is the square root of a sum of two parts (@eq-sd). 
The first part is the average of the squared standard error in all imputation sets $\frac{\sum_{l = 1}^L {{\sigma_a^2}_l}}{L}$.
The second part is the variance of the coefficient among the imputation sets $\frac{\sum_{l = 1}^L({\gamma_a}_l - \bar{\gamma}_a) ^ 2}{L - 1}$, multiplied by a correction factor $1 + \frac{1}{L}$. 

$$\bar{\gamma}_a = \frac{\sum_{l = 1}^L{\gamma_a}_l}{L}$${#eq-mean}
$$\bar{\sigma}_a = \sqrt{\frac{\sum_{l = 1}^L {{\sigma_a^2}_l}}{L} + (1 + \frac{1}{L}) 
\frac{\sum_{l = 1}^L({\gamma_a}_l - \bar{\gamma}_a) ^ 2}{L - 1}}$${#eq-sd}
