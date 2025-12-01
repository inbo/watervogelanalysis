---
title: "Analyses on imputed monthly population totals"
---

## Non-linear seasonal population average

The model estimates the monthly population total of birds $T_{wm}$ at winter $w$ and month $m$.
We assume that the monthly population total follows a negative binomial distribution with mean $\mu_{wm}$ and overdispersion parameter $n$ (@eq-nb).

$$T_{wm} \sim NB(\mu_{wm}, n)$${#eq-nb}
The mean $\mu_{wm}$ is the link to the linear predictor by @eq-link

$$\log(\mu_{wm}) = \xi_{wm}$${#eq-link}

The linear predictor $\xi_{swm}$ depends on two of the three terms below:

1. $\gamma_0$ is the global average, used when the data consists of a single month (@eq-single).
1. $\gamma_m$ is the average of month $m$, used when the data consists of multiple months (@eq-multiple).
1. $c_w$ is the relative effect of winter $w$, used in both [@eq-single] and [@eq-multiple]. 
We use a first order random walk in order to take into account the temporal autocorrelation (@eq-rw1).
The difference between two consecutive year follows a zero mean Gaussian distribution with variance $\sigma^2_c$.

$$\xi_{swm} = \gamma_0 + c_w$${#eq-single} or $$\xi_{swm} = \gamma_m + c_w$${#eq-multiple}

$$c_w - c_{w - 1} = \Delta_{c_w} \sim \mathcal{N}(0, \sigma^2_c)$${#eq-rw1}

The seasonal population average in winter $w$ ($S_w$) is the combination of the average of the month effect ($\sum^n_{m=1}\frac{\beta_m}{n}$) and the effect of winter $w$ ($c_w$) (@eq-season).
This is conceptually similar to the geometric mean of the monthly population totals $T_{wm}$, corrected for the global seasonal pattern.
When the seasonal pattern is stable over the time series, the credible interval on the seasonal population will be more narrow than that of the geometric mean.
This analysis is always run on the entire time series.

$$S_w = e^{\sum^n_{m=1}\frac{\gamma_m}{n} + c_w}$${#eq-season}

### Indices

The index $I_{ir}$ for winter $i$ using winter $r$ as reference is the seasonal population average for winter $i$ ($S_i$) divided by the seasonal population average for winter $r$ ($S_r$) (@eq-index-basis).

$$I_{ir} = \frac{S_i}{S_r}$${#eq-index-basis}
We can substitute $S_w$ with the formula we gave above, resulting in @eq-index-substitute.

$$I_{ir} = \frac{e^{\sum^n_{m=1}\frac{\gamma_m}{n} + c_{w_i}}}{e^{\sum^n_{m=1}\frac{\gamma_m}{n} + c_{w_r}}}$${#eq-index-substitute}

which we can simplify to @eq-index

$$I_{ir} = \frac{e ^{c_{w_i}}}{e^{c_{w_r}}}  = e ^{c_{w_i} - c_{w_r}}$${#eq-index}

Note that the index only depends on the difference in winter effects.
The index at reference winter is by default exact $e^0 = 1$ and pointless to calculate or display.

### Moving average

The moving average $M_w$ is the average of the monthly population totals $T_{wm}$ over a moving window of $n$ years starting in winter $w$ (@eq-ma).
We calculate the moving average for all periods of 5 and 10 years long.
Calculating the moving average on the winter effect $c_w$ has the benefit that we exclude the average seasonal pattern.

$$\log M_w = \sum^{n-1}_{i = 0}\frac{c_{w+i}}{n}$${#eq-ma}

### Ten-winter difference

The ten-winter difference $D_{ir}$ is the relative change in the seasonal population average between the two non-overlapping ten-winter periods (@eq-ten).
Assume $r$ is the first winter of the reference period and $i$ is the first winter of the second period.

$$\log D_{ir} = \log \frac{M_i}{M_r} = \log\sum^{9}_{j=0}\frac{c_{i + j}}{10} - \sum^{9}_{j=0}\frac{c_{r + j}}{10}$${#eq-ten}

### Linear change in a moving window

We calculate the linear change in the moving window as a linear combination of the winter effect $c_w$.
We do this analysis for all windows of size 10 and 12 winters and for the total length of the time series.
For readability, we explain the algorithm using a 4 winter window as example.

1. Give every winter a weight $z$ equal to its number in the window. ($z_1=1, z_2=2, z_3=3, z_4=4$).
1. Centre the weights by subtracting the mean of the weights. ($z_1 =-1.5, z_2=-0.5, z_3=0.5, z_4=1.5$).
1. Divide the weights by the sum of their squares $\frac{z_i}{\sum^4_{i = 1}z_i^2}$. ($z_1 =-0.3, z_2=-0.1, z_3=0.1, z_4=0.3$)
1. Multiply the winter effect by the weight and sum the results. ($-0.3c_1 - 0.1c_2 + 0.1c_3 + 0.3c_4$).

## Smoothed trend

The smoothed trend updates @eq-single to @eq-single2 and @eq-multiple to @eq-multiple2.
Instead of using a first order random walk for the winter effect (@eq-rw1), we use a second order random walk (@eq-rw2).

$$\xi_{swm} = \gamma_0 + d_w$${#eq-single2} or $$\xi_{swm} = \gamma_m + d_w$${#eq-multiple2}


$$\Delta\Delta_{d_w} = \Delta_{d_{w+1}} - \Delta_{d_w} = d_{w+1} - 2 d_w + d_{w - 1} \sim \mathcal{N}(0, \sigma^2_{d})$${#eq-rw2}

## Average winter maximum

The winter maximum $X_w$ is the largest monthly population total $T_{wm}$ of winter $w$ (@eq-winter-maximum).
Note that the winter maximum may occur to a different month depending on the winter.
We model the winter maxima of the latest ten winters.
We assume that the winter maximum follows a negative binomial distribution with mean $\mu_w$ and overdispersion parameter $n$ (@eq-winter-maximum-nb).

$$X_w = \max(T_{wm})$${#eq-winter-maximum}

$$X_w \sim NB(\mu_w, n)$${#eq-winter-maximum-nb}
The mean $\mu_w$ is the link to the linear predictor by @eq-link-winter-maximum

$$\log(\mu_w) = \eta_w$${#eq-link-winter-maximum}

The linear predictor $\eta_w$ depends on two terms (@eq-winter-maximum-eta):

1. $\beta_0$ is the global average at the reference period.
We use the last five winters in the data as the reference period.
1. $\beta_1$ is the change in average winter maximum between the last two five-winter periods.
$P = 0$ when $w$ is within the last period of five winters.
$P = -1$ when $w$ is within the last but one period of five winters.

$$\eta_w = \beta_0 + \beta_1P$${#eq-winter-maximum-eta}

$e^{\beta_0}$ estimates the average winter maximum over the last five winters.
It is equivalent to the geometric mean of the winter maxima.
$e^{\beta_1}$ estimates the change in average winter maximum between the last two five-winter periods.
