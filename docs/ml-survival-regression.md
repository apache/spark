---
layout: global
title: Survival Regression - spark.ml
displayTitle: Survival Regression - spark.ml
---


`\[
\newcommand{\R}{\mathbb{R}}
\newcommand{\E}{\mathbb{E}}
\newcommand{\x}{\mathbf{x}}
\newcommand{\y}{\mathbf{y}}
\newcommand{\wv}{\mathbf{w}}
\newcommand{\av}{\mathbf{\alpha}}
\newcommand{\bv}{\mathbf{b}}
\newcommand{\N}{\mathbb{N}}
\newcommand{\id}{\mathbf{I}}
\newcommand{\ind}{\mathbf{1}}
\newcommand{\0}{\mathbf{0}}
\newcommand{\unit}{\mathbf{e}}
\newcommand{\one}{\mathbf{1}}
\newcommand{\zero}{\mathbf{0}}
\]`


In `spark.ml`, we implement the [Accelerated failure time (AFT)](https://en.wikipedia.org/wiki/Accelerated_failure_time_model) 
model which is a parametric survival regression model for censored data. 
It describes a model for the log of survival time, so it's often called 
log-linear model for survival analysis. Different from 
[Proportional hazards](https://en.wikipedia.org/wiki/Proportional_hazards_model) model
designed for the same purpose, the AFT model is more easily to parallelize 
because each instance contribute to the objective function independently.

Given the values of the covariates $x^{'}$, for random lifetime $t_{i}$ of 
subjects i = 1, ..., n, with possible right-censoring, 
the likelihood function under the AFT model is given as:
`\[
L(\beta,\sigma)=\prod_{i=1}^n[\frac{1}{\sigma}f_{0}(\frac{\log{t_{i}}-x^{'}\beta}{\sigma})]^{\delta_{i}}S_{0}(\frac{\log{t_{i}}-x^{'}\beta}{\sigma})^{1-\delta_{i}}
\]`
Where $\delta_{i}$ is the indicator of the event has occurred i.e. uncensored or not.
Using $\epsilon_{i}=\frac{\log{t_{i}}-x^{'}\beta}{\sigma}$, the log-likelihood function
assumes the form:
`\[
\iota(\beta,\sigma)=\sum_{i=1}^{n}[-\delta_{i}\log\sigma+\delta_{i}\log{f_{0}}(\epsilon_{i})+(1-\delta_{i})\log{S_{0}(\epsilon_{i})}]
\]`
Where $S_{0}(\epsilon_{i})$ is the baseline survivor function,
and $f_{0}(\epsilon_{i})$ is corresponding density function.

The most commonly used AFT model is based on the Weibull distribution of the survival time. 
The Weibull distribution for lifetime corresponding to extreme value distribution for 
log of the lifetime, and the $S_{0}(\epsilon)$ function is:
`\[   
S_{0}(\epsilon_{i})=\exp(-e^{\epsilon_{i}})
\]`
the $f_{0}(\epsilon_{i})$ function is:
`\[
f_{0}(\epsilon_{i})=e^{\epsilon_{i}}\exp(-e^{\epsilon_{i}})
\]`
The log-likelihood function for AFT model with Weibull distribution of lifetime is:
`\[
\iota(\beta,\sigma)= -\sum_{i=1}^n[\delta_{i}\log\sigma-\delta_{i}\epsilon_{i}+e^{\epsilon_{i}}]
\]`
Due to minimizing the negative log-likelihood equivalent to maximum a posteriori probability,
the loss function we use to optimize is $-\iota(\beta,\sigma)$.
The gradient functions for $\beta$ and $\log\sigma$ respectively are:
`\[   
\frac{\partial (-\iota)}{\partial \beta}=\sum_{1=1}^{n}[\delta_{i}-e^{\epsilon_{i}}]\frac{x_{i}}{\sigma}
\]`
`\[ 
\frac{\partial (-\iota)}{\partial (\log\sigma)}=\sum_{i=1}^{n}[\delta_{i}+(\delta_{i}-e^{\epsilon_{i}})\epsilon_{i}]
\]`

The AFT model can be formulated as a convex optimization problem, 
i.e. the task of finding a minimizer of a convex function $-\iota(\beta,\sigma)$ 
that depends coefficients vector $\beta$ and the log of scale parameter $\log\sigma$.
The optimization algorithm underlying the implementation is L-BFGS.
The implementation matches the result from R's survival function 
[survreg](https://stat.ethz.ch/R-manual/R-devel/library/survival/html/survreg.html)

## Example:

<div class="codetabs">

<div data-lang="scala" markdown="1">
{% include_example scala/org/apache/spark/examples/ml/AFTSurvivalRegressionExample.scala %}
</div>

<div data-lang="java" markdown="1">
{% include_example java/org/apache/spark/examples/ml/JavaAFTSurvivalRegressionExample.java %}
</div>

<div data-lang="python" markdown="1">
{% include_example python/ml/aft_survival_regression.py %}
</div>

</div>