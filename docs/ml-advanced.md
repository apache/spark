---
layout: global
title: Advanced topics
displayTitle: Advanced topics
---

* Table of contents
{:toc}

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

# Optimization of linear methods (developer)

## Limited-memory BFGS (L-BFGS)
[L-BFGS](http://en.wikipedia.org/wiki/Limited-memory_BFGS) is an optimization 
algorithm in the family of quasi-Newton methods to solve the optimization problems of the form 
`$\min_{\wv \in\R^d} \; f(\wv)$`. The L-BFGS method approximates the objective function locally as a 
quadratic without evaluating the second partial derivatives of the objective function to construct the 
Hessian matrix. The Hessian matrix is approximated by previous gradient evaluations, so there is no 
vertical scalability issue (the number of training features) unlike computing the Hessian matrix 
explicitly in Newton's method. As a result, L-BFGS often achieves faster convergence compared with 
other first-order optimizations.

[Orthant-Wise Limited-memory
Quasi-Newton](http://research-srv.microsoft.com/en-us/um/people/jfgao/paper/icml07scalable.pdf)
(OWL-QN) is an extension of L-BFGS that can effectively handle L1 and elastic net regularization.

L-BFGS is used as a solver for [LinearRegression](api/scala/index.html#org.apache.spark.ml.regression.LinearRegression),
[LogisticRegression](api/scala/index.html#org.apache.spark.ml.classification.LogisticRegression),
[AFTSurvivalRegression](api/scala/index.html#org.apache.spark.ml.regression.AFTSurvivalRegression)
and [MultilayerPerceptronClassifier](api/scala/index.html#org.apache.spark.ml.classification.MultilayerPerceptronClassifier).

MLlib L-BFGS solver calls the corresponding implementation in [breeze](https://github.com/scalanlp/breeze/blob/master/math/src/main/scala/breeze/optimize/LBFGS.scala).

## Normal equation solver for weighted least squares

MLlib implements normal equation solver for [weighted least squares](https://en.wikipedia.org/wiki/Least_squares#Weighted_least_squares) by [WeightedLeastSquares]({{site.SPARK_GITHUB_URL}}/blob/v{{site.SPARK_VERSION_SHORT}}/mllib/src/main/scala/org/apache/spark/ml/optim/WeightedLeastSquares.scala).

Given $n$ weighted observations $(w_i, a_i, b_i)$:

* $w_i$ the weight of i-th observation
* $a_i$ the features vector of i-th observation
* $b_i$ the label of i-th observation

The number of features for each observation is $m$. We use the following weighted least squares formulation:
`\[   
minimize_{x}\frac{1}{2} \sum_{i=1}^n \frac{w_i(a_i^T x -b_i)^2}{\sum_{k=1}^n w_k} + \frac{1}{2}\frac{\lambda}{\delta}\sum_{j=1}^m(\sigma_{j} x_{j})^2
\]`
where $\lambda$ is the regularization parameter, $\delta$ is the population standard deviation of the label
and $\sigma_j$ is the population standard deviation of the j-th feature column.

This objective function has an analytic solution and it requires only one pass over the data to collect necessary statistics to solve.
Unlike the original dataset which can only be stored in a distributed system,
these statistics can be loaded into memory on a single machine if the number of features is relatively small, and then we can solve the objective function through Cholesky factorization on the driver.

WeightedLeastSquares only supports L2 regularization and provides options to enable or disable regularization and standardization.
In order to make the normal equation approach efficient, WeightedLeastSquares requires that the number of features be no more than 4096. For larger problems, use L-BFGS instead.

## Iteratively reweighted least squares (IRLS)

MLlib implements [iteratively reweighted least squares (IRLS)](https://en.wikipedia.org/wiki/Iteratively_reweighted_least_squares) by [IterativelyReweightedLeastSquares]({{site.SPARK_GITHUB_URL}}/blob/v{{site.SPARK_VERSION_SHORT}}/mllib/src/main/scala/org/apache/spark/ml/optim/IterativelyReweightedLeastSquares.scala).
It can be used to find the maximum likelihood estimates of a generalized linear model (GLM), find M-estimator in robust regression and other optimization problems.
Refer to [Iteratively Reweighted Least Squares for Maximum Likelihood Estimation, and some Robust and Resistant Alternatives](http://www.jstor.org/stable/2345503) for more information.

It solves certain optimization problems iteratively through the following procedure:

* linearize the objective at current solution and update corresponding weight.
* solve a weighted least squares (WLS) problem by WeightedLeastSquares.
* repeat above steps until convergence.

Since it involves solving a weighted least squares (WLS) problem by WeightedLeastSquares in each iteration,
it also requires the number of features to be no more than 4096.
Currently IRLS is used as the default solver of [GeneralizedLinearRegression](api/scala/index.html#org.apache.spark.ml.regression.GeneralizedLinearRegression).
