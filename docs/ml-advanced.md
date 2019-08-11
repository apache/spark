---
layout: global
title: Advanced topics
displayTitle: Advanced topics
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
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
Quasi-Newton](https://www.microsoft.com/en-us/research/wp-content/uploads/2007/01/andrew07scalable.pdf)
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
\min_{\mathbf{x}}\frac{1}{2} \sum_{i=1}^n \frac{w_i(\mathbf{a}_i^T \mathbf{x} -b_i)^2}{\sum_{k=1}^n w_k} + \frac{\lambda}{\delta}\left[\frac{1}{2}(1 - \alpha)\sum_{j=1}^m(\sigma_j x_j)^2 + \alpha\sum_{j=1}^m |\sigma_j x_j|\right]
\]`
where $\lambda$ is the regularization parameter, $\alpha$ is the elastic-net mixing parameter, $\delta$ is the population standard deviation of the label
and $\sigma_j$ is the population standard deviation of the j-th feature column.

This objective function requires only one pass over the data to collect the statistics necessary to solve it. For an
$n \times m$ data matrix, these statistics require only $O(m^2)$ storage and so can be stored on a single machine when $m$ (the number of features) is
relatively small. We can then solve the normal equations on a single machine using local methods like direct Cholesky factorization or iterative optimization programs.

Spark MLlib currently supports two types of solvers for the normal equations: Cholesky factorization and Quasi-Newton methods (L-BFGS/OWL-QN). Cholesky factorization
depends on a positive definite covariance matrix (i.e. columns of the data matrix must be linearly independent) and will fail if this condition is violated. Quasi-Newton methods
are still capable of providing a reasonable solution even when the covariance matrix is not positive definite, so the normal equation solver can also fall back to 
Quasi-Newton methods in this case. This fallback is currently always enabled for the `LinearRegression` and `GeneralizedLinearRegression` estimators.

`WeightedLeastSquares` supports L1, L2, and elastic-net regularization and provides options to enable or disable regularization and standardization. In the case where no 
L1 regularization is applied (i.e. $\alpha = 0$), there exists an analytical solution and either Cholesky or Quasi-Newton solver may be used. When $\alpha > 0$ no analytical 
solution exists and we instead use the Quasi-Newton solver to find the coefficients iteratively. 

In order to make the normal equation approach efficient, `WeightedLeastSquares` requires that the number of features is no more than 4096. For larger problems, use L-BFGS instead.

## Iteratively reweighted least squares (IRLS)

MLlib implements [iteratively reweighted least squares (IRLS)](https://en.wikipedia.org/wiki/Iteratively_reweighted_least_squares) by [IterativelyReweightedLeastSquares]({{site.SPARK_GITHUB_URL}}/blob/v{{site.SPARK_VERSION_SHORT}}/mllib/src/main/scala/org/apache/spark/ml/optim/IterativelyReweightedLeastSquares.scala).
It can be used to find the maximum likelihood estimates of a generalized linear model (GLM), find M-estimator in robust regression and other optimization problems.
Refer to [Iteratively Reweighted Least Squares for Maximum Likelihood Estimation, and some Robust and Resistant Alternatives](http://www.jstor.org/stable/2345503) for more information.

It solves certain optimization problems iteratively through the following procedure:

* linearize the objective at current solution and update corresponding weight.
* solve a weighted least squares (WLS) problem by WeightedLeastSquares.
* repeat above steps until convergence.

Since it involves solving a weighted least squares (WLS) problem by `WeightedLeastSquares` in each iteration,
it also requires the number of features to be no more than 4096.
Currently IRLS is used as the default solver of [GeneralizedLinearRegression](api/scala/index.html#org.apache.spark.ml.regression.GeneralizedLinearRegression).
