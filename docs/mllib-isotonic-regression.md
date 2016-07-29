---
layout: global
title: Isotonic regression - RDD-based API
displayTitle: Regression - RDD-based API
---

## Isotonic regression
[Isotonic regression](http://en.wikipedia.org/wiki/Isotonic_regression)
belongs to the family of regression algorithms. Formally isotonic regression is a problem where
given a finite set of real numbers `$Y = {y_1, y_2, ..., y_n}$` representing observed responses
and `$X = {x_1, x_2, ..., x_n}$` the unknown response values to be fitted
finding a function that minimises

`\begin{equation}
  f(x) = \sum_{i=1}^n w_i (y_i - x_i)^2
\end{equation}`

with respect to complete order subject to
`$x_1\le x_2\le ...\le x_n$` where `$w_i$` are positive weights.
The resulting function is called isotonic regression and it is unique.
It can be viewed as least squares problem under order restriction.
Essentially isotonic regression is a
[monotonic function](http://en.wikipedia.org/wiki/Monotonic_function)
best fitting the original data points.

`spark.mllib` supports a
[pool adjacent violators algorithm](http://doi.org/10.1198/TECH.2010.10111)
which uses an approach to
[parallelizing isotonic regression](http://doi.org/10.1007/978-3-642-99789-1_10).
The training input is a RDD of tuples of three double values that represent
label, feature and weight in this order. Additionally IsotonicRegression algorithm has one
optional parameter called $isotonic$ defaulting to true.
This argument specifies if the isotonic regression is
isotonic (monotonically increasing) or antitonic (monotonically decreasing).

Training returns an IsotonicRegressionModel that can be used to predict
labels for both known and unknown features. The result of isotonic regression
is treated as piecewise linear function. The rules for prediction therefore are:

* If the prediction input exactly matches a training feature
  then associated prediction is returned. In case there are multiple predictions with the same
  feature then one of them is returned. Which one is undefined
  (same as java.util.Arrays.binarySearch).
* If the prediction input is lower or higher than all training features
  then prediction with lowest or highest feature is returned respectively.
  In case there are multiple predictions with the same feature
  then the lowest or highest is returned respectively.
* If the prediction input falls between two training features then prediction is treated
  as piecewise linear function and interpolated value is calculated from the
  predictions of the two closest features. In case there are multiple values
  with the same feature then the same rules as in previous point are used.

### Examples

<div class="codetabs">
<div data-lang="scala" markdown="1">
Data are read from a file where each line has a format label,feature
i.e. 4710.28,500.00. The data are split to training and testing set.
Model is created using the training set and a mean squared error is calculated from the predicted
labels and real labels in the test set.

Refer to the [`IsotonicRegression` Scala docs](api/scala/index.html#org.apache.spark.mllib.regression.IsotonicRegression) and [`IsotonicRegressionModel` Scala docs](api/scala/index.html#org.apache.spark.mllib.regression.IsotonicRegressionModel) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/IsotonicRegressionExample.scala %}
</div>
<div data-lang="java" markdown="1">
Data are read from a file where each line has a format label,feature
i.e. 4710.28,500.00. The data are split to training and testing set.
Model is created using the training set and a mean squared error is calculated from the predicted
labels and real labels in the test set.

Refer to the [`IsotonicRegression` Java docs](api/java/org/apache/spark/mllib/regression/IsotonicRegression.html) and [`IsotonicRegressionModel` Java docs](api/java/org/apache/spark/mllib/regression/IsotonicRegressionModel.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaIsotonicRegressionExample.java %}
</div>
<div data-lang="python" markdown="1">
Data are read from a file where each line has a format label,feature
i.e. 4710.28,500.00. The data are split to training and testing set.
Model is created using the training set and a mean squared error is calculated from the predicted
labels and real labels in the test set.

Refer to the [`IsotonicRegression` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.regression.IsotonicRegression) and [`IsotonicRegressionModel` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.regression.IsotonicRegressionModel) for more details on the API.

{% include_example python/mllib/isotonic_regression_example.py %}
</div>
</div>
