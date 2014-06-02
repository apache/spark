---
layout: global
title: Machine Learning Library (MLlib)
---

MLlib is a Spark implementation of some common machine learning algorithms and utilities,
including classification, regression, clustering, collaborative
filtering, dimensionality reduction, as well as underlying optimization primitives:

* [Basics](mllib-basics.html)
  * data types 
  * summary statistics
* Classification and regression
  * [linear support vector machine (SVM)](mllib-linear-methods.html#linear-support-vector-machine-svm)
  * [logistic regression](mllib-linear-methods.html#logistic-regression)
  * [linear least squares, Lasso, and ridge regression](mllib-linear-methods.html#linear-least-squares-lasso-and-ridge-regression)
  * [decision tree](mllib-decision-tree.html)
  * [naive Bayes](mllib-naive-bayes.html)
* [Collaborative filtering](mllib-collaborative-filtering.html)
  * alternating least squares (ALS)
* [Clustering](mllib-clustering.html)
  * k-means
* [Dimensionality reduction](mllib-dimensionality-reduction.html)
  * singular value decomposition (SVD)
  * principal component analysis (PCA)
* [Optimization](mllib-optimization.html)
  * stochastic gradient descent
  * limited-memory BFGS (L-BFGS)

MLlib is a new component under active development.
The APIs marked `Experimental`/`DeveloperApi` may change in future releases, 
and we will provide migration guide between releases.

# Dependencies

MLlib uses linear algebra packages [Breeze](http://www.scalanlp.org/), which depends on
[netlib-java](https://github.com/fommil/netlib-java), and
[jblas](https://github.com/mikiobraun/jblas). 
`netlib-java` and `jblas` depend on native Fortran routines.
You need to install the
[gfortran runtime library](https://github.com/mikiobraun/jblas/wiki/Missing-Libraries) if it is not
already present on your nodes. MLlib will throw a linking error if it cannot detect these libraries
automatically.  Due to license issues, we do not include `netlib-java`'s native libraries in MLlib's
dependency set. If no native library is available at runtime, you will see a warning message.  To
use native libraries from `netlib-java`, please include artifact
`com.github.fommil.netlib:all:1.1.2` as a dependency of your project or build your own (see
[instructions](https://github.com/fommil/netlib-java/blob/master/README.md#machine-optimised-system-libraries)).

To use MLlib in Python, you will need [NumPy](http://www.numpy.org) version 1.4 or newer.

---

# Migration Guide

## From 0.9 to 1.0

In MLlib v1.0, we support both dense and sparse input in a unified way, which introduces a few
breaking changes.  If your data is sparse, please store it in a sparse format instead of dense to
take advantage of sparsity in both storage and computation.

<div class="codetabs">
<div data-lang="scala" markdown="1">

We used to represent a feature vector by `Array[Double]`, which is replaced by
[`Vector`](api/scala/index.html#org.apache.spark.mllib.linalg.Vector) in v1.0. Algorithms that used
to accept `RDD[Array[Double]]` now take
`RDD[Vector]`. [`LabeledPoint`](api/scala/index.html#org.apache.spark.mllib.regression.LabeledPoint)
is now a wrapper of `(Double, Vector)` instead of `(Double, Array[Double])`. Converting
`Array[Double]` to `Vector` is straightforward:

{% highlight scala %}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

val array: Array[Double] = ... // a double array
val vector: Vector = Vectors.dense(array) // a dense vector
{% endhighlight %}

[`Vectors`](api/scala/index.html#org.apache.spark.mllib.linalg.Vectors$) provides factory methods to create sparse vectors.

*Note*. Scala imports `scala.collection.immutable.Vector` by default, so you have to import `org.apache.spark.mllib.linalg.Vector` explicitly to use MLlib's `Vector`.

</div>

<div data-lang="java" markdown="1">

We used to represent a feature vector by `double[]`, which is replaced by
[`Vector`](api/java/index.html?org/apache/spark/mllib/linalg/Vector.html) in v1.0. Algorithms that used
to accept `RDD<double[]>` now take
`RDD<Vector>`. [`LabeledPoint`](api/java/index.html?org/apache/spark/mllib/regression/LabeledPoint.html)
is now a wrapper of `(double, Vector)` instead of `(double, double[])`. Converting `double[]` to
`Vector` is straightforward:

{% highlight java %}
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

double[] array = ... // a double array
Vector vector = Vectors.dense(array); // a dense vector
{% endhighlight %}

[`Vectors`](api/scala/index.html#org.apache.spark.mllib.linalg.Vectors$) provides factory methods to
create sparse vectors.

</div>

<div data-lang="python" markdown="1">

We used to represent a labeled feature vector in a NumPy array, where the first entry corresponds to
the label and the rest are features.  This representation is replaced by class
[`LabeledPoint`](api/python/pyspark.mllib.regression.LabeledPoint-class.html), which takes both
dense and sparse feature vectors.

{% highlight python %}
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint

# Create a labeled point with a positive label and a dense feature vector.
pos = LabeledPoint(1.0, [1.0, 0.0, 3.0])

# Create a labeled point with a negative label and a sparse feature vector.
neg = LabeledPoint(0.0, SparseVector(3, [0, 2], [1.0, 3.0]))
{% endhighlight %}
</div>
</div>
