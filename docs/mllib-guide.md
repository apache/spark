---
layout: global
title: Machine Learning Library (MLlib)
---

MLlib is a Spark implementation of some common machine learning algorithms and utilities,
including classification, regression, clustering, collaborative
filtering, dimensionality reduction, as well as underlying optimization primitives:

* <a href="mllib-data-types.html">Data types</a>
  * local vector and matrix
  * labeled point
  * distributed matrix
* Classification and regression
  * <a href="mllib-linear-methods.html">linear methods</a>
    * logistic regression
    * support vector machine (SVM)
    * linear linear squares, Lasso, and ridge regression
  * <a href="mllib-decision-tree.html">decision tree</a>
  * <a href="mllib-naive-bayes.html">naive Bayes</a>
* <a href="mllib-collaborative-filtering.html">Collaborative Filtering</a>
  * alternating least squares (ALS)
* <a href="mllib-clustering.html">Clustering</a>
  * k-means
* <a href="mllib-linear-algebra.html">Linear algebra and statistics</a>
  * singular value decomposition (SVD)
  * principal component analysis (PCA)
* <a href="mllib-optimization.html">Optimization</a>
  * stochastic gradient descent
  * limited-memory BFGS (L-BFGS)
* Utilities

## Dependencies

MLlib uses linear algebra packages [jblas](https://github.com/mikiobraun/jblas) and [netlib-java](https://github.com/fommil/netlib-java) (via [Breeze](http://www.scalanlp.org/)), which depend on native Fortran routines. You need to install the
[gfortran runtime library](https://github.com/mikiobraun/jblas/wiki/Missing-Libraries)
if it is not already present on your nodes. MLlib will throw a linking error if it cannot
detect these libraries automatically.
Due to license issues, we do not include `netlib-java`'s native libraries in MLlib's dependency set. If no native library is available at runtime, you will see a warning message.
To use native libraries from `netlib-java`, please include artifact `com.github.fommil.netlib:all:1.1.2` as a dependency of your project or build your own (see [instructions](https://github.com/fommil/netlib-java/blob/master/README.md#machine-optimised-system-libraries)).

To use MLlib in Python, you will need [NumPy](http://www.numpy.org) version 1.4 or newer.

---

## Migration guide

### From 0.9 to 1.0

In MLlib v1.0, we support both dense and sparse input in a unified way, which introduces a few breaking changes.

<div class="codetabs">
<div data-lang="scala" markdown="1">
We used to represent a feature vector by `Array[Double]`, which is replaced by [`Vector`](api/mllib/index.html#org.apache.spark.mllib.linalg.Vector) in v1.0. Algorithms that used to accept `RDD[Array[Double]]` now take `RDD[Vector]`. [`LabeledPoint`](api/mllib/index.html#org.apache.spark.mllib.regression.LabeledPoint) is now a wrapper of `(Double, Vector)` instead of `(Double, Array[Double])`. Converting `Array[Double]` to `Vector` is straightforward:

{% highlight scala %}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

val array: Array[Double] = ... // a double array
val vector: Vector = Vectors.dense(array) // a dense vector
{% endhighlight %}

If your data is sparse, please store it in a sparse format instead of dense to take advantage of sparsity in both storage and computation. [`Vectors`](api/mllib/index.html#org.apache.spark.mllib.linalg.Vectors$) provides factory methods to create sparse vectors.

*Note*. Scala imports `scala.collection.immutable.Vector` by default, so you have to import `org.apache.spark.mllib.linalg.Vector` explicitly to use MLlib's `Vector`.

</div>

<div data-lang="java" markdown="1">
We used to represent a feature vector by `double[]`, which is replaced by [`Vector`](api/mllib/index.html#org.apache.spark.mllib.linalg.Vector) in v1.0. Algorithms that used to accept `RDD<double[]>` now take `RDD<Vector>`. [`LabeledPoint`](api/mllib/index.html#org.apache.spark.mllib.regression.LabeledPoint) is now a wrapper of `(double, Vector)` instead of `(double, double[])`. Converting `double[]` to `Vector` is straightforward:

{% highlight java %}
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

double[] array = ... // a double array
Vector vector = Vectors.dense(array) // a dense vector
{% endhighlight %}

If your data is sparse, please store it in a sparse format instead of dense to take advantage of sparsity in both storage and computation. [`Vectors`](api/mllib/index.html#org.apache.spark.mllib.linalg.Vectors$) provides factory methods to create sparse vectors.
</div>
</div>
