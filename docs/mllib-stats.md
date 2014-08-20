---
layout: global
title: Statistics Functionality - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Statistics Functionality 
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

## Random data generation

Random data generation is useful for randomized algorithms, prototyping, and performance testing.
MLlib supports generating random RDDs with i.i.d. values drawn from a given distribution:
uniform, standard normal, or Poisson.

<div class="codetabs">
<div data-lang="scala" markdown="1">
[`RandomRDDs`](api/scala/index.html#org.apache.spark.mllib.random.RandomRDDs) provides factory
methods to generate random double RDDs or vector RDDs.
The following example generates a random double RDD, whose values follows the standard normal
distribution `N(0, 1)`, and then map it to `N(1, 4)`.

{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.random.RandomRDDs._

val sc: SparkContext = ...

// Generate a random double RDD that contains 1 million i.i.d. values drawn from the
// standard normal distribution `N(0, 1)`, evenly distributed in 10 partitions.
val u = normalRDD(sc, 1000000L, 10)
// Apply a transform to get a random double RDD following `N(1, 4)`.
val v = u.map(x => 1.0 + 2.0 * x)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
[`RandomRDDs`](api/java/index.html#org.apache.spark.mllib.random.RandomRDDs) provides factory
methods to generate random double RDDs or vector RDDs.
The following example generates a random double RDD, whose values follows the standard normal
distribution `N(0, 1)`, and then map it to `N(1, 4)`.

{% highlight java %}
import org.apache.spark.SparkContext;
import org.apache.spark.api.JavaDoubleRDD;
import static org.apache.spark.mllib.random.RandomRDDs.*;

JavaSparkContext jsc = ...

// Generate a random double RDD that contains 1 million i.i.d. values drawn from the
// standard normal distribution `N(0, 1)`, evenly distributed in 10 partitions.
JavaDoubleRDD u = normalJavaRDD(jsc, 1000000L, 10);
// Apply a transform to get a random double RDD following `N(1, 4)`.
JavaDoubleRDD v = u.map(
  new Function<Double, Double>() {
    public Double call(Double x) {
      return 1.0 + 2.0 * x;
    }
  });
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
[`RandomRDDs`](api/python/pyspark.mllib.random.RandomRDDs-class.html) provides factory
methods to generate random double RDDs or vector RDDs.
The following example generates a random double RDD, whose values follows the standard normal
distribution `N(0, 1)`, and then map it to `N(1, 4)`.

{% highlight python %}
from pyspark.mllib.random import RandomRDDs

sc = ... # SparkContext

# Generate a random double RDD that contains 1 million i.i.d. values drawn from the
# standard normal distribution `N(0, 1)`, evenly distributed in 10 partitions.
u = RandomRDDs.uniformRDD(sc, 1000000L, 10)
# Apply a transform to get a random double RDD following `N(1, 4)`.
v = u.map(lambda x: 1.0 + 2.0 * x)
{% endhighlight %}
</div>

</div>

## Stratified Sampling 

## Summary Statistics 

### Multivariate summary statistics

We provide column summary statistics for `RowMatrix` (note: this functionality is not currently supported in `IndexedRowMatrix` or `CoordinateMatrix`). 
If the number of columns is not large, e.g., on the order of thousands, then the 
covariance matrix can also be computed as a local matrix, which requires $\mathcal{O}(n^2)$ storage where $n$ is the
number of columns. The total CPU time is $\mathcal{O}(m n^2)$, where $m$ is the number of rows,
and is faster if the rows are sparse.

<div class="codetabs">
<div data-lang="scala" markdown="1">

[`computeColumnSummaryStatistics()`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.RowMatrix) returns an instance of
[`MultivariateStatisticalSummary`](api/scala/index.html#org.apache.spark.mllib.stat.MultivariateStatisticalSummary),
which contains the column-wise max, min, mean, variance, and number of nonzeros, as well as the
total count.

{% highlight scala %}
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary

val mat: RowMatrix = ... // a RowMatrix

// Compute column summary statistics.
val summary: MultivariateStatisticalSummary = mat.computeColumnSummaryStatistics()
println(summary.mean) // a dense vector containing the mean value for each column
println(summary.variance) // column-wise variance
println(summary.numNonzeros) // number of nonzeros in each column

// Compute the covariance matrix.
val cov: Matrix = mat.computeCovariance()
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

[`RowMatrix#computeColumnSummaryStatistics`](api/java/org/apache/spark/mllib/linalg/distributed/RowMatrix.html#computeColumnSummaryStatistics()) returns an instance of
[`MultivariateStatisticalSummary`](api/java/org/apache/spark/mllib/stat/MultivariateStatisticalSummary.html),
which contains the column-wise max, min, mean, variance, and number of nonzeros, as well as the
total count.

{% highlight java %}
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;

RowMatrix mat = ... // a RowMatrix

// Compute column summary statistics.
MultivariateStatisticalSummary summary = mat.computeColumnSummaryStatistics();
System.out.println(summary.mean()); // a dense vector containing the mean value for each column
System.out.println(summary.variance()); // column-wise variance
System.out.println(summary.numNonzeros()); // number of nonzeros in each column

// Compute the covariance matrix.
Matrix cov = mat.computeCovariance();
{% endhighlight %}
</div>
</div>


## Hypothesis Testing 
