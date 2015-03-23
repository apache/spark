---
layout: global
title: Basic Statistics - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Basic Statistics 
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

## Summary statistics 

We provide column summary statistics for `RDD[Vector]` through the function `colStats` 
available in `Statistics`.

<div class="codetabs">
<div data-lang="scala" markdown="1">

[`colStats()`](api/scala/index.html#org.apache.spark.mllib.stat.Statistics$) returns an instance of
[`MultivariateStatisticalSummary`](api/scala/index.html#org.apache.spark.mllib.stat.MultivariateStatisticalSummary),
which contains the column-wise max, min, mean, variance, and number of nonzeros, as well as the
total count.

{% highlight scala %}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

val observations: RDD[Vector] = ... // an RDD of Vectors

// Compute column summary statistics.
val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
println(summary.mean) // a dense vector containing the mean value for each column
println(summary.variance) // column-wise variance
println(summary.numNonzeros) // number of nonzeros in each column

{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

[`colStats()`](api/java/org/apache/spark/mllib/stat/Statistics.html) returns an instance of
[`MultivariateStatisticalSummary`](api/java/org/apache/spark/mllib/stat/MultivariateStatisticalSummary.html),
which contains the column-wise max, min, mean, variance, and number of nonzeros, as well as the
total count.

{% highlight java %}
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

JavaSparkContext jsc = ...

JavaRDD<Vector> mat = ... // an RDD of Vectors

// Compute column summary statistics.
MultivariateStatisticalSummary summary = Statistics.colStats(mat.rdd());
System.out.println(summary.mean()); // a dense vector containing the mean value for each column
System.out.println(summary.variance()); // column-wise variance
System.out.println(summary.numNonzeros()); // number of nonzeros in each column

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
[`colStats()`](api/python/pyspark.mllib.html#pyspark.mllib.stat.Statistics.colStats) returns an instance of
[`MultivariateStatisticalSummary`](api/python/pyspark.mllib.html#pyspark.mllib.stat.MultivariateStatisticalSummary),
which contains the column-wise max, min, mean, variance, and number of nonzeros, as well as the
total count.

{% highlight python %}
from pyspark.mllib.stat import Statistics

sc = ... # SparkContext

mat = ... # an RDD of Vectors

# Compute column summary statistics.
summary = Statistics.colStats(mat)
print summary.mean()
print summary.variance()
print summary.numNonzeros()

{% endhighlight %}
</div>

</div>

## Correlations

Calculating the correlation between two series of data is a common operation in Statistics. In MLlib
we provide the flexibility to calculate pairwise correlations among many series. The supported 
correlation methods are currently Pearson's and Spearman's correlation.
 
<div class="codetabs">
<div data-lang="scala" markdown="1">
[`Statistics`](api/scala/index.html#org.apache.spark.mllib.stat.Statistics$) provides methods to 
calculate correlations between series. Depending on the type of input, two `RDD[Double]`s or 
an `RDD[Vector]`, the output will be a `Double` or the correlation `Matrix` respectively.

{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics

val sc: SparkContext = ...

val seriesX: RDD[Double] = ... // a series
val seriesY: RDD[Double] = ... // must have the same number of partitions and cardinality as seriesX

// compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a 
// method is not specified, Pearson's method will be used by default. 
val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")

val data: RDD[Vector] = ... // note that each Vector is a row and not a column

// calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method.
// If a method is not specified, Pearson's method will be used by default. 
val correlMatrix: Matrix = Statistics.corr(data, "pearson")

{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
[`Statistics`](api/java/org/apache/spark/mllib/stat/Statistics.html) provides methods to 
calculate correlations between series. Depending on the type of input, two `JavaDoubleRDD`s or 
a `JavaRDD<Vector>`, the output will be a `Double` or the correlation `Matrix` respectively.

{% highlight java %}
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.stat.Statistics;

JavaSparkContext jsc = ...

JavaDoubleRDD seriesX = ... // a series
JavaDoubleRDD seriesY = ... // must have the same number of partitions and cardinality as seriesX

// compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a 
// method is not specified, Pearson's method will be used by default. 
Double correlation = Statistics.corr(seriesX.srdd(), seriesY.srdd(), "pearson");

JavaRDD<Vector> data = ... // note that each Vector is a row and not a column

// calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method.
// If a method is not specified, Pearson's method will be used by default. 
Matrix correlMatrix = Statistics.corr(data.rdd(), "pearson");

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
[`Statistics`](api/python/pyspark.mllib.html#pyspark.mllib.stat.Statistics) provides methods to 
calculate correlations between series. Depending on the type of input, two `RDD[Double]`s or 
an `RDD[Vector]`, the output will be a `Double` or the correlation `Matrix` respectively.

{% highlight python %}
from pyspark.mllib.stat import Statistics

sc = ... # SparkContext

seriesX = ... # a series
seriesY = ... # must have the same number of partitions and cardinality as seriesX

# Compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a 
# method is not specified, Pearson's method will be used by default. 
print Statistics.corr(seriesX, seriesY, method="pearson")

data = ... # an RDD of Vectors
# calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method.
# If a method is not specified, Pearson's method will be used by default. 
print Statistics.corr(data, method="pearson")

{% endhighlight %}
</div>

</div>

## Stratified sampling

Unlike the other statistics functions, which reside in MLlib, stratified sampling methods,
`sampleByKey` and `sampleByKeyExact`, can be performed on RDD's of key-value pairs. For stratified
sampling, the keys can be thought of as a label and the value as a specific attribute. For example 
the key can be man or woman, or document ids, and the respective values can be the list of ages 
of the people in the population or the list of words in the documents. The `sampleByKey` method 
will flip a coin to decide whether an observation will be sampled or not, therefore requires one 
pass over the data, and provides an *expected* sample size. `sampleByKeyExact` requires significant 
more resources than the per-stratum simple random sampling used in `sampleByKey`, but will provide
the exact sampling size with 99.99% confidence. `sampleByKeyExact` is currently not supported in 
python.

<div class="codetabs">
<div data-lang="scala" markdown="1">
[`sampleByKeyExact()`](api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions) allows users to
sample exactly $\lceil f_k \cdot n_k \rceil \, \forall k \in K$ items, where $f_k$ is the desired 
fraction for key $k$, $n_k$ is the number of key-value pairs for key $k$, and $K$ is the set of
keys. Sampling without replacement requires one additional pass over the RDD to guarantee sample 
size, whereas sampling with replacement requires two additional passes.

{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions

val sc: SparkContext = ...

val data = ... // an RDD[(K, V)] of any key value pairs
val fractions: Map[K, Double] = ... // specify the exact fraction desired from each key

// Get an exact sample from each stratum
val approxSample = data.sampleByKey(withReplacement = false, fractions)
val exactSample = data.sampleByKeyExact(withReplacement = false, fractions)

{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
[`sampleByKeyExact()`](api/java/org/apache/spark/api/java/JavaPairRDD.html) allows users to
sample exactly $\lceil f_k \cdot n_k \rceil \, \forall k \in K$ items, where $f_k$ is the desired 
fraction for key $k$, $n_k$ is the number of key-value pairs for key $k$, and $K$ is the set of
keys. Sampling without replacement requires one additional pass over the RDD to guarantee sample 
size, whereas sampling with replacement requires two additional passes.

{% highlight java %}
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

JavaSparkContext jsc = ...

JavaPairRDD<K, V> data = ... // an RDD of any key value pairs
Map<K, Object> fractions = ... // specify the exact fraction desired from each key

// Get an exact sample from each stratum
JavaPairRDD<K, V> approxSample = data.sampleByKey(false, fractions);
JavaPairRDD<K, V> exactSample = data.sampleByKeyExact(false, fractions);

{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
[`sampleByKey()`](api/python/pyspark.html#pyspark.RDD.sampleByKey) allows users to
sample approximately $\lceil f_k \cdot n_k \rceil \, \forall k \in K$ items, where $f_k$ is the 
desired fraction for key $k$, $n_k$ is the number of key-value pairs for key $k$, and $K$ is the 
set of keys.

*Note:* `sampleByKeyExact()` is currently not supported in Python.

{% highlight python %}

sc = ... # SparkContext

data = ... # an RDD of any key value pairs
fractions = ... # specify the exact fraction desired from each key as a dictionary

approxSample = data.sampleByKey(False, fractions);

{% endhighlight %}
</div>

</div>

## Hypothesis testing

Hypothesis testing is a powerful tool in statistics to determine whether a result is statistically 
significant, whether this result occurred by chance or not. MLlib currently supports Pearson's 
chi-squared ( $\chi^2$) tests for goodness of fit and independence. The input data types determine 
whether the goodness of fit or the independence test is conducted. The goodness of fit test requires 
an input type of `Vector`, whereas the independence test requires a `Matrix` as input.

MLlib also supports the input type `RDD[LabeledPoint]` to enable feature selection via chi-squared 
independence tests.

<div class="codetabs">
<div data-lang="scala" markdown="1">
[`Statistics`](api/scala/index.html#org.apache.spark.mllib.stat.Statistics$) provides methods to 
run Pearson's chi-squared tests. The following example demonstrates how to run and interpret 
hypothesis tests.

{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics._

val sc: SparkContext = ...

val vec: Vector = ... // a vector composed of the frequencies of events

// compute the goodness of fit. If a second vector to test against is not supplied as a parameter, 
// the test runs against a uniform distribution.  
val goodnessOfFitTestResult = Statistics.chiSqTest(vec)
println(goodnessOfFitTestResult) // summary of the test including the p-value, degrees of freedom, 
                                 // test statistic, the method used, and the null hypothesis.

val mat: Matrix = ... // a contingency matrix

// conduct Pearson's independence test on the input contingency matrix
val independenceTestResult = Statistics.chiSqTest(mat) 
println(independenceTestResult) // summary of the test including the p-value, degrees of freedom...

val obs: RDD[LabeledPoint] = ... // (feature, label) pairs.

// The contingency table is constructed from the raw (feature, label) pairs and used to conduct
// the independence test. Returns an array containing the ChiSquaredTestResult for every feature 
// against the label.
val featureTestResults: Array[ChiSqTestResult] = Statistics.chiSqTest(obs)
var i = 1
featureTestResults.foreach { result =>
    println(s"Column $i:\n$result")
    i += 1
} // summary of the test 

{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
[`Statistics`](api/java/org/apache/spark/mllib/stat/Statistics.html) provides methods to 
run Pearson's chi-squared tests. The following example demonstrates how to run and interpret 
hypothesis tests.

{% highlight java %}
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;

JavaSparkContext jsc = ...

Vector vec = ... // a vector composed of the frequencies of events

// compute the goodness of fit. If a second vector to test against is not supplied as a parameter, 
// the test runs against a uniform distribution.  
ChiSqTestResult goodnessOfFitTestResult = Statistics.chiSqTest(vec);
// summary of the test including the p-value, degrees of freedom, test statistic, the method used, 
// and the null hypothesis.
System.out.println(goodnessOfFitTestResult);

Matrix mat = ... // a contingency matrix

// conduct Pearson's independence test on the input contingency matrix
ChiSqTestResult independenceTestResult = Statistics.chiSqTest(mat);
// summary of the test including the p-value, degrees of freedom...
System.out.println(independenceTestResult);

JavaRDD<LabeledPoint> obs = ... // an RDD of labeled points

// The contingency table is constructed from the raw (feature, label) pairs and used to conduct
// the independence test. Returns an array containing the ChiSquaredTestResult for every feature 
// against the label.
ChiSqTestResult[] featureTestResults = Statistics.chiSqTest(obs.rdd());
int i = 1;
for (ChiSqTestResult result : featureTestResults) {
    System.out.println("Column " + i + ":");
    System.out.println(result); // summary of the test
    i++;
}

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
[`Statistics`](api/python/index.html#pyspark.mllib.stat.Statistics$) provides methods to
run Pearson's chi-squared tests. The following example demonstrates how to run and interpret
hypothesis tests.

{% highlight python %}
from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors, Matrices
from pyspark.mllib.regresssion import LabeledPoint
from pyspark.mllib.stat import Statistics

sc = SparkContext()

vec = Vectors.dense(...) # a vector composed of the frequencies of events

# compute the goodness of fit. If a second vector to test against is not supplied as a parameter,
# the test runs against a uniform distribution.
goodnessOfFitTestResult = Statistics.chiSqTest(vec)
print goodnessOfFitTestResult # summary of the test including the p-value, degrees of freedom,
                              # test statistic, the method used, and the null hypothesis.

mat = Matrices.dense(...) # a contingency matrix

# conduct Pearson's independence test on the input contingency matrix
independenceTestResult = Statistics.chiSqTest(mat)
print independenceTestResult  # summary of the test including the p-value, degrees of freedom...

obs = sc.parallelize(...)  # LabeledPoint(feature, label) .

# The contingency table is constructed from an RDD of LabeledPoint and used to conduct
# the independence test. Returns an array containing the ChiSquaredTestResult for every feature
# against the label.
featureTestResults = Statistics.chiSqTest(obs)

for i, result in enumerate(featureTestResults):
    print "Column $d:" % (i + 1)
    print result
{% endhighlight %}
</div>

</div>

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
[`RandomRDDs`](api/python/pyspark.mllib.html#pyspark.mllib.random.RandomRDDs) provides factory
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
