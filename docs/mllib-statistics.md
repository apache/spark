---
layout: global
title: Basic Statistics - RDD-based API
displayTitle: Basic Statistics - RDD-based API
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

## Summary statistics

We provide column summary statistics for `RDD[Vector]` through the function `colStats`
available in `Statistics`.

<div class="codetabs">
<div data-lang="scala" markdown="1">

[`colStats()`](api/scala/index.html#org.apache.spark.mllib.stat.Statistics$) returns an instance of
[`MultivariateStatisticalSummary`](api/scala/index.html#org.apache.spark.mllib.stat.MultivariateStatisticalSummary),
which contains the column-wise max, min, mean, variance, and number of nonzeros, as well as the
total count.

Refer to the [`MultivariateStatisticalSummary` Scala docs](api/scala/index.html#org.apache.spark.mllib.stat.MultivariateStatisticalSummary) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/SummaryStatisticsExample.scala %}
</div>

<div data-lang="java" markdown="1">

[`colStats()`](api/java/org/apache/spark/mllib/stat/Statistics.html) returns an instance of
[`MultivariateStatisticalSummary`](api/java/org/apache/spark/mllib/stat/MultivariateStatisticalSummary.html),
which contains the column-wise max, min, mean, variance, and number of nonzeros, as well as the
total count.

Refer to the [`MultivariateStatisticalSummary` Java docs](api/java/org/apache/spark/mllib/stat/MultivariateStatisticalSummary.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaSummaryStatisticsExample.java %}
</div>

<div data-lang="python" markdown="1">
[`colStats()`](api/python/pyspark.mllib.html#pyspark.mllib.stat.Statistics.colStats) returns an instance of
[`MultivariateStatisticalSummary`](api/python/pyspark.mllib.html#pyspark.mllib.stat.MultivariateStatisticalSummary),
which contains the column-wise max, min, mean, variance, and number of nonzeros, as well as the
total count.

Refer to the [`MultivariateStatisticalSummary` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.stat.MultivariateStatisticalSummary) for more details on the API.

{% include_example python/mllib/summary_statistics_example.py %}
</div>

</div>

## Correlations

Calculating the correlation between two series of data is a common operation in Statistics. In `spark.mllib`
we provide the flexibility to calculate pairwise correlations among many series. The supported
correlation methods are currently Pearson's and Spearman's correlation.

<div class="codetabs">
<div data-lang="scala" markdown="1">
[`Statistics`](api/scala/index.html#org.apache.spark.mllib.stat.Statistics$) provides methods to
calculate correlations between series. Depending on the type of input, two `RDD[Double]`s or
an `RDD[Vector]`, the output will be a `Double` or the correlation `Matrix` respectively.

Refer to the [`Statistics` Scala docs](api/scala/index.html#org.apache.spark.mllib.stat.Statistics$) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/CorrelationsExample.scala %}
</div>

<div data-lang="java" markdown="1">
[`Statistics`](api/java/org/apache/spark/mllib/stat/Statistics.html) provides methods to
calculate correlations between series. Depending on the type of input, two `JavaDoubleRDD`s or
a `JavaRDD<Vector>`, the output will be a `Double` or the correlation `Matrix` respectively.

Refer to the [`Statistics` Java docs](api/java/org/apache/spark/mllib/stat/Statistics.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaCorrelationsExample.java %}
</div>

<div data-lang="python" markdown="1">
[`Statistics`](api/python/pyspark.mllib.html#pyspark.mllib.stat.Statistics) provides methods to
calculate correlations between series. Depending on the type of input, two `RDD[Double]`s or
an `RDD[Vector]`, the output will be a `Double` or the correlation `Matrix` respectively.

Refer to the [`Statistics` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.stat.Statistics) for more details on the API.

{% include_example python/mllib/correlations_example.py %}
</div>

</div>

## Stratified sampling

Unlike the other statistics functions, which reside in `spark.mllib`, stratified sampling methods,
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

{% include_example scala/org/apache/spark/examples/mllib/StratifiedSamplingExample.scala %}
</div>

<div data-lang="java" markdown="1">
[`sampleByKeyExact()`](api/java/org/apache/spark/api/java/JavaPairRDD.html) allows users to
sample exactly $\lceil f_k \cdot n_k \rceil \, \forall k \in K$ items, where $f_k$ is the desired
fraction for key $k$, $n_k$ is the number of key-value pairs for key $k$, and $K$ is the set of
keys. Sampling without replacement requires one additional pass over the RDD to guarantee sample
size, whereas sampling with replacement requires two additional passes.

{% include_example java/org/apache/spark/examples/mllib/JavaStratifiedSamplingExample.java %}
</div>
<div data-lang="python" markdown="1">
[`sampleByKey()`](api/python/pyspark.html#pyspark.RDD.sampleByKey) allows users to
sample approximately $\lceil f_k \cdot n_k \rceil \, \forall k \in K$ items, where $f_k$ is the
desired fraction for key $k$, $n_k$ is the number of key-value pairs for key $k$, and $K$ is the
set of keys.

*Note:* `sampleByKeyExact()` is currently not supported in Python.

{% include_example python/mllib/stratified_sampling_example.py %}
</div>

</div>

## Hypothesis testing

Hypothesis testing is a powerful tool in statistics to determine whether a result is statistically
significant, whether this result occurred by chance or not. `spark.mllib` currently supports Pearson's
chi-squared ( $\chi^2$) tests for goodness of fit and independence. The input data types determine
whether the goodness of fit or the independence test is conducted. The goodness of fit test requires
an input type of `Vector`, whereas the independence test requires a `Matrix` as input.

`spark.mllib` also supports the input type `RDD[LabeledPoint]` to enable feature selection via chi-squared
independence tests.

<div class="codetabs">
<div data-lang="scala" markdown="1">
[`Statistics`](api/scala/index.html#org.apache.spark.mllib.stat.Statistics$) provides methods to
run Pearson's chi-squared tests. The following example demonstrates how to run and interpret
hypothesis tests.

{% include_example scala/org/apache/spark/examples/mllib/HypothesisTestingExample.scala %}
</div>

<div data-lang="java" markdown="1">
[`Statistics`](api/java/org/apache/spark/mllib/stat/Statistics.html) provides methods to
run Pearson's chi-squared tests. The following example demonstrates how to run and interpret
hypothesis tests.

Refer to the [`ChiSqTestResult` Java docs](api/java/org/apache/spark/mllib/stat/test/ChiSqTestResult.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaHypothesisTestingExample.java %}
</div>

<div data-lang="python" markdown="1">
[`Statistics`](api/python/index.html#pyspark.mllib.stat.Statistics$) provides methods to
run Pearson's chi-squared tests. The following example demonstrates how to run and interpret
hypothesis tests.

Refer to the [`Statistics` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.stat.Statistics) for more details on the API.

{% include_example python/mllib/hypothesis_testing_example.py %}
</div>

</div>

Additionally, `spark.mllib` provides a 1-sample, 2-sided implementation of the Kolmogorov-Smirnov (KS) test
for equality of probability distributions. By providing the name of a theoretical distribution
(currently solely supported for the normal distribution) and its parameters, or a function to
calculate the cumulative distribution according to a given theoretical distribution, the user can
test the null hypothesis that their sample is drawn from that distribution. In the case that the
user tests against the normal distribution (`distName="norm"`), but does not provide distribution
parameters, the test initializes to the standard normal distribution and logs an appropriate
message.

<div class="codetabs">
<div data-lang="scala" markdown="1">
[`Statistics`](api/scala/index.html#org.apache.spark.mllib.stat.Statistics$) provides methods to
run a 1-sample, 2-sided Kolmogorov-Smirnov test. The following example demonstrates how to run
and interpret the hypothesis tests.

Refer to the [`Statistics` Scala docs](api/scala/index.html#org.apache.spark.mllib.stat.Statistics$) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/HypothesisTestingKolmogorovSmirnovTestExample.scala %}
</div>

<div data-lang="java" markdown="1">
[`Statistics`](api/java/org/apache/spark/mllib/stat/Statistics.html) provides methods to
run a 1-sample, 2-sided Kolmogorov-Smirnov test. The following example demonstrates how to run
and interpret the hypothesis tests.

Refer to the [`Statistics` Java docs](api/java/org/apache/spark/mllib/stat/Statistics.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaHypothesisTestingKolmogorovSmirnovTestExample.java %}
</div>

<div data-lang="python" markdown="1">
[`Statistics`](api/python/pyspark.mllib.html#pyspark.mllib.stat.Statistics) provides methods to
run a 1-sample, 2-sided Kolmogorov-Smirnov test. The following example demonstrates how to run
and interpret the hypothesis tests.

Refer to the [`Statistics` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.stat.Statistics) for more details on the API.

{% include_example python/mllib/hypothesis_testing_kolmogorov_smirnov_test_example.py %}
</div>
</div>

### Streaming Significance Testing
`spark.mllib` provides online implementations of some tests to support use cases
like A/B testing. These tests may be performed on a Spark Streaming
`DStream[(Boolean, Double)]` where the first element of each tuple
indicates control group (`false`) or treatment group (`true`) and the
second element is the value of an observation.

Streaming significance testing supports the following parameters:

* `peacePeriod` - The number of initial data points from the stream to
ignore, used to mitigate novelty effects.
* `windowSize` - The number of past batches to perform hypothesis
testing over. Setting to `0` will perform cumulative processing using
all prior batches.


<div class="codetabs">
<div data-lang="scala" markdown="1">
[`StreamingTest`](api/scala/index.html#org.apache.spark.mllib.stat.test.StreamingTest)
provides streaming hypothesis testing.

{% include_example scala/org/apache/spark/examples/mllib/StreamingTestExample.scala %}
</div>

<div data-lang="java" markdown="1">
[`StreamingTest`](api/java/index.html#org.apache.spark.mllib.stat.test.StreamingTest)
provides streaming hypothesis testing.

{% include_example java/org/apache/spark/examples/mllib/JavaStreamingTestExample.java %}
</div>
</div>


## Random data generation

Random data generation is useful for randomized algorithms, prototyping, and performance testing.
`spark.mllib` supports generating random RDDs with i.i.d. values drawn from a given distribution:
uniform, standard normal, or Poisson.

<div class="codetabs">
<div data-lang="scala" markdown="1">
[`RandomRDDs`](api/scala/index.html#org.apache.spark.mllib.random.RandomRDDs$) provides factory
methods to generate random double RDDs or vector RDDs.
The following example generates a random double RDD, whose values follows the standard normal
distribution `N(0, 1)`, and then map it to `N(1, 4)`.

Refer to the [`RandomRDDs` Scala docs](api/scala/index.html#org.apache.spark.mllib.random.RandomRDDs$) for details on the API.

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

Refer to the [`RandomRDDs` Java docs](api/java/org/apache/spark/mllib/random/RandomRDDs) for details on the API.

{% highlight java %}
import org.apache.spark.SparkContext;
import org.apache.spark.api.JavaDoubleRDD;
import static org.apache.spark.mllib.random.RandomRDDs.*;

JavaSparkContext jsc = ...

// Generate a random double RDD that contains 1 million i.i.d. values drawn from the
// standard normal distribution `N(0, 1)`, evenly distributed in 10 partitions.
JavaDoubleRDD u = normalJavaRDD(jsc, 1000000L, 10);
// Apply a transform to get a random double RDD following `N(1, 4)`.
JavaDoubleRDD v = u.mapToDouble(x -> 1.0 + 2.0 * x);
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
[`RandomRDDs`](api/python/pyspark.mllib.html#pyspark.mllib.random.RandomRDDs) provides factory
methods to generate random double RDDs or vector RDDs.
The following example generates a random double RDD, whose values follows the standard normal
distribution `N(0, 1)`, and then map it to `N(1, 4)`.

Refer to the [`RandomRDDs` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.random.RandomRDDs) for more details on the API.

{% highlight python %}
from pyspark.mllib.random import RandomRDDs

sc = ... # SparkContext

# Generate a random double RDD that contains 1 million i.i.d. values drawn from the
# standard normal distribution `N(0, 1)`, evenly distributed in 10 partitions.
u = RandomRDDs.normalRDD(sc, 1000000L, 10)
# Apply a transform to get a random double RDD following `N(1, 4)`.
v = u.map(lambda x: 1.0 + 2.0 * x)
{% endhighlight %}
</div>
</div>

## Kernel density estimation

[Kernel density estimation](https://en.wikipedia.org/wiki/Kernel_density_estimation) is a technique
useful for visualizing empirical probability distributions without requiring assumptions about the
particular distribution that the observed samples are drawn from. It computes an estimate of the
probability density function of a random variables, evaluated at a given set of points. It achieves
this estimate by expressing the PDF of the empirical distribution at a particular point as the
mean of PDFs of normal distributions centered around each of the samples.

<div class="codetabs">

<div data-lang="scala" markdown="1">
[`KernelDensity`](api/scala/index.html#org.apache.spark.mllib.stat.KernelDensity) provides methods
to compute kernel density estimates from an RDD of samples. The following example demonstrates how
to do so.

Refer to the [`KernelDensity` Scala docs](api/scala/index.html#org.apache.spark.mllib.stat.KernelDensity) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/KernelDensityEstimationExample.scala %}
</div>

<div data-lang="java" markdown="1">
[`KernelDensity`](api/java/index.html#org.apache.spark.mllib.stat.KernelDensity) provides methods
to compute kernel density estimates from an RDD of samples. The following example demonstrates how
to do so.

Refer to the [`KernelDensity` Java docs](api/java/org/apache/spark/mllib/stat/KernelDensity.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaKernelDensityEstimationExample.java %}
</div>

<div data-lang="python" markdown="1">
[`KernelDensity`](api/python/pyspark.mllib.html#pyspark.mllib.stat.KernelDensity) provides methods
to compute kernel density estimates from an RDD of samples. The following example demonstrates how
to do so.

Refer to the [`KernelDensity` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.stat.KernelDensity) for more details on the API.

{% include_example python/mllib/kernel_density_estimation_example.py %}
</div>

</div>
