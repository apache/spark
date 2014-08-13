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

## Data Generators 

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
