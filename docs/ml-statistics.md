---
layout: global
title: Basic Statistics
displayTitle: Basic Statistics
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

**Table of Contents**

* This will become a table of contents (this text will be scraped).
{:toc}

## Correlation

Calculating the correlation between two series of data is a common operation in Statistics. In `spark.ml`
we provide the flexibility to calculate pairwise correlations among many series. The supported
correlation methods are currently Pearson's and Spearman's correlation.

<div class="codetabs">
<div data-lang="scala" markdown="1">
[`Correlation`](api/scala/index.html#org.apache.spark.ml.stat.Correlation$)
computes the correlation matrix for the input Dataset of Vectors using the specified method.
The output will be a DataFrame that contains the correlation matrix of the column of vectors.

{% include_example scala/org/apache/spark/examples/ml/CorrelationExample.scala %}
</div>

<div data-lang="java" markdown="1">
[`Correlation`](api/java/org/apache/spark/ml/stat/Correlation.html)
computes the correlation matrix for the input Dataset of Vectors using the specified method.
The output will be a DataFrame that contains the correlation matrix of the column of vectors.

{% include_example java/org/apache/spark/examples/ml/JavaCorrelationExample.java %}
</div>

<div data-lang="python" markdown="1">
[`Correlation`](api/python/pyspark.ml.html#pyspark.ml.stat.Correlation$)
computes the correlation matrix for the input Dataset of Vectors using the specified method.
The output will be a DataFrame that contains the correlation matrix of the column of vectors.

{% include_example python/ml/correlation_example.py %}
</div>

</div>

## Hypothesis testing

Hypothesis testing is a powerful tool in statistics to determine whether a result is statistically
significant, whether this result occurred by chance or not. `spark.ml` currently supports Pearson's
Chi-squared ( $\chi^2$) tests for independence.

`ChiSquareTest` conducts Pearson's independence test for every feature against the label.
For each feature, the (feature, label) pairs are converted into a contingency matrix for which
the Chi-squared statistic is computed. All label and feature values must be categorical.

<div class="codetabs">
<div data-lang="scala" markdown="1">
Refer to the [`ChiSquareTest` Scala docs](api/scala/index.html#org.apache.spark.ml.stat.ChiSquareTest$) for details on the API.

{% include_example scala/org/apache/spark/examples/ml/ChiSquareTestExample.scala %}
</div>

<div data-lang="java" markdown="1">
Refer to the [`ChiSquareTest` Java docs](api/java/org/apache/spark/ml/stat/ChiSquareTest.html) for details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaChiSquareTestExample.java %}
</div>

<div data-lang="python" markdown="1">
Refer to the [`ChiSquareTest` Python docs](api/python/index.html#pyspark.ml.stat.ChiSquareTest$) for details on the API.

{% include_example python/ml/chi_square_test_example.py %}
</div>

</div>