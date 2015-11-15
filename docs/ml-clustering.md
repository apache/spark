---
layout: global
title: Multilayer perceptron classifier - ML
displayTitle: <a href="ml-guide.html">ML</a> - Clustering
---

In `spark.ml`, we implement the corresponding pipeline API for 
[clustering in mllib](mllib-clustering.html).

## Latent Dirichlet allocation (LDA)

`LDA` is implemented as an Estimator that supports both `EMLDAOptimizer` and `OnlineLDAOptimizer`,
and generates `LocalLDAModel` and `DistributedLDAModel` respectively, as the base models.

<div class="codetabs">

<div data-lang="scala" markdown="1">
{% include_example scala/org/apache/spark/examples/ml/LDAExample.scala %}
</div>

<div data-lang="java" markdown="1">
{% include_example java/org/apache/spark/examples/ml/JavaLDAExample.java %}
</div>

</div>
