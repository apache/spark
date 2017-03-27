---
layout: global
title: Frequent Pattern Mining
displayTitle: Frequent Pattern Mining
---

Mining frequent items, itemsets, subsequences, or other substructures is usually among the
first steps to analyze a large-scale dataset, which has been an active research topic in
data mining for years.
We refer users to Wikipedia's [association rule learning](http://en.wikipedia.org/wiki/Association_rule_learning)
for more information.

**Table of Contents**

* This will become a table of contents (this text will be scraped).
{:toc}

## FP-Growth

The FP-growth algorithm is described in the paper
[Han et al., Mining frequent patterns without candidate generation](http://dx.doi.org/10.1145/335191.335372),
where "FP" stands for frequent pattern.
Given a dataset of transactions, the first step of FP-growth is to calculate item frequencies and identify frequent items.
Different from [Apriori-like](http://en.wikipedia.org/wiki/Apriori_algorithm) algorithms designed for the same purpose,
the second step of FP-growth uses a suffix tree (FP-tree) structure to encode transactions without generating candidate sets
explicitly, which are usually expensive to generate.
After the second step, the frequent itemsets can be extracted from the FP-tree.
In `spark.mllib`, we implemented a parallel version of FP-growth called PFP,
as described in [Li et al., PFP: Parallel FP-growth for query recommendation](http://dx.doi.org/10.1145/1454008.1454027).
PFP distributes the work of growing FP-trees based on the suffices of transactions,
and hence more scalable than a single-machine implementation.
We refer users to the papers for more details.

`spark.ml`'s FP-growth implementation takes the following (hyper-)parameters:

* `minSupport`: the minimum support for an itemset to be identified as frequent.
  For example, if an item appears 3 out of 5 transactions, it has a support of 3/5=0.6.
* `minConfidence`: minimum confidence for generating Association Rule. The parameter will not affect the mining
  for frequent itemsets,, but specify the minimum confidence for generating association rules from frequent itemsets.
* `numPartitions`: the number of partitions used to distribute the work. By default the param is not set, and
  partition number of the input dataset is used.

The `FPGrowthModel` provides:

* `freqItemsets`: frequent itemsets in the format of DataFrame("items"[Array], "freq"[Long])
* `associationRules`: association rules generated with confidence above `minConfidence`, in the format of 
  DataFrame("antecedent"[Array], "consequent"[Array], "confidence"[Double]).
* `transform`: The transform method examines the input items in `itemsCol` against all the association rules and
  summarize the consequents as prediction. The prediction column has the same data type as the
  `itemsCol` and does not contain existing items in the `itemsCol`.


**Examples**

<div class="codetabs">

<div data-lang="scala" markdown="1">
Refer to the [Scala API docs](api/scala/index.html#org.apache.spark.ml.fpm.FPGrowth) for more details.

{% include_example scala/org/apache/spark/examples/ml/FPGrowthExample.scala %}
</div>

<div data-lang="java" markdown="1">
Refer to the [Java API docs](api/java/org/apache/spark/ml/fpm/FPGrowth.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaFPGrowthExample.java %}
</div>

<div data-lang="python" markdown="1">
Refer to the [Python API docs](api/python/pyspark.ml.html#pyspark.ml.fpm.FPGrowth) for more details.

{% include_example python/ml/fpgrowth_example.py %}
</div>

</div>
