---
layout: global
title: Frequent Pattern Mining - spark.mllib
displayTitle: Frequent Pattern Mining - spark.mllib
---

Mining frequent items, itemsets, subsequences, or other substructures is usually among the
first steps to analyze a large-scale dataset, which has been an active research topic in
data mining for years.
We refer users to Wikipedia's [association rule learning](http://en.wikipedia.org/wiki/Association_rule_learning)
for more information.
`spark.mllib` provides a parallel implementation of FP-growth,
a popular algorithm to mining frequent itemsets.

## FP-growth

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

`spark.mllib`'s FP-growth implementation takes the following (hyper-)parameters:

* `minSupport`: the minimum support for an itemset to be identified as frequent.
  For example, if an item appears 3 out of 5 transactions, it has a support of 3/5=0.6.
* `numPartitions`: the number of partitions used to distribute the work.

**Examples**

<div class="codetabs">
<div data-lang="scala" markdown="1">

[`FPGrowth`](api/scala/index.html#org.apache.spark.mllib.fpm.FPGrowth) implements the
FP-growth algorithm.
It take a `RDD` of transactions, where each transaction is an `Array` of items of a generic type.
Calling `FPGrowth.run` with transactions returns an
[`FPGrowthModel`](api/scala/index.html#org.apache.spark.mllib.fpm.FPGrowthModel)
that stores the frequent itemsets with their frequencies.  The following
example illustrates how to mine frequent itemsets and association rules
(see [Association
Rules](mllib-frequent-pattern-mining.html#association-rules) for
details) from `transactions`.

Refer to the [`FPGrowth` Scala docs](api/scala/index.html#org.apache.spark.mllib.fpm.FPGrowth) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/SimpleFPGrowth.scala %}

</div>

<div data-lang="java" markdown="1">

[`FPGrowth`](api/java/org/apache/spark/mllib/fpm/FPGrowth.html) implements the
FP-growth algorithm.
It take an `JavaRDD` of transactions, where each transaction is an `Iterable` of items of a generic type.
Calling `FPGrowth.run` with transactions returns an
[`FPGrowthModel`](api/java/org/apache/spark/mllib/fpm/FPGrowthModel.html)
that stores the frequent itemsets with their frequencies.  The following
example illustrates how to mine frequent itemsets and association rules
(see [Association
Rules](mllib-frequent-pattern-mining.html#association-rules) for
details) from `transactions`.

Refer to the [`FPGrowth` Java docs](api/java/org/apache/spark/mllib/fpm/FPGrowth.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaSimpleFPGrowth.java %}

</div>

<div data-lang="python" markdown="1">

[`FPGrowth`](api/python/pyspark.mllib.html#pyspark.mllib.fpm.FPGrowth) implements the
FP-growth algorithm.
It take an `RDD` of transactions, where each transaction is an `List` of items of a generic type.
Calling `FPGrowth.train` with transactions returns an
[`FPGrowthModel`](api/python/pyspark.mllib.html#pyspark.mllib.fpm.FPGrowthModel)
that stores the frequent itemsets with their frequencies.

Refer to the [`FPGrowth` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.fpm.FPGrowth) for more details on the API.

{% include_example python/mllib/fpgrowth_example.py %}

</div>

</div>

## Association Rules

<div class="codetabs">
<div data-lang="scala" markdown="1">
[AssociationRules](api/scala/index.html#org.apache.spark.mllib.fpm.AssociationRules)
implements a parallel rule generation algorithm for constructing rules
that have a single item as the consequent.

Refer to the [`AssociationRules` Scala docs](api/java/org/apache/spark/mllib/fpm/AssociationRules.html) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/AssociationRulesExample.scala %}

</div>

<div data-lang="java" markdown="1">
[AssociationRules](api/java/org/apache/spark/mllib/fpm/AssociationRules.html)
implements a parallel rule generation algorithm for constructing rules
that have a single item as the consequent.

Refer to the [`AssociationRules` Java docs](api/java/org/apache/spark/mllib/fpm/AssociationRules.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaAssociationRulesExample.java %}

</div>
</div>

## PrefixSpan

PrefixSpan is a sequential pattern mining algorithm described in
[Pei et al., Mining Sequential Patterns by Pattern-Growth: The
PrefixSpan Approach](http://dx.doi.org/10.1109%2FTKDE.2004.77). We refer
the reader to the referenced paper for formalizing the sequential
pattern mining problem.

`spark.mllib`'s PrefixSpan implementation takes the following parameters:

* `minSupport`: the minimum support required to be considered a frequent
  sequential pattern.
* `maxPatternLength`: the maximum length of a frequent sequential
  pattern. Any frequent pattern exceeding this length will not be
  included in the results.
* `maxLocalProjDBSize`: the maximum number of items allowed in a
  prefix-projected database before local iterative processing of the
  projected databse begins. This parameter should be tuned with respect
  to the size of your executors.

**Examples**

The following example illustrates PrefixSpan running on the sequences
(using same notation as Pei et al):

~~~
  <(12)3>
  <1(32)(12)>
  <(12)5>
  <6>
~~~

<div class="codetabs">
<div data-lang="scala" markdown="1">

[`PrefixSpan`](api/scala/index.html#org.apache.spark.mllib.fpm.PrefixSpan) implements the
PrefixSpan algorithm.
Calling `PrefixSpan.run` returns a
[`PrefixSpanModel`](api/scala/index.html#org.apache.spark.mllib.fpm.PrefixSpanModel)
that stores the frequent sequences with their frequencies.

Refer to the [`PrefixSpan` Scala docs](api/scala/index.html#org.apache.spark.mllib.fpm.PrefixSpan) and [`PrefixSpanModel` Scala docs](api/scala/index.html#org.apache.spark.mllib.fpm.PrefixSpanModel) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/PrefixSpanExample.scala %}

</div>

<div data-lang="java" markdown="1">

[`PrefixSpan`](api/java/org/apache/spark/mllib/fpm/PrefixSpan.html) implements the
PrefixSpan algorithm.
Calling `PrefixSpan.run` returns a
[`PrefixSpanModel`](api/java/org/apache/spark/mllib/fpm/PrefixSpanModel.html)
that stores the frequent sequences with their frequencies.

Refer to the [`PrefixSpan` Java docs](api/java/org/apache/spark/mllib/fpm/PrefixSpan.html) and [`PrefixSpanModel` Java docs](api/java/org/apache/spark/mllib/fpm/PrefixSpanModel.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaPrefixSpanExample.java %}

</div>
</div>

