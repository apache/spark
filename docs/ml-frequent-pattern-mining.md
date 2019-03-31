---
layout: global
title: Frequent Pattern Mining
displayTitle: Frequent Pattern Mining
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
[Han et al., Mining frequent patterns without candidate generation](https://doi.org/10.1145/335191.335372),
where "FP" stands for frequent pattern.
Given a dataset of transactions, the first step of FP-growth is to calculate item frequencies and identify frequent items.
Different from [Apriori-like](http://en.wikipedia.org/wiki/Apriori_algorithm) algorithms designed for the same purpose,
the second step of FP-growth uses a suffix tree (FP-tree) structure to encode transactions without generating candidate sets
explicitly, which are usually expensive to generate.
After the second step, the frequent itemsets can be extracted from the FP-tree.
In `spark.mllib`, we implemented a parallel version of FP-growth called PFP,
as described in [Li et al., PFP: Parallel FP-growth for query recommendation](https://doi.org/10.1145/1454008.1454027).
PFP distributes the work of growing FP-trees based on the suffixes of transactions,
and hence is more scalable than a single-machine implementation.
We refer users to the papers for more details.

`spark.ml`'s FP-growth implementation takes the following (hyper-)parameters:

* `minSupport`: the minimum support for an itemset to be identified as frequent.
  For example, if an item appears 3 out of 5 transactions, it has a support of 3/5=0.6.
* `minConfidence`: minimum confidence for generating Association Rule. Confidence is an indication of how often an
  association rule has been found to be true. For example, if in the transactions itemset `X` appears 4 times, `X`
  and `Y` co-occur only 2 times, the confidence for the rule `X => Y` is then 2/4 = 0.5. The parameter will not
  affect the mining for frequent itemsets, but specify the minimum confidence for generating association rules
  from frequent itemsets.
* `numPartitions`: the number of partitions used to distribute the work. By default the param is not set, and
  number of partitions of the input dataset is used.

The `FPGrowthModel` provides:

* `freqItemsets`: frequent itemsets in the format of DataFrame("items"[Array], "freq"[Long])
* `associationRules`: association rules generated with confidence above `minConfidence`, in the format of 
  DataFrame("antecedent"[Array], "consequent"[Array], "confidence"[Double]).
* `transform`: For each transaction in `itemsCol`, the `transform` method will compare its items against the antecedents
  of each association rule. If the record contains all the antecedents of a specific association rule, the rule
  will be considered as applicable and its consequents will be added to the prediction result. The transform
  method will summarize the consequents from all the applicable rules as prediction. The prediction column has
  the same data type as `itemsCol` and does not contain existing items in the `itemsCol`.


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

<div data-lang="r" markdown="1">

Refer to the [R API docs](api/R/spark.fpGrowth.html) for more details.

{% include_example r/ml/fpm.R %}
</div>

</div>

## PrefixSpan

PrefixSpan is a sequential pattern mining algorithm described in
[Pei et al., Mining Sequential Patterns by Pattern-Growth: The
PrefixSpan Approach](https://doi.org/10.1109%2FTKDE.2004.77). We refer
the reader to the referenced paper for formalizing the sequential
pattern mining problem.

`spark.ml`'s PrefixSpan implementation takes the following parameters:

* `minSupport`: the minimum support required to be considered a frequent
  sequential pattern.
* `maxPatternLength`: the maximum length of a frequent sequential
  pattern. Any frequent pattern exceeding this length will not be
  included in the results.
* `maxLocalProjDBSize`: the maximum number of items allowed in a
  prefix-projected database before local iterative processing of the
  projected database begins. This parameter should be tuned with respect
  to the size of your executors.
* `sequenceCol`: the name of the sequence column in dataset (default "sequence"), rows with
  nulls in this column are ignored.

**Examples**

<div class="codetabs">

<div data-lang="scala" markdown="1">
Refer to the [Scala API docs](api/scala/index.html#org.apache.spark.ml.fpm.PrefixSpan) for more details.

{% include_example scala/org/apache/spark/examples/ml/PrefixSpanExample.scala %}
</div>

<div data-lang="java" markdown="1">
Refer to the [Java API docs](api/java/org/apache/spark/ml/fpm/PrefixSpan.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaPrefixSpanExample.java %}
</div>

<div data-lang="python" markdown="1">
Refer to the [Python API docs](api/python/pyspark.ml.html#pyspark.ml.fpm.PrefixSpan) for more details.

{% include_example python/ml/prefixspan_example.py %}
</div>

<div data-lang="r" markdown="1">

Refer to the [R API docs](api/R/spark.prefixSpan.html) for more details.

{% include_example r/ml/prefixSpan.R %}
</div>

</div>
