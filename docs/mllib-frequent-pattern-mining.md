---
layout: global
title: Frequent Pattern Mining - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Frequent Pattern Mining
---

Mining frequent items, itemsets, subsequences, or other substructures is usually among the
first steps to analyze a large-scale dataset, which has been an active research topic in
data mining for years.
We refer users to Wikipedia's [association rule learning](http://en.wikipedia.org/wiki/Association_rule_learning)
for more information.
MLlib provides a parallel implementation of FP-growth,
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
In MLlib, we implemented a parallel version of FP-growth called PFP,
as described in [Li et al., PFP: Parallel FP-growth for query recommendation](http://dx.doi.org/10.1145/1454008.1454027).
PFP distributes the work of growing FP-trees based on the suffices of transactions,
and hence more scalable than a single-machine implementation.
We refer users to the papers for more details.

MLlib's FP-growth implementation takes the following (hyper-)parameters:

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


{% highlight scala %}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.fpm.FPGrowth

val data = sc.textFile("data/mllib/sample_fpgrowth.txt")

val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))

val fpg = new FPGrowth()
  .setMinSupport(0.2)
  .setNumPartitions(10)
val model = fpg.run(transactions)

model.freqItemsets.collect().foreach { itemset =>
  println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
}

val minConfidence = 0.8
model.generateAssociationRules(minConfidence).collect().foreach { rule =>
  println(
    rule.antecedent.mkString("[", ",", "]")
      + " => " + rule.consequent .mkString("[", ",", "]")
      + ", " + rule.confidence)
}
{% endhighlight %}

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

{% highlight java %}
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

SparkConf conf = new SparkConf().setAppName("FP-growth Example");
JavaSparkContext sc = new JavaSparkContext(conf);

JavaRDD<String> data = sc.textFile("data/mllib/sample_fpgrowth.txt");

JavaRDD<List<String>> transactions = data.map(
  new Function<String, List<String>>() {
    public List<String> call(String line) {
      String[] parts = line.split(" ");
      return Arrays.asList(parts);
    }
  }
);

FPGrowth fpg = new FPGrowth()
  .setMinSupport(0.2)
  .setNumPartitions(10);
FPGrowthModel<String> model = fpg.run(transactions);

for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
  System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
}

double minConfidence = 0.8;
for (AssociationRules.Rule<String> rule
    : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
  System.out.println(
    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
}
{% endhighlight %}

</div>

<div data-lang="python" markdown="1">

[`FPGrowth`](api/python/pyspark.mllib.html#pyspark.mllib.fpm.FPGrowth) implements the
FP-growth algorithm.
It take an `RDD` of transactions, where each transaction is an `List` of items of a generic type.
Calling `FPGrowth.train` with transactions returns an
[`FPGrowthModel`](api/python/pyspark.mllib.html#pyspark.mllib.fpm.FPGrowthModel)
that stores the frequent itemsets with their frequencies.

{% highlight python %}
from pyspark.mllib.fpm import FPGrowth

data = sc.textFile("data/mllib/sample_fpgrowth.txt")

transactions = data.map(lambda line: line.strip().split(' '))

model = FPGrowth.train(transactions, minSupport=0.2, numPartitions=10)

result = model.freqItemsets().collect()
for fi in result:
    print(fi)
{% endhighlight %}

</div>

</div>

## Association Rules

<div class="codetabs">
<div data-lang="scala" markdown="1">
[AssociationRules](api/scala/index.html#org.apache.spark.mllib.fpm.AssociationRules)
implements a parallel rule generation algorithm for constructing rules
that have a single item as the consequent.

{% highlight scala %}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset

val freqItemsets = sc.parallelize(Seq(
  new FreqItemset(Array("a"), 15L),
  new FreqItemset(Array("b"), 35L),
  new FreqItemset(Array("a", "b"), 12L)
));

val ar = new AssociationRules()
  .setMinConfidence(0.8)
val results = ar.run(freqItemsets)

results.collect().foreach { rule =>
  println("[" + rule.antecedent.mkString(",")
    + "=>"
    + rule.consequent.mkString(",") + "]," + rule.confidence)
}
{% endhighlight %}

</div>

<div data-lang="java" markdown="1">
[AssociationRules](api/java/org/apache/spark/mllib/fpm/AssociationRules.html)
implements a parallel rule generation algorithm for constructing rules
that have a single item as the consequent.

{% highlight java %}
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset;

JavaRDD<FPGrowth.FreqItemset<String>> freqItemsets = sc.parallelize(Arrays.asList(
  new FreqItemset<String>(new String[] {"a"}, 15L),
  new FreqItemset<String>(new String[] {"b"}, 35L),
  new FreqItemset<String>(new String[] {"a", "b"}, 12L)
));

AssociationRules arules = new AssociationRules()
  .setMinConfidence(0.8);
JavaRDD<AssociationRules.Rule<String>> results = arules.run(freqItemsets);

for (AssociationRules.Rule<String> rule: results.collect()) {
  System.out.println(
    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
}
{% endhighlight %}

</div>
</div>

## PrefixSpan

PrefixSpan is a sequential pattern mining algorithm described in
[Pei et al., Mining Sequential Patterns by Pattern-Growth: The
PrefixSpan Approach](http://dx.doi.org/10.1109%2FTKDE.2004.77). We refer
the reader to the referenced paper for formalizing the sequential
pattern mining problem.

MLlib's PrefixSpan implementation takes the following parameters:

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

{% highlight scala %}
import org.apache.spark.mllib.fpm.PrefixSpan

val sequences = sc.parallelize(Seq(
    Array(Array(1, 2), Array(3)),
    Array(Array(1), Array(3, 2), Array(1, 2)),
    Array(Array(1, 2), Array(5)),
    Array(Array(6))
  ), 2).cache()
val prefixSpan = new PrefixSpan()
  .setMinSupport(0.5)
  .setMaxPatternLength(5)
val model = prefixSpan.run(sequences)
model.freqSequences.collect().foreach { freqSequence =>
println(
  freqSequence.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]") + ", " + freqSequence.freq)
}
{% endhighlight %}

</div>

<div data-lang="java" markdown="1">

[`PrefixSpan`](api/java/org/apache/spark/mllib/fpm/PrefixSpan.html) implements the
PrefixSpan algorithm.
Calling `PrefixSpan.run` returns a
[`PrefixSpanModel`](api/java/org/apache/spark/mllib/fpm/PrefixSpanModel.html)
that stores the frequent sequences with their frequencies.

{% highlight java %}
import java.util.Arrays;
import java.util.List;

import org.apache.spark.mllib.fpm.PrefixSpan;
import org.apache.spark.mllib.fpm.PrefixSpanModel;

JavaRDD<List<List<Integer>>> sequences = sc.parallelize(Arrays.asList(
  Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3)),
  Arrays.asList(Arrays.asList(1), Arrays.asList(3, 2), Arrays.asList(1, 2)),
  Arrays.asList(Arrays.asList(1, 2), Arrays.asList(5)),
  Arrays.asList(Arrays.asList(6))
), 2);
PrefixSpan prefixSpan = new PrefixSpan()
  .setMinSupport(0.5)
  .setMaxPatternLength(5);
PrefixSpanModel<Integer> model = prefixSpan.run(sequences);
for (PrefixSpan.FreqSequence<Integer> freqSeq: model.freqSequences().toJavaRDD().collect()) {
  System.out.println(freqSeq.javaSequence() + ", " + freqSeq.freq());
}
{% endhighlight %}

</div>
</div>

