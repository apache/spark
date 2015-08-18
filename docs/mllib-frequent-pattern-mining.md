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
that stores the frequent itemsets with their frequencies.

{% highlight scala %}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.fpm.FPGrowth

val data = sc.textFile("data/mllib/sample_fpgrowth.txt")

val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))

val fpm = new FPGrowth()
  .setMinSupport(0.2)
  .setNumPartitions(10)
val model = fpm.run(transactions)

model.freqItemsets.collect().foreach { itemset =>
  println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
}
{% endhighlight %}

</div>

<div data-lang="java" markdown="1">

[`FPGrowth`](api/java/org/apache/spark/mllib/fpm/FPGrowth.html) implements the
FP-growth algorithm.
It take an `JavaRDD` of transactions, where each transaction is an `Iterable` of items of a generic type.
Calling `FPGrowth.run` with transactions returns an
[`FPGrowthModel`](api/java/org/apache/spark/mllib/fpm/FPGrowthModel.html)
that stores the frequent itemsets with their frequencies.

{% highlight java %}
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.fpm.FPGrowth;

SparkConf conf = new SparkConf().setAppName("FP-growth Example");
JavaSparkContext sc = new JavaSparkContext(conf);

JavaRDD<String> data = sc.textFile("data/mllib/sample_fpgrowth.txt");

JavaRDD<List<String>> transactions = data.map(
    new Function<String, List<String>>() {
        public List<String> call(String line) {
            String[] parts = line.split(" ");
            return Lists.newArrayList(parts);
        }
    }
);

FPGrowth fpm = new FPGrowth()
  .setMinSupport(0.2)
  .setNumPartitions(10);
FPGrowthModel<String> model = fpm.run(transactions);

for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
   System.out.println("[" + Joiner.on(",").join(itemset.javaItems()) + "], " + itemset.freq());
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

model = FPGrowth.train(transactions, 0.2, 10)

result = model.freqItemsets().collect()
for fi in result:
    print(fi)
{% endhighlight %}

</div>

</div>
