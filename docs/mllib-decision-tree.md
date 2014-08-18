---
layout: global
title: Decision Tree - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Decision Tree
---

* Table of contents
{:toc}

Decision trees and their ensembles are popular methods for the machine learning tasks of
classification and regression. Decision trees are widely used since they are easy to interpret,
handle categorical variables, extend to the multiclass classification setting, do not require
feature scaling and are able to capture nonlinearities and feature interactions. Tree ensemble
algorithms such as decision forest and boosting are among the top performers for classification and
regression tasks.

## Basic algorithm

The decision tree is a greedy algorithm that performs a recursive binary partitioning of the feature
space by choosing a single element from the *best split set* where each element of the set maximizes
the information gain at a tree node. In other words, the split chosen at each tree node is chosen
from the set `$\underset{s}{\operatorname{argmax}} IG(D,s)$` where `$IG(D,s)$` is the information
gain when a split `$s$` is applied to a dataset `$D$`.

### Node impurity and information gain

The *node impurity* is a measure of the homogeneity of the labels at the node. The current
implementation provides two impurity measures for classification (Gini impurity and entropy) and one
impurity measure for regression (variance).

<table class="table">
  <thead>
    <tr><th>Impurity</th><th>Task</th><th>Formula</th><th>Description</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Gini impurity</td>
	  <td>Classification</td>
	  <td>$\sum_{i=1}^{M} f_i(1-f_i)$</td><td>$f_i$ is the frequency of label $i$ at a node and $M$ is the number of unique labels.</td>
    </tr>
    <tr>
      <td>Entropy</td>
	  <td>Classification</td>
	  <td>$\sum_{i=1}^{M} -f_ilog(f_i)$</td><td>$f_i$ is the frequency of label $i$ at a node and $M$ is the number of unique labels.</td>
    </tr>
    <tr>
      <td>Variance</td>
	  <td>Regression</td>
     <td>$\frac{1}{n} \sum_{i=1}^{N} (x_i - \mu)^2$</td><td>$y_i$ is label for an instance,
	  $N$ is the number of instances and $\mu$ is the mean given by $\frac{1}{N} \sum_{i=1}^n x_i$.</td>
    </tr>
  </tbody>
</table>

The *information gain* is the difference in the parent node impurity and the weighted sum of the two
child node impurities. Assuming that a split $s$ partitions the dataset `$D$` of size `$N$` into two
datasets `$D_{left}$` and `$D_{right}$` of sizes `$N_{left}$` and `$N_{right}$`, respectively:

`$IG(D,s) = Impurity(D) - \frac{N_{left}}{N} Impurity(D_{left}) - \frac{N_{right}}{N} Impurity(D_{right})$`

### Split candidates

**Continuous features**

For small datasets in single machine implementations, the split candidates for each continuous
feature are typically the unique values for the feature. Some implementations sort the feature
values and then use the ordered unique values as split candidates for faster tree calculations.

Finding ordered unique feature values is computationally intensive for large distributed
datasets. One can get an approximate set of split candidates by performing a quantile calculation
over a sampled fraction of the data. The ordered splits create "bins" and the maximum number of such
bins can be specified using the `maxBins` parameters.

Note that the number of bins cannot be greater than the number of instances `$N$` (a rare scenario
since the default `maxBins` value is 100). The tree algorithm automatically reduces the number of
bins if the condition is not satisfied.

**Categorical features**

For `$M$` categorical feature values, one could come up with `$2^(M-1)-1$` split candidates. For
binary classification, we can reduce the number of split candidates to `$M-1$` by ordering the
categorical feature values by the proportion of labels falling in one of the two classes (see
Section 9.2.4 in
[Elements of Statistical Machine Learning](http://statweb.stanford.edu/~tibs/ElemStatLearn/) for
details). For example, for a binary classification problem with one categorical feature with three
categories A, B and C with corresponding proportion of label 1 as 0.2, 0.6 and 0.4, the categorical
features are ordered as A followed by C followed B or A, B, C. The two split candidates are A \| C, B
and A , B \| C where \| denotes the split. A similar heuristic is used for multiclass classification
when `$2^(M-1)-1$` is greater than the number of bins -- the impurity for each categorical feature value
is used for ordering.

### Stopping rule

The recursive tree construction is stopped at a node when one of the two conditions is met:

1. The node depth is equal to the `maxDepth` training parameter
2. No split candidate leads to an information gain at the node.

### Max memory requirements

For faster processing, the decision tree algorithm performs simultaneous histogram computations for all nodes at each level of the tree. This could lead to high memory requirements at deeper levels of the tree leading to memory overflow errors. To alleviate this problem, a 'maxMemoryInMB' training parameter is provided which specifies the maximum amount of memory at the workers (twice as much at the master) to be allocated to the histogram computation. The default value is conservatively chosen to be 128 MB to allow the decision algorithm to work in most scenarios. Once the memory requirements for a level-wise computation crosses the `maxMemoryInMB` threshold, the node training tasks at each subsequent level is split into smaller tasks.

### Practical limitations

1. The implemented algorithm reads both sparse and dense data. However, it is not optimized for sparse input.
2. Python is not supported in this release.

## Examples

### Classification

The example below demonstrates how to load a CSV file, parse it as an RDD of `LabeledPoint` and then
perform classification using a decision tree using Gini impurity as an impurity measure and a
maximum tree depth of 5. The training error is calculated to measure the algorithm accuracy.

<div class="codetabs">
<div data-lang="scala">
{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Gini

// Load and parse the data file
val data = sc.textFile("data/mllib/sample_tree_data.csv")
val parsedData = data.map { line =>
  val parts = line.split(',').map(_.toDouble)
  LabeledPoint(parts(0), Vectors.dense(parts.tail))
}

// Run training algorithm to build the model
val maxDepth = 5
val model = DecisionTree.train(parsedData, Classification, Gini, maxDepth)

// Evaluate model on training examples and compute training error
val labelAndPreds = parsedData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / parsedData.count
println("Training Error = " + trainErr)
{% endhighlight %}
</div>
</div>

### Regression

The example below demonstrates how to load a CSV file, parse it as an RDD of `LabeledPoint` and then
perform regression using a decision tree using variance as an impurity measure and a maximum tree
depth of 5. The Mean Squared Error (MSE) is computed at the end to evaluate
[goodness of fit](http://en.wikipedia.org/wiki/Goodness_of_fit).

<div class="codetabs">
<div data-lang="scala">
{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Variance

// Load and parse the data file
val data = sc.textFile("data/mllib/sample_tree_data.csv")
val parsedData = data.map { line =>
  val parts = line.split(',').map(_.toDouble)
  LabeledPoint(parts(0), Vectors.dense(parts.tail))
}

// Run training algorithm to build the model
val maxDepth = 5
val model = DecisionTree.train(parsedData, Regression, Variance, maxDepth)

// Evaluate model on training examples and compute training error
val valuesAndPreds = parsedData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2)}.mean()
println("training Mean Squared Error = " + MSE)
{% endhighlight %}
</div>
</div>
