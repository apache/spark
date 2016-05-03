---
layout: global
title: Decision Trees - spark.mllib
displayTitle: Decision Trees - spark.mllib
---

* Table of contents
{:toc}

[Decision trees](http://en.wikipedia.org/wiki/Decision_tree_learning)
and their ensembles are popular methods for the machine learning tasks of
classification and regression. Decision trees are widely used since they are easy to interpret,
handle categorical features, extend to the multiclass classification setting, do not require
feature scaling, and are able to capture non-linearities and feature interactions. Tree ensemble
algorithms such as random forests and boosting are among the top performers for classification and
regression tasks.

`spark.mllib` supports decision trees for binary and multiclass classification and for regression,
using both continuous and categorical features. The implementation partitions data by rows,
allowing distributed training with millions of instances.

Ensembles of trees (Random Forests and Gradient-Boosted Trees) are described in the [Ensembles guide](mllib-ensembles.html).

## Basic algorithm

The decision tree is a greedy algorithm that performs a recursive binary partitioning of the feature
space.  The tree predicts the same label for each bottommost (leaf) partition.
Each partition is chosen greedily by selecting the *best split* from a set of possible splits,
in order to maximize the information gain at a tree node. In other words, the split chosen at each
tree node is chosen from the set `$\underset{s}{\operatorname{argmax}} IG(D,s)$` where `$IG(D,s)$`
is the information gain when a split `$s$` is applied to a dataset `$D$`.

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
	  <td>$\sum_{i=1}^{C} f_i(1-f_i)$</td><td>$f_i$ is the frequency of label $i$ at a node and $C$ is the number of unique labels.</td>
    </tr>
    <tr>
      <td>Entropy</td>
	  <td>Classification</td>
	  <td>$\sum_{i=1}^{C} -f_ilog(f_i)$</td><td>$f_i$ is the frequency of label $i$ at a node and $C$ is the number of unique labels.</td>
    </tr>
    <tr>
      <td>Variance</td>
	  <td>Regression</td>
     <td>$\frac{1}{N} \sum_{i=1}^{N} (y_i - \mu)^2$</td><td>$y_i$ is label for an instance,
	  $N$ is the number of instances and $\mu$ is the mean given by $\frac{1}{N} \sum_{i=1}^N y_i$.</td>
    </tr>
  </tbody>
</table>

The *information gain* is the difference between the parent node impurity and the weighted sum of
the two child node impurities. Assuming that a split $s$ partitions the dataset `$D$` of size `$N$`
into two datasets `$D_{left}$` and `$D_{right}$` of sizes `$N_{left}$` and `$N_{right}$`,
respectively, the information gain is:

`$IG(D,s) = Impurity(D) - \frac{N_{left}}{N} Impurity(D_{left}) - \frac{N_{right}}{N} Impurity(D_{right})$`

### Split candidates

**Continuous features**

For small datasets in single-machine implementations, the split candidates for each continuous
feature are typically the unique values for the feature. Some implementations sort the feature
values and then use the ordered unique values as split candidates for faster tree calculations.

Sorting feature values is expensive for large distributed datasets.
This implementation computes an approximate set of split candidates by performing a quantile
calculation over a sampled fraction of the data.
The ordered splits create "bins" and the maximum number of such
bins can be specified using the `maxBins` parameter.

Note that the number of bins cannot be greater than the number of instances `$N$` (a rare scenario
since the default `maxBins` value is 32). The tree algorithm automatically reduces the number of
bins if the condition is not satisfied.

**Categorical features**

For a categorical feature with `$M$` possible values (categories), one could come up with
`$2^{M-1}-1$` split candidates. For binary (0/1) classification and regression,
we can reduce the number of split candidates to `$M-1$` by ordering the
categorical feature values by the average label. (See Section 9.2.4 in
[Elements of Statistical Machine Learning](http://statweb.stanford.edu/~tibs/ElemStatLearn/) for
details.) For example, for a binary classification problem with one categorical feature with three
categories A, B and C whose corresponding proportions of label 1 are 0.2, 0.6 and 0.4, the categorical
features are ordered as A, C, B. The two split candidates are A \| C, B
and A , C \| B where \| denotes the split.

In multiclass classification, all `$2^{M-1}-1$` possible splits are used whenever possible.
When `$2^{M-1}-1$` is greater than the `maxBins` parameter, we use a (heuristic) method
similar to the method used for binary classification and regression.
The `$M$` categorical feature values are ordered by impurity,
and the resulting `$M-1$` split candidates are considered.

### Stopping rule

The recursive tree construction is stopped at a node when one of the following conditions is met:

1. The node depth is equal to the `maxDepth` training parameter.
2. No split candidate leads to an information gain greater than `minInfoGain`.
3. No split candidate produces child nodes which each have at least `minInstancesPerNode` training instances.

## Usage tips

We include a few guidelines for using decision trees by discussing the various parameters.
The parameters are listed below roughly in order of descending importance.  New users should mainly consider the "Problem specification parameters" section and the `maxDepth` parameter.

### Problem specification parameters

These parameters describe the problem you want to solve and your dataset.
They should be specified and do not require tuning.

* **`algo`**: Type of decision tree, either `Classification` or `Regression`.

* **`numClasses`**: Number of classes (for `Classification` only).

* **`categoricalFeaturesInfo`**: Specifies which features are categorical and how many categorical values each of those features can take.  This is given as a map from feature indices to feature arity (number of categories).  Any features not in this map are treated as continuous.
  * For example, `Map(0 -> 2, 4 -> 10)` specifies that feature `0` is binary (taking values `0` or `1`) and that feature `4` has 10 categories (values `{0, 1, ..., 9}`).  Note that feature indices are 0-based: features `0` and `4` are the 1st and 5th elements of an instance's feature vector.
  * Note that you do not have to specify `categoricalFeaturesInfo`.  The algorithm will still run and may get reasonable results.  However, performance should be better if categorical features are properly designated.

### Stopping criteria

These parameters determine when the tree stops building (adding new nodes).
When tuning these parameters, be careful to validate on held-out test data to avoid overfitting.

* **`maxDepth`**: Maximum depth of a tree.  Deeper trees are more expressive (potentially allowing higher accuracy), but they are also more costly to train and are more likely to overfit.

* **`minInstancesPerNode`**: For a node to be split further, each of its children must receive at least this number of training instances.  This is commonly used with [RandomForest](api/scala/index.html#org.apache.spark.mllib.tree.RandomForest) since those are often trained deeper than individual trees.

* **`minInfoGain`**: For a node to be split further, the split must improve at least this much (in terms of information gain).

### Tunable parameters

These parameters may be tuned.  Be careful to validate on held-out test data when tuning in order to avoid overfitting.

* **`maxBins`**: Number of bins used when discretizing continuous features.
  * Increasing `maxBins` allows the algorithm to consider more split candidates and make fine-grained split decisions.  However, it also increases computation and communication.
  * Note that the `maxBins` parameter must be at least the maximum number of categories `$M$` for any categorical feature.

* **`maxMemoryInMB`**: Amount of memory to be used for collecting sufficient statistics.
  * The default value is conservatively chosen to be 256 MB to allow the decision algorithm to work in most scenarios.  Increasing `maxMemoryInMB` can lead to faster training (if the memory is available) by allowing fewer passes over the data.  However, there may be decreasing returns as `maxMemoryInMB` grows since the amount of communication on each iteration can be proportional to `maxMemoryInMB`.
  * *Implementation details*: For faster processing, the decision tree algorithm collects statistics about groups of nodes to split (rather than 1 node at a time).  The number of nodes which can be handled in one group is determined by the memory requirements (which vary per features).  The `maxMemoryInMB` parameter specifies the memory limit in terms of megabytes which each worker can use for these statistics.

* **`subsamplingRate`**: Fraction of the training data used for learning the decision tree.  This parameter is most relevant for training ensembles of trees (using [`RandomForest`](api/scala/index.html#org.apache.spark.mllib.tree.RandomForest) and [`GradientBoostedTrees`](api/scala/index.html#org.apache.spark.mllib.tree.GradientBoostedTrees)), where it can be useful to subsample the original data.  For training a single decision tree, this parameter is less useful since the number of training instances is generally not the main constraint.

* **`impurity`**: Impurity measure (discussed above) used to choose between candidate splits.  This measure must match the `algo` parameter.

### Caching and checkpointing

MLlib 1.2 adds several features for scaling up to larger (deeper) trees and tree ensembles.  When `maxDepth` is set to be large, it can be useful to turn on node ID caching and checkpointing.  These parameters are also useful for [RandomForest](api/scala/index.html#org.apache.spark.mllib.tree.RandomForest) when `numTrees` is set to be large.

* **`useNodeIdCache`**: If this is set to true, the algorithm will avoid passing the current model (tree or trees) to executors on each iteration.
  * This can be useful with deep trees (speeding up computation on workers) and for large Random Forests (reducing communication on each iteration).
  * *Implementation details*: By default, the algorithm communicates the current model to executors so that executors can match training instances with tree nodes.  When this setting is turned on, then the algorithm will instead cache this information.

Node ID caching generates a sequence of RDDs (1 per iteration).  This long lineage can cause performance problems, but checkpointing intermediate RDDs can alleviate those problems.
Note that checkpointing is only applicable when `useNodeIdCache` is set to true.

* **`checkpointDir`**: Directory for checkpointing node ID cache RDDs.

* **`checkpointInterval`**: Frequency for checkpointing node ID cache RDDs.  Setting this too low will cause extra overhead from writing to HDFS; setting this too high can cause problems if executors fail and the RDD needs to be recomputed.

## Scaling

Computation scales approximately linearly in the number of training instances,
in the number of features, and in the `maxBins` parameter.
Communication scales approximately linearly in the number of features and in `maxBins`.

The implemented algorithm reads both sparse and dense data. However, it is not optimized for sparse input.

## Examples

### Classification

The example below demonstrates how to load a
[LIBSVM data file](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/),
parse it as an RDD of `LabeledPoint` and then
perform classification using a decision tree with Gini impurity as an impurity measure and a
maximum tree depth of 5. The test error is calculated to measure the algorithm accuracy.

<div class="codetabs">

<div data-lang="scala" markdown="1">
Refer to the [`DecisionTree` Scala docs](api/scala/index.html#org.apache.spark.mllib.tree.DecisionTree) and [`DecisionTreeModel` Scala docs](api/scala/index.html#org.apache.spark.mllib.tree.model.DecisionTreeModel) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/DecisionTreeClassificationExample.scala %}
</div>

<div data-lang="java" markdown="1">
Refer to the [`DecisionTree` Java docs](api/java/org/apache/spark/mllib/tree/DecisionTree.html) and [`DecisionTreeModel` Java docs](api/java/org/apache/spark/mllib/tree/model/DecisionTreeModel.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaDecisionTreeClassificationExample.java %}
</div>

<div data-lang="python" markdown="1">
Refer to the [`DecisionTree` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.tree.DecisionTree) and [`DecisionTreeModel` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.tree.DecisionTreeModel) for more details on the API.

{% include_example python/mllib/decision_tree_classification_example.py %}
</div>

</div>

### Regression

The example below demonstrates how to load a
[LIBSVM data file](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/),
parse it as an RDD of `LabeledPoint` and then
perform regression using a decision tree with variance as an impurity measure and a maximum tree
depth of 5. The Mean Squared Error (MSE) is computed at the end to evaluate
[goodness of fit](http://en.wikipedia.org/wiki/Goodness_of_fit).

<div class="codetabs">

<div data-lang="scala" markdown="1">
Refer to the [`DecisionTree` Scala docs](api/scala/index.html#org.apache.spark.mllib.tree.DecisionTree) and [`DecisionTreeModel` Scala docs](api/scala/index.html#org.apache.spark.mllib.tree.model.DecisionTreeModel) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/DecisionTreeRegressionExample.scala %}
</div>

<div data-lang="java" markdown="1">
Refer to the [`DecisionTree` Java docs](api/java/org/apache/spark/mllib/tree/DecisionTree.html) and [`DecisionTreeModel` Java docs](api/java/org/apache/spark/mllib/tree/model/DecisionTreeModel.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaDecisionTreeRegressionExample.java %}
</div>

<div data-lang="python" markdown="1">
Refer to the [`DecisionTree` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.tree.DecisionTree) and [`DecisionTreeModel` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.tree.DecisionTreeModel) for more details on the API.

{% include_example python/mllib/decision_tree_regression_example.py %}
</div>

</div>
