---
layout: global
title: Decision Tree - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Decision Tree
---

* Table of contents
{:toc}

[Decision trees](http://en.wikipedia.org/wiki/Decision_tree_learning)
and their ensembles are popular methods for the machine learning tasks of
classification and regression. Decision trees are widely used since they are easy to interpret,
handle categorical features, extend to the multiclass classification setting, do not require
feature scaling and are able to capture nonlinearities and feature interactions. Tree ensemble
algorithms such as random forests and boosting are among the top performers for classification and
regression tasks.

MLlib supports decision trees for binary and multiclass classification and for regression,
using both continuous and categorical features. The implementation partitions data by rows,
allowing distributed training with millions of instances.

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
since the default `maxBins` value is 100). The tree algorithm automatically reduces the number of
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

The recursive tree construction is stopped at a node when one of the two conditions is met:

1. The node depth is equal to the `maxDepth` training parameter.
2. No split candidate leads to an information gain at the node.

## Implementation details

### Max memory requirements

For faster processing, the decision tree algorithm performs simultaneous histogram computations for
all nodes at each level of the tree. This could lead to high memory requirements at deeper levels
of the tree, potentially leading to memory overflow errors. To alleviate this problem, a `maxMemoryInMB`
training parameter specifies the maximum amount of memory at the workers (twice as much at the
master) to be allocated to the histogram computation. The default value is conservatively chosen to
be 128 MB to allow the decision algorithm to work in most scenarios. Once the memory requirements
for a level-wise computation cross the `maxMemoryInMB` threshold, the node training tasks at each
subsequent level are split into smaller tasks.

Note that, if you have a large amount of memory, increasing `maxMemoryInMB` can lead to faster
training by requiring fewer passes over the data.

### Binning feature values

Increasing `maxBins` allows the algorithm to consider more split candidates and make fine-grained
split decisions.  However, it also increases computation and communication.

Note that the `maxBins` parameter must be at least the maximum number of categories `$M$` for
any categorical feature.

### Scaling

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
maximum tree depth of 5. The training error is calculated to measure the algorithm accuracy.

<div class="codetabs">

<div data-lang="scala">
{% highlight scala %}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils

// Load and parse the data file.
// Cache the data since we will use it again to compute training error.
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").cache()

// Train a DecisionTree model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "gini"
val maxDepth = 5
val maxBins = 100

val model = DecisionTree.trainClassifier(data, numClasses, categoricalFeaturesInfo, impurity,
  maxDepth, maxBins)

// Evaluate model on training instances and compute training error
val labelAndPreds = data.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / data.count
println("Training Error = " + trainErr)
println("Learned classification tree model:\n" + model)
{% endhighlight %}
</div>

<div data-lang="java">
{% highlight java %}
import java.util.HashMap;
import scala.Tuple2;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkConf;

SparkConf sparkConf = new SparkConf().setAppName("JavaDecisionTree");
JavaSparkContext sc = new JavaSparkContext(sparkConf);

// Load and parse the data file.
// Cache the data since we will use it again to compute training error.
String datapath = "data/mllib/sample_libsvm_data.txt";
JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), datapath).toJavaRDD().cache();

// Set parameters.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
Integer numClasses = 2;
HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
String impurity = "gini";
Integer maxDepth = 5;
Integer maxBins = 100;

// Train a DecisionTree model for classification.
final DecisionTreeModel model = DecisionTree.trainClassifier(data, numClasses,
  categoricalFeaturesInfo, impurity, maxDepth, maxBins);

// Evaluate model on training instances and compute training error
JavaPairRDD<Double, Double> predictionAndLabel =
  data.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
    @Override public Tuple2<Double, Double> call(LabeledPoint p) {
      return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
    }
  });
Double trainErr =
  1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
    @Override public Boolean call(Tuple2<Double, Double> pl) {
      return !pl._1().equals(pl._2());
    }
  }).count() / data.count();
System.out.println("Training error: " + trainErr);
System.out.println("Learned classification tree model:\n" + model);
{% endhighlight %}
</div>

<div data-lang="python">
{% highlight python %}
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.util import MLUtils

# Load and parse the data file into an RDD of LabeledPoint.
# Cache the data since we will use it again to compute training error.
data = MLUtils.loadLibSVMFile(sc, 'data/mllib/sample_libsvm_data.txt').cache()

# Train a DecisionTree model.
#  Empty categoricalFeaturesInfo indicates all features are continuous.
model = DecisionTree.trainClassifier(data, numClasses=2, categoricalFeaturesInfo={},
                                     impurity='gini', maxDepth=5, maxBins=100)

# Evaluate model on training instances and compute training error
predictions = model.predict(data.map(lambda x: x.features))
labelsAndPredictions = data.map(lambda lp: lp.label).zip(predictions)
trainErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(data.count())
print('Training Error = ' + str(trainErr))
print('Learned classification tree model:')
print(model)
{% endhighlight %}

Note: When making predictions for a dataset, it is more efficient to do batch prediction rather
than separately calling `predict` on each data point.  This is because the Python code makes calls
to an underlying `DecisionTree` model in Scala.
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

<div data-lang="scala">
{% highlight scala %}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils

// Load and parse the data file.
// Cache the data since we will use it again to compute training error.
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").cache()

// Train a DecisionTree model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "variance"
val maxDepth = 5
val maxBins = 100

val model = DecisionTree.trainRegressor(data, categoricalFeaturesInfo, impurity,
  maxDepth, maxBins)

// Evaluate model on training instances and compute training error
val labelsAndPredictions = data.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val trainMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
println("Training Mean Squared Error = " + trainMSE)
println("Learned regression tree model:\n" + model)
{% endhighlight %}
</div>

<div data-lang="java">
{% highlight java %}
import java.util.HashMap;
import scala.Tuple2;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkConf;

// Load and parse the data file.
// Cache the data since we will use it again to compute training error.
String datapath = "data/mllib/sample_libsvm_data.txt";
JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), datapath).toJavaRDD().cache();

SparkConf sparkConf = new SparkConf().setAppName("JavaDecisionTree");
JavaSparkContext sc = new JavaSparkContext(sparkConf);

// Set parameters.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
String impurity = "variance";
Integer maxDepth = 5;
Integer maxBins = 100;

// Train a DecisionTree model.
final DecisionTreeModel model = DecisionTree.trainRegressor(data,
  categoricalFeaturesInfo, impurity, maxDepth, maxBins);

// Evaluate model on training instances and compute training error
JavaPairRDD<Double, Double> predictionAndLabel =
  data.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
    @Override public Tuple2<Double, Double> call(LabeledPoint p) {
      return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
    }
  });
Double trainMSE =
  predictionAndLabel.map(new Function<Tuple2<Double, Double>, Double>() {
    @Override public Double call(Tuple2<Double, Double> pl) {
      Double diff = pl._1() - pl._2();
      return diff * diff;
    }
  }).reduce(new Function2<Double, Double, Double>() {
    @Override public Double call(Double a, Double b) {
      return a + b;
    }
  }) / data.count();
System.out.println("Training Mean Squared Error: " + trainMSE);
System.out.println("Learned regression tree model:\n" + model);
{% endhighlight %}
</div>

<div data-lang="python">
{% highlight python %}
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.util import MLUtils

# Load and parse the data file into an RDD of LabeledPoint.
# Cache the data since we will use it again to compute training error.
data = MLUtils.loadLibSVMFile(sc, 'data/mllib/sample_libsvm_data.txt').cache()

# Train a DecisionTree model.
#  Empty categoricalFeaturesInfo indicates all features are continuous.
model = DecisionTree.trainRegressor(data, categoricalFeaturesInfo={},
                                    impurity='variance', maxDepth=5, maxBins=100)

# Evaluate model on training instances and compute training error
predictions = model.predict(data.map(lambda x: x.features))
labelsAndPredictions = data.map(lambda lp: lp.label).zip(predictions)
trainMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() / float(data.count())
print('Training Mean Squared Error = ' + str(trainMSE))
print('Learned regression tree model:')
print(model)
{% endhighlight %}

Note: When making predictions for a dataset, it is more efficient to do batch prediction rather
than separately calling `predict` on each data point.  This is because the Python code makes calls
to an underlying `DecisionTree` model in Scala.
</div>

</div>
