---
layout: global
title: Decision Trees - SparkML
displayTitle: <a href="ml-guide.html">ML</a> - Decision Trees
---

**Table of Contents**

* This will become a table of contents (this text will be scraped).
{:toc}


# Overview

[Decision trees](http://en.wikipedia.org/wiki/Decision_tree_learning)
and their ensembles are popular methods for the machine learning tasks of
classification and regression. Decision trees are widely used since they are easy to interpret,
handle categorical features, extend to the multiclass classification setting, do not require
feature scaling, and are able to capture non-linearities and feature interactions. Tree ensemble
algorithms such as random forests and boosting are among the top performers for classification and
regression tasks.

MLlib supports decision trees for binary and multiclass classification and for regression,
using both continuous and categorical features. The implementation partitions data by rows,
allowing distributed training with millions or even billions of instances.

Users can find more information about the decision tree algorithm in the [MLlib Decision Tree guide](mllib-decision-tree.html).  In this section, we demonstrate the Pipelines API for Decision Trees.

Ensembles of trees (Random Forests and Gradient-Boosted Trees) are described in the [Ensembles guide](ml-ensembles.html).


# Examples

The below examples demonstrate the Pipelines API for Decision Trees. The main differences between this API and the [original MLlib Decision Tree API](mllib-decision-tree.html) are:

* support for ML Pipelines
* separation of Decision Trees for classification vs. regression
* use of DataFrame metadata to distinguish continuous and categorical features



## Classification

The following examples load a dataset in LibSVM format, split it into training and test sets, train on the first dataset, and then evaluate on the held-out test set.
We use two feature transformers to prepare the data; these help index categories for the label and categorical features, adding metadata to the `DataFrame` which the Decision Tree algorithm can recognize.

<div class="codetabs">
<div data-lang="scala" markdown="1">

More details on parameters can be found in the [Scala API documentation](api/scala/index.html#org.apache.spark.ml.classification.DecisionTreeClassifier).

{% highlight scala %}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerInverse, VectorIndexer}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.Row

// Load and parse the data file, converting it to a DataFrame.
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").toDF()
// Split the data into training and test sets (30% held out for testing)
val splits = data.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))

// Index labels, adding metadata to the label column.
val labelIndexer = new StringIndexer()
  .setInputCol("label")
  .setOutputCol("indexedLabel")

// Automatically identify categorical features, and index them.
val featureIndexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexedFeatures")
  .setMaxCategories(4) // features with > 4 distinct values are treated as continuous

// Train a DecisionTree model.
val dt = new DecisionTreeClassifier()
  .setLabelCol("indexedLabel") // "label" is the default
  .setFeaturesCol("indexedFeatures") // "features" is the default

// Convert indexed labels back to original labels.
val labelConverter = new StringIndexerInverse()
  .setInputCol("prediction")
  .setOutputCol("predictedLabel")

// Chain indexers and tree in a Pipeline
val pipeline = new Pipeline()
  .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

// Train model.  This also runs the indexers.
val model = pipeline.fit(trainingData)

// Make predictions.
val predictions = model.transform(testData)

// Select (prediction, true label) and compute test error
val labelAndPreds = predictions.select("predictedLabel", "label").map {
  case Row(pred: Double, label: Double) =>
    (pred, label)
}

val testErr = new MulticlassClassificationEvaluator()
  .setPredictionCol("predictedLabel")
  .setLabelCol("label")
  .setMetricName("precision")

val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
println("Test Error = " + testErr)
println("Learned classification tree model:\n" + model.toDebugString)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

More details on parameters can be found in the [Java API documentation](api/java/org/apache/spark/ml/classification/DecisionTreeClassifier.html).

{% highlight java %}
import scala.Tuple2;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerInverse;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.mllib.util.MLUtils;

// Load and parse the data file, converting it to a DataFrame.
String datapath = "data/mllib/sample_libsvm_data.txt";
DataFrame data = MLUtils.loadLibSVMFile(sc.sc(), datapath).toDF();
// Split the data into training and test sets (30% held out for testing)
JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
JavaRDD<LabeledPoint> trainingData = splits[0];
JavaRDD<LabeledPoint> testData = splits[1];

// Automatically identify categorical features, and index them.
VectorIndexer indexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("scaledFeatures")
  .setMaxCategories(4); // features with > 4 distinct values are treated as continuous

// Train a DecisionTree model.
DecisionTreeClassifier dt = new DecisionTreeClassifier()
  .setFeaturesCol("scaledFeatures"); // "features" is the default

// Chain indexer and tree in a Pipeline
Pipeline pipeline = new Pipeline()
  .setStages(new PipelineStage[]{indexer, dt});

// Train model.  This also runs the indexer.
PipelineModel model = pipeline.fit(trainingData);

// Make predictions.
DataFrame predictions = model.transform(testData);

// Select (prediction, true label) and compute test error
JavaPairRDD<Double, Double> labelAndPreds =
  predictions.select("label", "prediction")
    .mapToPair(new PairFunction<Row, Double, Double>() {
      @Override
      public Tuple2<Double, Double> call(Row row) {
        return new Tuple2<Double, Double>(row.getDouble(0), row.getDouble(1));
      }
    });

// Evaluate model on test instances and compute test error
Double testErr =
  1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
    @Override
    public Boolean call(Tuple2<Double, Double> pl) {
      return !pl._1().equals(pl._2());
    }
  }).count() / testData.count();
System.out.println("Test Error: " + testErr);
System.out.println("Learned classification tree model:\n" + model.toDebugString());
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

More details on parameters can be found in the [Python API documentation](api/python/pyspark.ml.html#pyspark.ml.classification.DecisionTreeClassifier).

{% highlight python %}
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils

# Load and parse the data file, converting it to a DataFrame.
data = MLUtils.loadLibSVMFile(sc, 'data/mllib/sample_libsvm_data.txt').toDF()
# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# Automatically identify categorical features, and index them.
# Features with > 4 distinct values are treated as continuous
indexer = VectorIndexer(inputCol="features",
                        outputCol="scaledFeatures",
                        maxCategories=4)

# Train a DecisionTree model.
dt = DecisionTreeClassifier(featuresCol="scaledFeatures") // "features" is the default

# Chain indexer and tree in a Pipeline
pipeline = Pipeline(stages=[indexer, dt])

# Train model.  This also runs the indexer.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select (prediction, true label) and compute test error
labelAndPreds = predictions.select("prediction", "label").map(lambda row: (row[0], row[1]))
testErr = labelAndPreds.filter(lambda (v, p): v != p).count() / float(testData.count())

print('Test Error = %g' % testErr)
print('Learned classification tree model:')
print(model.toDebugString())
{% endhighlight %}
</div>

</div>


## Regression
