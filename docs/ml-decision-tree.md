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

The Pipelines API for Decision Trees offers a bit more functionality than the original API.  In particular, for classification, users can get the predicted probability of each class (a.k.a. class conditional probabilities).

Ensembles of trees (Random Forests and Gradient-Boosted Trees) are described in the [Ensembles guide](ml-ensembles.html).

# Inputs and Outputs

We list the input and output (prediction) column types here.
All output columns are optional; to exclude an output column, set its corresponding Param to an empty string.

## Input Columns

<table class="table">
  <thead>
    <tr>
      <th align="left">Param name</th>
      <th align="left">Type(s)</th>
      <th align="left">Default</th>
      <th align="left">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>labelCol</td>
      <td>Double</td>
      <td>"label"</td>
      <td>Label to predict</td>
    </tr>
    <tr>
      <td>featuresCol</td>
      <td>Vector</td>
      <td>"features"</td>
      <td>Feature vector</td>
    </tr>
  </tbody>
</table>

## Output Columns

<table class="table">
  <thead>
    <tr>
      <th align="left">Param name</th>
      <th align="left">Type(s)</th>
      <th align="left">Default</th>
      <th align="left">Description</th>
      <th align="left">Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>predictionCol</td>
      <td>Double</td>
      <td>"prediction"</td>
      <td>Predicted label</td>
      <td></td>
    </tr>
    <tr>
      <td>rawPredictionCol</td>
      <td>Vector</td>
      <td>"rawPrediction"</td>
      <td>Vector of length # classes, with the counts of training instance labels at the tree node which makes the prediction</td>
      <td>Classification only</td>
    </tr>
    <tr>
      <td>probabilityCol</td>
      <td>Vector</td>
      <td>"probability"</td>
      <td>Vector of length # classes equal to rawPrediction normalized to a multinomial distribution</td>
      <td>Classification only</td>
    </tr>
  </tbody>
</table>

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
import org.apache.spark.ml.feature.{StringIndexer, IndexToString, VectorIndexer}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.util.MLUtils

// Load and parse the data file, converting it to a DataFrame.
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").toDF()

// Index labels, adding metadata to the label column.
// Fit on whole dataset to include all labels in index.
val labelIndexer = new StringIndexer()
  .setInputCol("label")
  .setOutputCol("indexedLabel")
  .fit(data)
// Automatically identify categorical features, and index them.
val featureIndexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexedFeatures")
  .setMaxCategories(4) // features with > 4 distinct values are treated as continuous
  .fit(data)

// Split the data into training and test sets (30% held out for testing)
val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

// Train a DecisionTree model.
val dt = new DecisionTreeClassifier()
  .setLabelCol("indexedLabel")
  .setFeaturesCol("indexedFeatures")

// Convert indexed labels back to original labels.
val labelConverter = new IndexToString()
  .setInputCol("prediction")
  .setOutputCol("predictedLabel")
  .setLabels(labelIndexer.labels)

// Chain indexers and tree in a Pipeline
val pipeline = new Pipeline()
  .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

// Train model.  This also runs the indexers.
val model = pipeline.fit(trainingData)

// Make predictions.
val predictions = model.transform(testData)

// Select example rows to display.
predictions.select("predictedLabel", "label", "features").show(5)

// Select (prediction, true label) and compute test error
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("indexedLabel")
  .setPredictionCol("prediction")
  .setMetricName("precision")
val accuracy = evaluator.evaluate(predictions)
println("Test Error = " + (1.0 - accuracy))

val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
println("Learned classification tree model:\n" + treeModel.toDebugString)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

More details on parameters can be found in the [Java API documentation](api/java/org/apache/spark/ml/classification/DecisionTreeClassifier.html).

{% highlight java %}
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;

// Load and parse the data file, converting it to a DataFrame.
RDD<LabeledPoint> rdd = MLUtils.loadLibSVMFile(sc.sc(), "data/mllib/sample_libsvm_data.txt");
DataFrame data = jsql.createDataFrame(rdd, LabeledPoint.class);

// Index labels, adding metadata to the label column.
// Fit on whole dataset to include all labels in index.
StringIndexerModel labelIndexer = new StringIndexer()
  .setInputCol("label")
  .setOutputCol("indexedLabel")
  .fit(data);
// Automatically identify categorical features, and index them.
VectorIndexerModel featureIndexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexedFeatures")
  .setMaxCategories(4) // features with > 4 distinct values are treated as continuous
  .fit(data);

// Split the data into training and test sets (30% held out for testing)
DataFrame[] splits = data.randomSplit(new double[] {0.7, 0.3});
DataFrame trainingData = splits[0];
DataFrame testData = splits[1];

// Train a DecisionTree model.
DecisionTreeClassifier dt = new DecisionTreeClassifier()
  .setLabelCol("indexedLabel")
  .setFeaturesCol("indexedFeatures");

// Convert indexed labels back to original labels.
IndexToString labelConverter = new IndexToString()
  .setInputCol("prediction")
  .setOutputCol("predictedLabel")
  .setLabels(labelIndexer.labels());

// Chain indexers and tree in a Pipeline
Pipeline pipeline = new Pipeline()
  .setStages(new PipelineStage[] {labelIndexer, featureIndexer, dt, labelConverter});

// Train model.  This also runs the indexers.
PipelineModel model = pipeline.fit(trainingData);

// Make predictions.
DataFrame predictions = model.transform(testData);

// Select example rows to display.
predictions.select("predictedLabel", "label", "features").show(5);

// Select (prediction, true label) and compute test error
MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("indexedLabel")
  .setPredictionCol("prediction")
  .setMetricName("precision");
double accuracy = evaluator.evaluate(predictions);
System.out.println("Test Error = " + (1.0 - accuracy));

DecisionTreeClassificationModel treeModel =
  (DecisionTreeClassificationModel)(model.stages()[2]);
System.out.println("Learned classification tree model:\n" + treeModel.toDebugString());
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

More details on parameters can be found in the [Python API documentation](api/python/pyspark.ml.html#pyspark.ml.classification.DecisionTreeClassifier).

{% highlight python %}
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.util import MLUtils

# Load and parse the data file, converting it to a DataFrame.
data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").toDF()

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)
# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# Train a DecisionTree model.
dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

# Chain indexers and tree in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "indexedLabel", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="precision")
accuracy = evaluator.evaluate(predictions)
print "Test Error = %g" % (1.0 - accuracy)

treeModel = model.stages[2]
print treeModel # summary only
{% endhighlight %}
</div>

</div>


## Regression

The following examples load a dataset in LibSVM format, split it into training and test sets, train on the first dataset, and then evaluate on the held-out test set.
We use a feature transformer to index categorical features, adding metadata to the `DataFrame` which the Decision Tree algorithm can recognize.

<div class="codetabs">
<div data-lang="scala" markdown="1">

More details on parameters can be found in the [Scala API documentation](api/scala/index.html#org.apache.spark.ml.regression.DecisionTreeRegressor).

{% highlight scala %}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.mllib.util.MLUtils

// Load and parse the data file, converting it to a DataFrame.
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").toDF()

// Automatically identify categorical features, and index them.
// Here, we treat features with > 4 distinct values as continuous.
val featureIndexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexedFeatures")
  .setMaxCategories(4)
  .fit(data)

// Split the data into training and test sets (30% held out for testing)
val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

// Train a DecisionTree model.
val dt = new DecisionTreeRegressor()
  .setLabelCol("label")
  .setFeaturesCol("indexedFeatures")

// Chain indexer and tree in a Pipeline
val pipeline = new Pipeline()
  .setStages(Array(featureIndexer, dt))

// Train model.  This also runs the indexer.
val model = pipeline.fit(trainingData)

// Make predictions.
val predictions = model.transform(testData)

// Select example rows to display.
predictions.select("prediction", "label", "features").show(5)

// Select (prediction, true label) and compute test error
val evaluator = new RegressionEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("rmse")
val rmse = evaluator.evaluate(predictions)
println("Root Mean Squared Error (RMSE) on test data = " + rmse)

val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
println("Learned regression tree model:\n" + treeModel.toDebugString)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

More details on parameters can be found in the [Java API documentation](api/java/org/apache/spark/ml/regression/DecisionTreeRegressor.html).

{% highlight java %}
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;

// Load and parse the data file, converting it to a DataFrame.
RDD<LabeledPoint> rdd = MLUtils.loadLibSVMFile(sc.sc(), "data/mllib/sample_libsvm_data.txt");
DataFrame data = jsql.createDataFrame(rdd, LabeledPoint.class);

// Automatically identify categorical features, and index them.
// Set maxCategories so features with > 4 distinct values are treated as continuous.
VectorIndexerModel featureIndexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexedFeatures")
  .setMaxCategories(4)
  .fit(data);

// Split the data into training and test sets (30% held out for testing)
DataFrame[] splits = data.randomSplit(new double[] {0.7, 0.3});
DataFrame trainingData = splits[0];
DataFrame testData = splits[1];

// Train a DecisionTree model.
DecisionTreeRegressor dt = new DecisionTreeRegressor()
  .setFeaturesCol("indexedFeatures");

// Chain indexer and tree in a Pipeline
Pipeline pipeline = new Pipeline()
  .setStages(new PipelineStage[] {featureIndexer, dt});

// Train model.  This also runs the indexer.
PipelineModel model = pipeline.fit(trainingData);

// Make predictions.
DataFrame predictions = model.transform(testData);

// Select example rows to display.
predictions.select("label", "features").show(5);

// Select (prediction, true label) and compute test error
RegressionEvaluator evaluator = new RegressionEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("rmse");
double rmse = evaluator.evaluate(predictions);
System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

DecisionTreeRegressionModel treeModel =
  (DecisionTreeRegressionModel)(model.stages()[1]);
System.out.println("Learned regression tree model:\n" + treeModel.toDebugString());
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

More details on parameters can be found in the [Python API documentation](api/python/pyspark.ml.html#pyspark.ml.regression.DecisionTreeRegressor).

{% highlight python %}
from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.util import MLUtils

# Load and parse the data file, converting it to a DataFrame.
data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").toDF()

# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# Train a DecisionTree model.
dt = DecisionTreeRegressor(featuresCol="indexedFeatures")

# Chain indexer and tree in a Pipeline
pipeline = Pipeline(stages=[featureIndexer, dt])

# Train model.  This also runs the indexer.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "label", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print "Root Mean Squared Error (RMSE) on test data = %g" % rmse

treeModel = model.stages[1]
print treeModel # summary only
{% endhighlight %}
</div>

</div>
