---
layout: global
title: Ensembles
displayTitle: <a href="ml-guide.html">ML</a> - Ensembles
---

**Table of Contents**

* This will become a table of contents (this text will be scraped).
{:toc}

An [ensemble method](http://en.wikipedia.org/wiki/Ensemble_learning)
is a learning algorithm which creates a model composed of a set of other base models.

## Tree Ensembles

The Pipelines API supports two major tree ensemble algorithms: [Random Forests](http://en.wikipedia.org/wiki/Random_forest) and [Gradient-Boosted Trees (GBTs)](http://en.wikipedia.org/wiki/Gradient_boosting).
Both use [MLlib decision trees](ml-decision-tree.html) as their base models.

Users can find more information about ensemble algorithms in the [MLlib Ensemble guide](mllib-ensembles.html).  In this section, we demonstrate the Pipelines API for ensembles.

The main differences between this API and the [original MLlib ensembles API](mllib-ensembles.html) are:
* support for ML Pipelines
* separation of classification vs. regression
* use of DataFrame metadata to distinguish continuous and categorical features
* a bit more functionality for random forests: estimates of feature importance, as well as the predicted probability of each class (a.k.a. class conditional probabilities) for classification.

### Random Forests

[Random forests](http://en.wikipedia.org/wiki/Random_forest)
are ensembles of [decision trees](ml-decision-tree.html).
Random forests combine many decision trees in order to reduce the risk of overfitting.
MLlib supports random forests for binary and multiclass classification and for regression,
using both continuous and categorical features.

This section gives examples of using random forests with the Pipelines API.
For more information on the algorithm, please see the [main MLlib docs on random forests](mllib-ensembles.html).

#### Inputs and Outputs

We list the input and output (prediction) column types here.
All output columns are optional; to exclude an output column, set its corresponding Param to an empty string.

##### Input Columns

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

##### Output Columns (Predictions)

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

#### Example: Classification

The following examples load a dataset in LibSVM format, split it into training and test sets, train on the first dataset, and then evaluate on the held-out test set.
We use two feature transformers to prepare the data; these help index categories for the label and categorical features, adding metadata to the `DataFrame` which the tree-based algorithms can recognize.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/index.html#org.apache.spark.ml.classification.RandomForestClassifier) for more details.

{% highlight scala %}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.RandomForestClassificationModel
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
// Set maxCategories so features with > 4 distinct values are treated as continuous.
val featureIndexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexedFeatures")
  .setMaxCategories(4)
  .fit(data)

// Split the data into training and test sets (30% held out for testing)
val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

// Train a RandomForest model.
val rf = new RandomForestClassifier()
  .setLabelCol("indexedLabel")
  .setFeaturesCol("indexedFeatures")
  .setNumTrees(10)

// Convert indexed labels back to original labels.
val labelConverter = new IndexToString()
  .setInputCol("prediction")
  .setOutputCol("predictedLabel")
  .setLabels(labelIndexer.labels)

// Chain indexers and forest in a Pipeline
val pipeline = new Pipeline()
  .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

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

val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
println("Learned classification forest model:\n" + rfModel.toDebugString)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/classification/RandomForestClassifier.html) for more details.

{% highlight java %}
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
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

// Train a RandomForest model.
RandomForestClassifier rf = new RandomForestClassifier()
  .setLabelCol("indexedLabel")
  .setFeaturesCol("indexedFeatures");

// Convert indexed labels back to original labels.
IndexToString labelConverter = new IndexToString()
  .setInputCol("prediction")
  .setOutputCol("predictedLabel")
  .setLabels(labelIndexer.labels());

// Chain indexers and forest in a Pipeline
Pipeline pipeline = new Pipeline()
  .setStages(new PipelineStage[] {labelIndexer, featureIndexer, rf, labelConverter});

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

RandomForestClassificationModel rfModel =
  (RandomForestClassificationModel)(model.stages()[2]);
System.out.println("Learned classification forest model:\n" + rfModel.toDebugString());
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/pyspark.ml.html#pyspark.ml.classification.RandomForestClassifier) for more details.

{% highlight python %}
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.util import MLUtils

# Load and parse the data file, converting it to a DataFrame.
data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").toDF()

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)
# Automatically identify categorical features, and index them.
# Set maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# Train a RandomForest model.
rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

# Chain indexers and forest in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf])

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

rfModel = model.stages[2]
print rfModel # summary only
{% endhighlight %}
</div>
</div>

#### Example: Regression

The following examples load a dataset in LibSVM format, split it into training and test sets, train on the first dataset, and then evaluate on the held-out test set.
We use a feature transformer to index categorical features, adding metadata to the `DataFrame` which the tree-based algorithms can recognize.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/index.html#org.apache.spark.ml.regression.RandomForestRegressor) for more details.

{% highlight scala %}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.mllib.util.MLUtils

// Load and parse the data file, converting it to a DataFrame.
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").toDF()

// Automatically identify categorical features, and index them.
// Set maxCategories so features with > 4 distinct values are treated as continuous.
val featureIndexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexedFeatures")
  .setMaxCategories(4)
  .fit(data)

// Split the data into training and test sets (30% held out for testing)
val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

// Train a RandomForest model.
val rf = new RandomForestRegressor()
  .setLabelCol("label")
  .setFeaturesCol("indexedFeatures")

// Chain indexer and forest in a Pipeline
val pipeline = new Pipeline()
  .setStages(Array(featureIndexer, rf))

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

val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
println("Learned regression forest model:\n" + rfModel.toDebugString)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/regression/RandomForestRegressor.html) for more details.

{% highlight java %}
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
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

// Train a RandomForest model.
RandomForestRegressor rf = new RandomForestRegressor()
  .setLabelCol("label")
  .setFeaturesCol("indexedFeatures");

// Chain indexer and forest in a Pipeline
Pipeline pipeline = new Pipeline()
  .setStages(new PipelineStage[] {featureIndexer, rf});

// Train model.  This also runs the indexer.
PipelineModel model = pipeline.fit(trainingData);

// Make predictions.
DataFrame predictions = model.transform(testData);

// Select example rows to display.
predictions.select("prediction", "label", "features").show(5);

// Select (prediction, true label) and compute test error
RegressionEvaluator evaluator = new RegressionEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("rmse");
double rmse = evaluator.evaluate(predictions);
System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

RandomForestRegressionModel rfModel =
  (RandomForestRegressionModel)(model.stages()[1]);
System.out.println("Learned regression forest model:\n" + rfModel.toDebugString());
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/pyspark.ml.html#pyspark.ml.regression.RandomForestRegressor) for more details.

{% highlight python %}
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.util import MLUtils

# Load and parse the data file, converting it to a DataFrame.
data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").toDF()

# Automatically identify categorical features, and index them.
# Set maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# Train a RandomForest model.
rf = RandomForestRegressor(featuresCol="indexedFeatures")

# Chain indexer and forest in a Pipeline
pipeline = Pipeline(stages=[featureIndexer, rf])

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

rfModel = model.stages[1]
print rfModel # summary only
{% endhighlight %}
</div>
</div>

### Gradient-Boosted Trees (GBTs)

[Gradient-Boosted Trees (GBTs)](http://en.wikipedia.org/wiki/Gradient_boosting)
are ensembles of [decision trees](ml-decision-tree.html).
GBTs iteratively train decision trees in order to minimize a loss function.
MLlib supports GBTs for binary classification and for regression,
using both continuous and categorical features.

This section gives examples of using GBTs with the Pipelines API.
For more information on the algorithm, please see the [main MLlib docs on GBTs](mllib-ensembles.html).

#### Inputs and Outputs

We list the input and output (prediction) column types here.
All output columns are optional; to exclude an output column, set its corresponding Param to an empty string.

##### Input Columns

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

Note that `GBTClassifier` currently only supports binary labels.

##### Output Columns (Predictions)

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
  </tbody>
</table>

In the future, `GBTClassifier` will also output columns for `rawPrediction` and `probability`, just as `RandomForestClassifier` does.

#### Example: Classification

The following examples load a dataset in LibSVM format, split it into training and test sets, train on the first dataset, and then evaluate on the held-out test set.
We use two feature transformers to prepare the data; these help index categories for the label and categorical features, adding metadata to the `DataFrame` which the tree-based algorithms can recognize.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/index.html#org.apache.spark.ml.classification.GBTClassifier) for more details.

{% highlight scala %}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.classification.GBTClassificationModel
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
// Set maxCategories so features with > 4 distinct values are treated as continuous.
val featureIndexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexedFeatures")
  .setMaxCategories(4)
  .fit(data)

// Split the data into training and test sets (30% held out for testing)
val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

// Train a GBT model.
val gbt = new GBTClassifier()
  .setLabelCol("indexedLabel")
  .setFeaturesCol("indexedFeatures")
  .setMaxIter(10)

// Convert indexed labels back to original labels.
val labelConverter = new IndexToString()
  .setInputCol("prediction")
  .setOutputCol("predictedLabel")
  .setLabels(labelIndexer.labels)

// Chain indexers and GBT in a Pipeline
val pipeline = new Pipeline()
  .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))

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

val gbtModel = model.stages(2).asInstanceOf[GBTClassificationModel]
println("Learned classification GBT model:\n" + gbtModel.toDebugString)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/classification/GBTClassifier.html) for more details.

{% highlight java %}
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.classification.GBTClassificationModel;
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

// Train a GBT model.
GBTClassifier gbt = new GBTClassifier()
  .setLabelCol("indexedLabel")
  .setFeaturesCol("indexedFeatures")
  .setMaxIter(10);

// Convert indexed labels back to original labels.
IndexToString labelConverter = new IndexToString()
  .setInputCol("prediction")
  .setOutputCol("predictedLabel")
  .setLabels(labelIndexer.labels());

// Chain indexers and GBT in a Pipeline
Pipeline pipeline = new Pipeline()
  .setStages(new PipelineStage[] {labelIndexer, featureIndexer, gbt, labelConverter});

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

GBTClassificationModel gbtModel =
  (GBTClassificationModel)(model.stages()[2]);
System.out.println("Learned classification GBT model:\n" + gbtModel.toDebugString());
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/pyspark.ml.html#pyspark.ml.classification.GBTClassifier) for more details.

{% highlight python %}
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.util import MLUtils

# Load and parse the data file, converting it to a DataFrame.
data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").toDF()

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)
# Automatically identify categorical features, and index them.
# Set maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# Train a GBT model.
gbt = GBTClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", maxIter=10)

# Chain indexers and GBT in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, gbt])

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

gbtModel = model.stages[2]
print gbtModel # summary only
{% endhighlight %}
</div>
</div>

#### Example: Regression

Note: For this example dataset, `GBTRegressor` actually only needs 1 iteration, but that will not
be true in general.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/index.html#org.apache.spark.ml.regression.GBTRegressor) for more details.

{% highlight scala %}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.regression.GBTRegressionModel
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.mllib.util.MLUtils

// Load and parse the data file, converting it to a DataFrame.
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").toDF()

// Automatically identify categorical features, and index them.
// Set maxCategories so features with > 4 distinct values are treated as continuous.
val featureIndexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexedFeatures")
  .setMaxCategories(4)
  .fit(data)

// Split the data into training and test sets (30% held out for testing)
val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

// Train a GBT model.
val gbt = new GBTRegressor()
  .setLabelCol("label")
  .setFeaturesCol("indexedFeatures")
  .setMaxIter(10)

// Chain indexer and GBT in a Pipeline
val pipeline = new Pipeline()
  .setStages(Array(featureIndexer, gbt))

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

val gbtModel = model.stages(1).asInstanceOf[GBTRegressionModel]
println("Learned regression GBT model:\n" + gbtModel.toDebugString)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/regression/GBTRegressor.html) for more details.

{% highlight java %}
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.regression.GBTRegressionModel;
import org.apache.spark.ml.regression.GBTRegressor;
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

// Train a GBT model.
GBTRegressor gbt = new GBTRegressor()
  .setLabelCol("label")
  .setFeaturesCol("indexedFeatures")
  .setMaxIter(10);

// Chain indexer and GBT in a Pipeline
Pipeline pipeline = new Pipeline()
  .setStages(new PipelineStage[] {featureIndexer, gbt});

// Train model.  This also runs the indexer.
PipelineModel model = pipeline.fit(trainingData);

// Make predictions.
DataFrame predictions = model.transform(testData);

// Select example rows to display.
predictions.select("prediction", "label", "features").show(5);

// Select (prediction, true label) and compute test error
RegressionEvaluator evaluator = new RegressionEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("rmse");
double rmse = evaluator.evaluate(predictions);
System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

GBTRegressionModel gbtModel =
  (GBTRegressionModel)(model.stages()[1]);
System.out.println("Learned regression GBT model:\n" + gbtModel.toDebugString());
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/pyspark.ml.html#pyspark.ml.regression.GBTRegressor) for more details.

{% highlight python %}
from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.util import MLUtils

# Load and parse the data file, converting it to a DataFrame.
data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").toDF()

# Automatically identify categorical features, and index them.
# Set maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# Train a GBT model.
gbt = GBTRegressor(featuresCol="indexedFeatures", maxIter=10)

# Chain indexer and GBT in a Pipeline
pipeline = Pipeline(stages=[featureIndexer, gbt])

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

gbtModel = model.stages[1]
print gbtModel # summary only
{% endhighlight %}
</div>
</div>


## One-vs-Rest (a.k.a. One-vs-All)

[OneVsRest](http://en.wikipedia.org/wiki/Multiclass_classification#One-vs.-rest) is an example of a machine learning reduction for performing multiclass classification given a base classifier that can perform binary classification efficiently.  It is also known as "One-vs-All."

`OneVsRest` is implemented as an `Estimator`. For the base classifier it takes instances of `Classifier` and creates a binary classification problem for each of the k classes. The classifier for class i is trained to predict whether the label is i or not, distinguishing class i from all other classes.

Predictions are done by evaluating each binary classifier and the index of the most confident classifier is output as label.

### Example

The example below demonstrates how to load the
[Iris dataset](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass/iris.scale), parse it as a DataFrame and perform multiclass classification using `OneVsRest`. The test error is calculated to measure the algorithm accuracy.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/index.html#org.apache.spark.ml.classifier.OneVsRest) for more details.

{% highlight scala %}
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Row, SQLContext}

val sqlContext = new SQLContext(sc)

// parse data into dataframe
val data = MLUtils.loadLibSVMFile(sc, 
  "data/mllib/sample_multiclass_classification_data.txt")
val Array(train, test) = data.toDF().randomSplit(Array(0.7, 0.3))

// instantiate multiclass learner and train
val ovr = new OneVsRest().setClassifier(new LogisticRegression)

val ovrModel = ovr.fit(train)

// score model on test data
val predictions = ovrModel.transform(test).select("prediction", "label")
val predictionsAndLabels = predictions.map {case Row(p: Double, l: Double) => (p, l)}

// compute confusion matrix
val metrics = new MulticlassMetrics(predictionsAndLabels)
println(metrics.confusionMatrix)

// the Iris DataSet has three classes
val numClasses = 3

println("label\tfpr\n")
(0 until numClasses).foreach { index =>
  val label = index.toDouble
  println(label + "\t" + metrics.falsePositiveRate(label))
}
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/classification/OneVsRest.html) for more details.

{% highlight java %}
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.classification.OneVsRestModel;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

SparkConf conf = new SparkConf().setAppName("JavaOneVsRestExample");
JavaSparkContext jsc = new JavaSparkContext(conf);
SQLContext jsql = new SQLContext(jsc);

RDD<LabeledPoint> data = MLUtils.loadLibSVMFile(jsc.sc(),
  "data/mllib/sample_multiclass_classification_data.txt");

DataFrame dataFrame = jsql.createDataFrame(data, LabeledPoint.class);
DataFrame[] splits = dataFrame.randomSplit(new double[] {0.7, 0.3}, 12345);
DataFrame train = splits[0];
DataFrame test = splits[1];

// instantiate the One Vs Rest Classifier
OneVsRest ovr = new OneVsRest().setClassifier(new LogisticRegression());

// train the multiclass model
OneVsRestModel ovrModel = ovr.fit(train.cache());

// score the model on test data
DataFrame predictions = ovrModel
  .transform(test)
  .select("prediction", "label");

// obtain metrics
MulticlassMetrics metrics = new MulticlassMetrics(predictions);
Matrix confusionMatrix = metrics.confusionMatrix();

// output the Confusion Matrix
System.out.println("Confusion Matrix");
System.out.println(confusionMatrix);

// compute the false positive rate per label
System.out.println();
System.out.println("label\tfpr\n");

// the Iris DataSet has three classes
int numClasses = 3;
for (int index = 0; index < numClasses; index++) {
  double label = (double) index;
  System.out.print(label);
  System.out.print("\t");
  System.out.print(metrics.falsePositiveRate(label));
  System.out.println();
}
{% endhighlight %}
</div>
</div>
