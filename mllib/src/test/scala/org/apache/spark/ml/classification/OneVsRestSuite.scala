/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.classification

import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.classification.LogisticRegressionSuite._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.{ParamMap, ParamsSuite}
import org.apache.spark.ml.util.{DefaultReadWriteTest, MetadataUtils, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.mllib.regression.{LabeledPoint => OldLabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.Metadata

class OneVsRestSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var dataset: Dataset[_] = _
  @transient var rdd: RDD[LabeledPoint] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val nPoints = 1000

    // The following coefficients and xMean/xVariance are computed from iris dataset with lambda=0.2
    // As a result, we are drawing samples from probability distribution of an actual model.
    val coefficients = Array(
      -0.57997, 0.912083, -0.371077, -0.819866, 2.688191,
      -0.16624, -0.84355, -0.048509, -0.301789, 4.170682)

    val xMean = Array(5.843, 3.057, 3.758, 1.199)
    val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)
    rdd = sc.parallelize(generateMultinomialLogisticInput(
      coefficients, xMean, xVariance, true, nPoints, 42), 2)
    dataset = rdd.toDF()
  }

  test("params") {
    ParamsSuite.checkParams(new OneVsRest)
    val lrModel = new LogisticRegressionModel("lr", Vectors.dense(0.0), 0.0)
    val model = new OneVsRestModel("ovr", Metadata.empty, Array(lrModel))
    ParamsSuite.checkParams(model)
  }

  test("one-vs-rest: default params") {
    val numClasses = 3
    val ova = new OneVsRest()
      .setClassifier(new LogisticRegression)
    assert(ova.getLabelCol === "label")
    assert(ova.getPredictionCol === "prediction")
    assert(ova.getRawPredictionCol === "rawPrediction")
    val ovaModel = ova.fit(dataset)

    MLTestingUtils.checkCopyAndUids(ova, ovaModel)

    assert(ovaModel.numClasses === numClasses)
    val transformedDataset = ovaModel.transform(dataset)

    // check for label metadata in prediction col
    val predictionColSchema = transformedDataset.schema(ovaModel.getPredictionCol)
    assert(MetadataUtils.getNumClasses(predictionColSchema) === Some(3))

    val lr = new LogisticRegressionWithLBFGS().setIntercept(true).setNumClasses(numClasses)
    lr.optimizer.setRegParam(0.1).setNumIterations(100)

    val model = lr.run(rdd.map(OldLabeledPoint.fromML))
    val results = model.predict(rdd.map(p => OldVectors.fromML(p.features))).zip(rdd.map(_.label))
    // determine the #confusion matrix in each class.
    // bound how much error we allow compared to multinomial logistic regression.
    val expectedMetrics = new MulticlassMetrics(results)

    testTransformerByGlobalCheckFunc[(Double, Vector)](dataset.toDF(), ovaModel,
      "prediction", "label") { rows =>
      val ovaResults = rows.map { row => (row.getDouble(0), row.getDouble(1)) }
      val ovaMetrics = new MulticlassMetrics(sc.makeRDD(ovaResults))
      assert(expectedMetrics.confusionMatrix.asML ~== ovaMetrics.confusionMatrix.asML absTol 400)
    }
  }

  test("one-vs-rest: tuning parallelism does not change output") {
    val ovaPar1 = new OneVsRest()
      .setClassifier(new LogisticRegression)

    val ovaModelPar1 = ovaPar1.fit(dataset)

    val transformedDatasetPar1 = ovaModelPar1.transform(dataset)

    val ovaResultsPar1 = transformedDatasetPar1.select("prediction", "label").rdd.map {
      row => (row.getDouble(0), row.getDouble(1))
    }

    val ovaPar2 = new OneVsRest()
      .setClassifier(new LogisticRegression)
      .setParallelism(2)

    val ovaModelPar2 = ovaPar2.fit(dataset)

    val transformedDatasetPar2 = ovaModelPar2.transform(dataset)

    val ovaResultsPar2 = transformedDatasetPar2.select("prediction", "label").rdd.map {
      row => (row.getDouble(0), row.getDouble(1))
    }

    val metricsPar1 = new MulticlassMetrics(ovaResultsPar1)
    val metricsPar2 = new MulticlassMetrics(ovaResultsPar2)
    assert(metricsPar1.confusionMatrix == metricsPar2.confusionMatrix)

    ovaModelPar1.models.zip(ovaModelPar2.models).foreach {
      case (lrModel1: LogisticRegressionModel, lrModel2: LogisticRegressionModel) =>
        assert(lrModel1.coefficients ~== lrModel2.coefficients relTol 1E-3)
        assert(lrModel1.intercept ~== lrModel2.intercept relTol 1E-3)
      case other =>
        fail("Loaded OneVsRestModel expected model of type LogisticRegressionModel " +
          s"but found ${other.getClass.getName}")
    }
  }

  test("one-vs-rest: pass label metadata correctly during train") {
    val numClasses = 3
    val ova = new OneVsRest()
    ova.setClassifier(new MockLogisticRegression)

    val labelMetadata = NominalAttribute.defaultAttr.withName("label").withNumValues(numClasses)
    val labelWithMetadata = dataset("label").as("label", labelMetadata.toMetadata())
    val features = dataset("features").as("features")
    val datasetWithLabelMetadata = dataset.select(labelWithMetadata, features)
    ova.fit(datasetWithLabelMetadata)
  }

  test("SPARK-8092: ensure label features and prediction cols are configurable") {
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexed")

    val indexedDataset = labelIndexer
      .fit(dataset)
      .transform(dataset)
      .drop("label")
      .withColumnRenamed("features", "f")

    val ova = new OneVsRest()
    ova.setClassifier(new LogisticRegression())
      .setLabelCol(labelIndexer.getOutputCol)
      .setFeaturesCol("f")
      .setPredictionCol("p")

    val ovaModel = ova.fit(indexedDataset)
    val transformedDataset = ovaModel.transform(indexedDataset)
    val outputFields = transformedDataset.schema.fieldNames.toSet
    assert(outputFields.contains("p"))
  }

  test("SPARK-18625 : OneVsRestModel should support setFeaturesCol and setPredictionCol") {
    val ova = new OneVsRest().setClassifier(new LogisticRegression)
    val ovaModel = ova.fit(dataset)
    val dataset2 = dataset.select(col("label").as("y"), col("features").as("fea"))
    ovaModel.setFeaturesCol("fea")
    ovaModel.setPredictionCol("pred")
    ovaModel.setRawPredictionCol("")
    val transformedDataset = ovaModel.transform(dataset2)
    val outputFields = transformedDataset.schema.fieldNames.toSet
    assert(outputFields === Set("y", "fea", "pred"))
  }

  test("SPARK-8049: OneVsRest shouldn't output temp columns") {
    val logReg = new LogisticRegression()
      .setMaxIter(1)
    val ovr = new OneVsRest()
      .setClassifier(logReg)
    val output = ovr.fit(dataset).transform(dataset)
    assert(output.schema.fieldNames.toSet
      === Set("label", "features", "prediction", "rawPrediction"))
  }

  test("SPARK-21306: OneVsRest should support setWeightCol") {
    val dataset2 = dataset.withColumn("weight", lit(1))
    // classifier inherits hasWeightCol
    val ova = new OneVsRest().setWeightCol("weight").setClassifier(new LogisticRegression())
    assert(ova.fit(dataset2) !== null)
    // classifier doesn't inherit hasWeightCol
    val ova2 = new OneVsRest().setWeightCol("weight").setClassifier(new DecisionTreeClassifier())
    assert(ova2.fit(dataset2) !== null)
  }

  test("OneVsRest.copy and OneVsRestModel.copy") {
    val lr = new LogisticRegression()
      .setMaxIter(1)

    val ovr = new OneVsRest()
    withClue("copy with classifier unset should work") {
      ovr.copy(ParamMap(lr.maxIter -> 10))
    }
    ovr.setClassifier(lr)
    val ovr1 = ovr.copy(ParamMap(lr.maxIter -> 10))
    require(ovr.getClassifier.getOrDefault(lr.maxIter) === 1, "copy should have no side-effects")
    require(ovr1.getClassifier.getOrDefault(lr.maxIter) === 10,
      "copy should handle extra classifier params")

    val ovrModel = ovr1.fit(dataset).copy(ParamMap(lr.thresholds -> Array(0.9, 0.1)))
    ovrModel.models.foreach { case m: LogisticRegressionModel =>
      require(m.getThreshold === 0.1, "copy should handle extra model params")
    }
  }

  test("read/write: OneVsRest") {
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)

    val ova = new OneVsRest()
      .setClassifier(lr)
      .setLabelCol("myLabel")
      .setFeaturesCol("myFeature")
      .setPredictionCol("myPrediction")

    val ova2 = testDefaultReadWrite(ova, testParams = false)
    assert(ova.uid === ova2.uid)
    assert(ova.getFeaturesCol === ova2.getFeaturesCol)
    assert(ova.getLabelCol === ova2.getLabelCol)
    assert(ova.getPredictionCol === ova2.getPredictionCol)

    ova2.getClassifier match {
      case lr2: LogisticRegression =>
        assert(lr.uid === lr2.uid)
        assert(lr.getMaxIter === lr2.getMaxIter)
        assert(lr.getRegParam === lr2.getRegParam)
      case other =>
        fail("Loaded OneVsRest expected classifier of type LogisticRegression" +
          s" but found ${other.getClass.getName}")
    }
  }

  test("read/write: OneVsRestModel") {
    def checkModelData(model: OneVsRestModel, model2: OneVsRestModel): Unit = {
      assert(model.uid === model2.uid)
      assert(model.getFeaturesCol === model2.getFeaturesCol)
      assert(model.getLabelCol === model2.getLabelCol)
      assert(model.getPredictionCol === model2.getPredictionCol)

      val classifier = model.getClassifier.asInstanceOf[LogisticRegression]

      model2.getClassifier match {
        case lr2: LogisticRegression =>
          assert(classifier.uid === lr2.uid)
          assert(classifier.getMaxIter === lr2.getMaxIter)
          assert(classifier.getRegParam === lr2.getRegParam)
        case other =>
          fail("Loaded OneVsRestModel expected classifier of type LogisticRegression" +
            s" but found ${other.getClass.getName}")
      }

      assert(model.labelMetadata === model2.labelMetadata)
      model.models.zip(model2.models).foreach {
        case (lrModel1: LogisticRegressionModel, lrModel2: LogisticRegressionModel) =>
          assert(lrModel1.uid === lrModel2.uid)
          assert(lrModel1.coefficients === lrModel2.coefficients)
          assert(lrModel1.intercept === lrModel2.intercept)
        case other =>
          fail(s"Loaded OneVsRestModel expected model of type LogisticRegressionModel" +
            s" but found ${other.getClass.getName}")
      }
    }

    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)
    val ova = new OneVsRest().setClassifier(lr)
    val ovaModel = ova.fit(dataset)
    val newOvaModel = testDefaultReadWrite(ovaModel, testParams = false)
    checkModelData(ovaModel, newOvaModel)
  }

  test("should ignore empty output cols") {
    val lr = new LogisticRegression().setMaxIter(1)
    val ovr = new OneVsRest().setClassifier(lr)
    val ovrModel = ovr.fit(dataset)

    val output1 = ovrModel.setPredictionCol("").setRawPredictionCol("")
      .transform(dataset)
    assert(output1.schema.fieldNames.toSet ===
      Set("label", "features"))

    val output2 = ovrModel.setPredictionCol("prediction").setRawPredictionCol("")
      .transform(dataset)
    assert(output2.schema.fieldNames.toSet ===
      Set("label", "features", "prediction"))

    val output3 = ovrModel.setPredictionCol("").setRawPredictionCol("rawPrediction")
      .transform(dataset)
    assert(output3.schema.fieldNames.toSet ===
      Set("label", "features", "rawPrediction"))

    val output4 = ovrModel.setPredictionCol("prediction").setRawPredictionCol("rawPrediction")
      .transform(dataset)
    assert(output4.schema.fieldNames.toSet ===
      Set("label", "features", "prediction", "rawPrediction"))
  }

  test("should support all NumericType labels and not support other types") {
    val ovr = new OneVsRest().setClassifier(new LogisticRegression().setMaxIter(1))
    MLTestingUtils.checkNumericTypes[OneVsRestModel, OneVsRest](
      ovr, spark) { (expected, actual) =>
        val expectedModels = expected.models.map(m => m.asInstanceOf[LogisticRegressionModel])
        val actualModels = actual.models.map(m => m.asInstanceOf[LogisticRegressionModel])
        assert(expectedModels.length === actualModels.length)
        expectedModels.zip(actualModels).foreach { case (e, a) =>
          assert(e.intercept === a.intercept)
          assert(e.coefficients.toArray === a.coefficients.toArray)
        }
      }
  }
}

private class MockLogisticRegression(uid: String) extends LogisticRegression(uid) {

  def this() = this("mockLogReg")

  setMaxIter(1)

  override protected[spark] def train(dataset: Dataset[_]): LogisticRegressionModel = {
    val labelSchema = dataset.schema($(labelCol))
    // check for label attribute propagation.
    assert(MetadataUtils.getNumClasses(labelSchema).forall(_ == 2))
    super.train(dataset)
  }
}
