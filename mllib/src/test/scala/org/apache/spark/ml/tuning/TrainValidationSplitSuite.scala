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

package org.apache.spark.ml.tuning

import java.io.File

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, OneVsRest}
import org.apache.spark.ml.classification.LogisticRegressionSuite.generateLogisticInput
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, Evaluator, RegressionEvaluator}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.mllib.util.LinearDataGenerator
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

class TrainValidationSplitSuite
  extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    dataset = sc.parallelize(generateLogisticInput(1.0, 1.0, 100, 42), 2).toDF()
  }

  test("train validation with logistic regression") {
    val lr = new LogisticRegression
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.001, 1000.0))
      .addGrid(lr.maxIter, Array(0, 10))
      .build()
    val eval = new BinaryClassificationEvaluator
    val tvs = new TrainValidationSplit()
      .setEstimator(lr)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setTrainRatio(0.5)
      .setSeed(42L)
    val tvsModel = tvs.fit(dataset)
    val parent = tvsModel.bestModel.parent.asInstanceOf[LogisticRegression]
    assert(tvs.getTrainRatio === 0.5)
    assert(parent.getRegParam === 0.001)
    assert(parent.getMaxIter === 10)
    assert(tvsModel.validationMetrics.length === lrParamMaps.length)

    val result = tvsModel.transform(dataset).select("prediction").as[Double].collect()
    testTransformerByGlobalCheckFunc[(Double, Vector)](dataset.toDF(), tvsModel, "prediction") {
      rows =>
        val result2 = rows.map(_.getDouble(0))
        assert(result === result2)
    }
  }

  test("train validation with linear regression") {
    val dataset = sc.parallelize(
      LinearDataGenerator.generateLinearInput(
        6.3, Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 100, 42, 0.1), 2)
      .map(_.asML).toDF()

    val trainer = new LinearRegression().setSolver("l-bfgs")
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(trainer.regParam, Array(1000.0, 0.001))
      .addGrid(trainer.maxIter, Array(0, 10))
      .build()
    val eval = new RegressionEvaluator()
    val tvs = new TrainValidationSplit()
      .setEstimator(trainer)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setTrainRatio(0.5)
      .setSeed(42L)
    val tvsModel = tvs.fit(dataset)

    MLTestingUtils.checkCopyAndUids(tvs, tvsModel)

    val parent = tvsModel.bestModel.parent.asInstanceOf[LinearRegression]
    assert(parent.getRegParam === 0.001)
    assert(parent.getMaxIter === 10)
    assert(tvsModel.validationMetrics.length === lrParamMaps.length)

      eval.setMetricName("r2")
    val tvsModel2 = tvs.fit(dataset)
    val parent2 = tvsModel2.bestModel.parent.asInstanceOf[LinearRegression]
    assert(parent2.getRegParam === 0.001)
    assert(parent2.getMaxIter === 10)
    assert(tvsModel2.validationMetrics.length === lrParamMaps.length)
  }

  test("transformSchema should check estimatorParamMaps") {
    import TrainValidationSplitSuite.{MyEstimator, MyEvaluator}

    val est = new MyEstimator("est")
    val eval = new MyEvaluator
    val paramMaps = new ParamGridBuilder()
      .addGrid(est.inputCol, Array("input1", "input2"))
      .build()

    val tvs = new TrainValidationSplit()
      .setEstimator(est)
      .setEstimatorParamMaps(paramMaps)
      .setEvaluator(eval)
      .setTrainRatio(0.5)
    tvs.transformSchema(new StructType()) // This should pass.

    val invalidParamMaps = paramMaps :+ ParamMap(est.inputCol -> "")
    tvs.setEstimatorParamMaps(invalidParamMaps)
    intercept[IllegalArgumentException] {
      tvs.transformSchema(new StructType())
    }
  }

  test("train validation with parallel evaluation") {
    val lr = new LogisticRegression
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.001, 1000.0))
      .addGrid(lr.maxIter, Array(0, 3))
      .build()
    val eval = new BinaryClassificationEvaluator
    val cv = new TrainValidationSplit()
      .setEstimator(lr)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setParallelism(1)
    val cvSerialModel = cv.fit(dataset)
    cv.setParallelism(2)
    val cvParallelModel = cv.fit(dataset)

    val serialMetrics = cvSerialModel.validationMetrics
    val parallelMetrics = cvParallelModel.validationMetrics
    assert(serialMetrics === parallelMetrics)

    val parentSerial = cvSerialModel.bestModel.parent.asInstanceOf[LogisticRegression]
    val parentParallel = cvParallelModel.bestModel.parent.asInstanceOf[LogisticRegression]
    assert(parentSerial.getRegParam === parentParallel.getRegParam)
    assert(parentSerial.getMaxIter === parentParallel.getMaxIter)
  }

  test("read/write: TrainValidationSplit") {
    val lr = new LogisticRegression().setMaxIter(3)
    val evaluator = new BinaryClassificationEvaluator()
    val paramMaps = new ParamGridBuilder()
        .addGrid(lr.regParam, Array(0.1, 0.2))
        .build()
    val tvs = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(evaluator)
      .setTrainRatio(0.5)
      .setEstimatorParamMaps(paramMaps)
      .setSeed(42L)
      .setParallelism(2)
      .setCollectSubModels(true)

    val tvs2 = testDefaultReadWrite(tvs, testParams = false)

    assert(tvs.getTrainRatio === tvs2.getTrainRatio)
    assert(tvs.getSeed === tvs2.getSeed)
    assert(tvs.getParallelism === tvs2.getParallelism)
    assert(tvs.getCollectSubModels === tvs2.getCollectSubModels)

    ValidatorParamsSuiteHelpers
      .compareParamMaps(tvs.getEstimatorParamMaps, tvs2.getEstimatorParamMaps)

    tvs2.getEstimator match {
      case lr2: LogisticRegression =>
        assert(lr.uid === lr2.uid)
        assert(lr.getMaxIter === lr2.getMaxIter)
      case other =>
        fail("Loaded TrainValidationSplit expected estimator of type LogisticRegression" +
          s" but found ${other.getClass.getName}")
    }
  }

  test("TrainValidationSplit expose sub models") {
    val lr = new LogisticRegression
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.001, 1000.0))
      .addGrid(lr.maxIter, Array(0, 3))
      .build()
    val eval = new BinaryClassificationEvaluator
    val subPath = new File(tempDir, "testTrainValidationSplitSubModels")

    val tvs = new TrainValidationSplit()
      .setEstimator(lr)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setParallelism(1)
      .setCollectSubModels(true)

    val tvsModel = tvs.fit(dataset)

    assert(tvsModel.hasSubModels && tvsModel.subModels.length == lrParamMaps.length)

    // Test the default value for option "persistSubModel" to be "true"
    val savingPathWithSubModels = new File(subPath, "tvsModel3").getPath
    tvsModel.save(savingPathWithSubModels)
    val tvsModel3 = TrainValidationSplitModel.load(savingPathWithSubModels)
    assert(tvsModel3.hasSubModels && tvsModel3.subModels.length == lrParamMaps.length)

    val savingPathWithoutSubModels = new File(subPath, "tvsModel2").getPath
    tvsModel.write.option("persistSubModels", "false").save(savingPathWithoutSubModels)
    val tvsModel2 = TrainValidationSplitModel.load(savingPathWithoutSubModels)
    assert(!tvsModel2.hasSubModels)

    for (i <- 0 until lrParamMaps.length) {
      assert(tvsModel.subModels(i).asInstanceOf[LogisticRegressionModel].uid ===
        tvsModel3.subModels(i).asInstanceOf[LogisticRegressionModel].uid)
    }

    val savingPathTestingIllegalParam = new File(subPath, "tvsModel4").getPath
    intercept[IllegalArgumentException] {
      tvsModel2.write.option("persistSubModels", "true").save(savingPathTestingIllegalParam)
    }
  }

  test("read/write: TrainValidationSplit with nested estimator") {
    val ova = new OneVsRest()
      .setClassifier(new LogisticRegression)
    val evaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderPR")  // not default metric
    val classifier1 = new LogisticRegression().setRegParam(2.0)
    val classifier2 = new LogisticRegression().setRegParam(3.0)
    val paramMaps = new ParamGridBuilder()
      .addGrid(ova.classifier, Array(classifier1, classifier2))
      .build()
    val tvs = new TrainValidationSplit()
      .setEstimator(ova)
      .setEvaluator(evaluator)
      .setTrainRatio(0.5)
      .setEstimatorParamMaps(paramMaps)
      .setSeed(42L)

    val tvs2 = testDefaultReadWrite(tvs, testParams = false)

    assert(tvs.getTrainRatio === tvs2.getTrainRatio)
    assert(tvs.getSeed === tvs2.getSeed)

    tvs2.getEstimator match {
      case ova2: OneVsRest =>
        assert(ova.uid === ova2.uid)
        ova2.getClassifier match {
          case lr: LogisticRegression =>
            assert(ova.getClassifier.asInstanceOf[LogisticRegression].getMaxIter
              === lr.getMaxIter)
          case other =>
            fail(s"Loaded TrainValidationSplit expected estimator of type LogisticRegression" +
              s" but found ${other.getClass.getName}")
        }

      case other =>
        fail(s"Loaded TrainValidationSplit expected estimator of type OneVsRest" +
          s" but found ${other.getClass.getName}")
    }

    ValidatorParamsSuiteHelpers
      .compareParamMaps(tvs.getEstimatorParamMaps, tvs2.getEstimatorParamMaps)
  }

  test("read/write: Persistence of nested estimator works if parent directory changes") {
    val ova = new OneVsRest()
      .setClassifier(new LogisticRegression)
    val evaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderPR")  // not default metric
    val classifier1 = new LogisticRegression().setRegParam(2.0)
    val classifier2 = new LogisticRegression().setRegParam(3.0)
    val paramMaps = new ParamGridBuilder()
      .addGrid(ova.classifier, Array(classifier1, classifier2))
      .build()
    val tvs = new TrainValidationSplit()
      .setEstimator(ova)
      .setEvaluator(evaluator)
      .setTrainRatio(0.5)
      .setEstimatorParamMaps(paramMaps)
      .setSeed(42L)

    ValidatorParamsSuiteHelpers.testFileMove(tvs, tempDir)
  }

  test("read/write: TrainValidationSplitModel") {
    val lr = new LogisticRegression()
      .setThreshold(0.6)
    val lrModel = new LogisticRegressionModel(lr.uid, Vectors.dense(1.0, 2.0), 1.2)
      .setThreshold(0.6)
    val evaluator = new BinaryClassificationEvaluator()
    val paramMaps = new ParamGridBuilder()
        .addGrid(lr.regParam, Array(0.1, 0.2))
        .build()
    val tvs = new TrainValidationSplitModel("cvUid", lrModel, Array(0.3, 0.6))
    tvs.set(tvs.estimator, lr)
      .set(tvs.evaluator, evaluator)
      .set(tvs.trainRatio, 0.5)
      .set(tvs.estimatorParamMaps, paramMaps)
      .set(tvs.seed, 42L)

    val tvs2 = testDefaultReadWrite(tvs, testParams = false)

    assert(tvs.getTrainRatio === tvs2.getTrainRatio)
    assert(tvs.validationMetrics === tvs2.validationMetrics)
    assert(tvs.getSeed === tvs2.getSeed)
  }
}

object TrainValidationSplitSuite extends SparkFunSuite {

  abstract class MyModel extends Model[MyModel]

  class MyEstimator(override val uid: String) extends Estimator[MyModel] with HasInputCol {

    override def fit(dataset: Dataset[_]): MyModel = {
      throw new UnsupportedOperationException
    }

    override def transformSchema(schema: StructType): StructType = {
      require($(inputCol).nonEmpty)
      schema
    }

    override def copy(extra: ParamMap): MyEstimator = defaultCopy(extra)
  }

  class MyEvaluator extends Evaluator {

    override def evaluate(dataset: Dataset[_]): Double = {
      throw new UnsupportedOperationException
    }

    override def isLargerBetter: Boolean = true

    override val uid: String = "eval"

    override def copy(extra: ParamMap): MyEvaluator = defaultCopy(extra)
  }
}
