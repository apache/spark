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

import org.apache.hadoop.fs.Path
import org.json4s.DefaultFormats

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, Evaluator}
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model, Pipeline}
import org.apache.spark.mllib.classification.LogisticRegressionSuite.generateLogisticInput
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

class SharedReadWriteSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  @transient var dataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val sqlContext = new SQLContext(sc)
    dataset = sqlContext.createDataFrame(
      sc.parallelize(generateLogisticInput(1.0, 1.0, 100, 42), 2))
  }

  test("read/write: MyValidator with simple estimator") {
    val lr = new LogisticRegression().setMaxIter(3)
    val evaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderPR")  // not default metric
    val paramMaps = new ParamGridBuilder()
        .addGrid(lr.regParam, Array(0.1, 0.2))
        .build()
    val cv = new MyValidator()
      .setEstimator(lr)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramMaps)

    val cv2 = testDefaultReadWrite(cv, testParams = false)

    assert(cv.uid === cv2.uid)

    assert(cv2.getEvaluator.isInstanceOf[BinaryClassificationEvaluator])
    val evaluator2 = cv2.getEvaluator.asInstanceOf[BinaryClassificationEvaluator]
    assert(evaluator.uid === evaluator2.uid)
    assert(evaluator.getMetricName === evaluator2.getMetricName)

    cv2.getEstimator match {
      case lr2: LogisticRegression =>
        assert(lr.uid === lr2.uid)
        assert(lr.getMaxIter === lr2.getMaxIter)
      case other =>
        throw new AssertionError(s"Loaded MyValidator expected estimator of type" +
          s" LogisticRegression but found ${other.getClass.getName}")
    }

    SharedReadWriteSuite.compareParamMaps(cv.getEstimatorParamMaps, cv2.getEstimatorParamMaps)
  }

  test("read/write: MyValidator with complex estimator") {
    // workflow: MyValidator[Pipeline[HashingTF, MyValidator[LogisticRegression]]]
    val lrEvaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderPR")  // not default metric

    val lr = new LogisticRegression().setMaxIter(3)
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.2))
      .build()
    val lrcv = new MyValidator()
      .setEstimator(lr)
      .setEvaluator(lrEvaluator)
      .setEstimatorParamMaps(lrParamMaps)

    val hashingTF = new HashingTF()
    val pipeline = new Pipeline().setStages(Array(hashingTF, lrcv))
    val paramMaps = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 20))
      .addGrid(lr.elasticNetParam, Array(0.0, 1.0))
      .build()
    val evaluator = new BinaryClassificationEvaluator()

    val cv = new MyValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramMaps)

    val cv2 = testDefaultReadWrite(cv, testParams = false)

    assert(cv.uid === cv2.uid)

    assert(cv2.getEvaluator.isInstanceOf[BinaryClassificationEvaluator])
    assert(cv.getEvaluator.uid === cv2.getEvaluator.uid)

    SharedReadWriteSuite.compareParamMaps(cv.getEstimatorParamMaps, cv2.getEstimatorParamMaps)

    cv2.getEstimator match {
      case pipeline2: Pipeline =>
        assert(pipeline.uid === pipeline2.uid)
        pipeline2.getStages match {
          case Array(hashingTF2: HashingTF, lrcv2: MyValidator) =>
            assert(hashingTF.uid === hashingTF2.uid)
            lrcv2.getEstimator match {
              case lr2: LogisticRegression =>
                assert(lr.uid === lr2.uid)
                assert(lr.getMaxIter === lr2.getMaxIter)
              case other =>
                throw new AssertionError(s"Loaded internal MyValidator expected to be" +
                  s" LogisticRegression but found type ${other.getClass.getName}")
            }
            assert(lrcv.uid === lrcv2.uid)
            assert(lrcv2.getEvaluator.isInstanceOf[BinaryClassificationEvaluator])
            assert(lrEvaluator.uid === lrcv2.getEvaluator.uid)
            SharedReadWriteSuite.compareParamMaps(lrParamMaps, lrcv2.getEstimatorParamMaps)
          case other =>
            throw new AssertionError("Loaded Pipeline expected stages (HashingTF, MyValidator)" +
              " but found: " + other.map(_.getClass.getName).mkString(", "))
        }
      case other =>
        throw new AssertionError(s"Loaded MyValidator expected estimator of type" +
          s" MyValidator but found ${other.getClass.getName}")
    }
  }

  test("read/write: MyValidator fails for extraneous Param") {
    val lr = new LogisticRegression()
    val lr2 = new LogisticRegression()
    val evaluator = new BinaryClassificationEvaluator()
    val paramMaps = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.2))
      .addGrid(lr2.regParam, Array(0.1, 0.2))
      .build()
    val cv = new MyValidator()
      .setEstimator(lr)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramMaps)
    withClue("MyValidator.write failed to catch extraneous Param error") {
      intercept[IllegalArgumentException] {
        cv.write
      }
    }
  }

  test("read/write: MyValidatorModel") {
    val lr = new LogisticRegression()
      .setThreshold(0.6)
    val lrModel = new LogisticRegressionModel(lr.uid, Vectors.dense(1.0, 2.0), 1.2)
      .setThreshold(0.6)
    val evaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderPR")  // not default metric
    val paramMaps = new ParamGridBuilder()
        .addGrid(lr.regParam, Array(0.1, 0.2))
        .build()
    val cv = new MyValidatorModel("cvUid", lrModel)
    cv.set(cv.estimator, lr)
      .set(cv.evaluator, evaluator)
      .set(cv.estimatorParamMaps, paramMaps)

    val cv2 = testDefaultReadWrite(cv, testParams = false)

    assert(cv.uid === cv2.uid)

    assert(cv2.getEvaluator.isInstanceOf[BinaryClassificationEvaluator])
    val evaluator2 = cv2.getEvaluator.asInstanceOf[BinaryClassificationEvaluator]
    assert(evaluator.uid === evaluator2.uid)
    assert(evaluator.getMetricName === evaluator2.getMetricName)

    cv2.getEstimator match {
      case lr2: LogisticRegression =>
        assert(lr.uid === lr2.uid)
        assert(lr.getThreshold === lr2.getThreshold)
      case other =>
        throw new AssertionError(s"Loaded MyValidator expected estimator of type" +
          s" LogisticRegression but found ${other.getClass.getName}")
    }

    SharedReadWriteSuite.compareParamMaps(cv.getEstimatorParamMaps, cv2.getEstimatorParamMaps)

    cv2.bestModel match {
      case lrModel2: LogisticRegressionModel =>
        assert(lrModel.uid === lrModel2.uid)
        assert(lrModel.getThreshold === lrModel2.getThreshold)
        assert(lrModel.coefficients === lrModel2.coefficients)
        assert(lrModel.intercept === lrModel2.intercept)
      case other =>
        throw new AssertionError(s"Loaded MyValidator expected bestModel of type" +
          s" LogisticRegressionModel but found ${other.getClass.getName}")
    }
  }
}

object SharedReadWriteSuite extends SparkFunSuite {
  /**
   * Assert sequences of estimatorParamMaps are identical.
   * Params must be simple types comparable with `===`.
   */
  def compareParamMaps(pMaps: Array[ParamMap], pMaps2: Array[ParamMap]): Unit = {
    assert(pMaps.length === pMaps2.length)
    pMaps.zip(pMaps2).foreach { case (pMap, pMap2) =>
      assert(pMap.size === pMap2.size)
      pMap.toSeq.foreach { case ParamPair(p, v) =>
        assert(pMap2.contains(p))
        assert(pMap2(p) === v)
      }
    }
  }
}

class MyValidator(override val uid: String)
  extends Estimator[MyValidatorModel] with ValidatorParams with MLWritable {

  def this() = this(Identifiable.randomUID("fakeValidator"))

  /** @group setParam */
  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

  /** @group setParam */
  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  /** @group setParam */
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  override def fit(dataset: DataFrame): MyValidatorModel = {
    val bestModel = $(estimator).fit(dataset, $(estimatorParamMaps).head).asInstanceOf[Model[_]]
    copyValues(new MyValidatorModel(uid, bestModel).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    $(estimator).transformSchema(schema)
  }

  override def validateParams(): Unit = {
    super.validateParams()
    val est = $(estimator)
    for (paramMap <- $(estimatorParamMaps)) {
      est.copy(paramMap).validateParams()
    }
  }

  override def copy(extra: ParamMap): MyValidator = {
    val copied = defaultCopy(extra).asInstanceOf[MyValidator]
    if (copied.isDefined(estimator)) {
      copied.setEstimator(copied.getEstimator.copy(extra))
    }
    if (copied.isDefined(evaluator)) {
      copied.setEvaluator(copied.getEvaluator.copy(extra))
    }
    copied
  }

  override def write: MLWriter = new MyValidator.MyValidatorWriter(this)
}

object MyValidator extends MLReadable[MyValidator] {

  override def read: MLReader[MyValidator] = new MyValidatorReader

  override def load(path: String): MyValidator = super.load(path)

  private[MyValidator] class MyValidatorWriter(instance: MyValidator)
    extends MLWriter with SharedReadWrite {
    validateParams(instance)
    override protected def saveImpl(path: String): Unit = saveImpl(path, instance, sc)
  }

  private class MyValidatorReader extends MLReader[MyValidator] with SharedReadWrite {
    /** Checked against metadata when loading model */
    private val className = classOf[MyValidator].getName

    override def load(path: String): MyValidator = {
      implicit val format = DefaultFormats
      val (metadata, estimator, evaluator, estimatorParamMaps) = load(path, sc, className)
      new MyValidator(metadata.uid)
        .setEstimator(estimator)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(estimatorParamMaps)
    }
  }
}

class MyValidatorModel private[ml] (
    override val uid: String,
    val bestModel: Model[_])
  extends Model[MyValidatorModel] with ValidatorParams with MLWritable {

  override def validateParams(): Unit = {
    bestModel.validateParams()
  }

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    bestModel.transform(dataset)
  }

  override def transformSchema(schema: StructType): StructType = {
    bestModel.transformSchema(schema)
  }

  override def copy(extra: ParamMap): MyValidatorModel = {
    val copied = new MyValidatorModel(
      uid,
      bestModel.copy(extra).asInstanceOf[Model[_]])
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new MyValidatorModel.MyValidatorModelWriter(this)
}

object MyValidatorModel extends MLReadable[MyValidatorModel] {

  override def read: MLReader[MyValidatorModel] = new MyValidatorModelReader

  override def load(path: String): MyValidatorModel = super.load(path)

  private[MyValidatorModel] class MyValidatorModelWriter(instance: MyValidatorModel)
    extends MLWriter with SharedReadWrite {
    validateParams(instance)
    override protected def saveImpl(path: String): Unit = {
      saveImpl(path, instance, sc, None)
      val bestModelPath = new Path(path, "bestModel").toString
      instance.bestModel.asInstanceOf[MLWritable].save(bestModelPath)
    }
  }

  private class MyValidatorModelReader extends MLReader[MyValidatorModel] with SharedReadWrite {

    /** Checked against metadata when loading model */
    private val className = classOf[MyValidatorModel].getName

    override def load(path: String): MyValidatorModel = {
      val (metadata, estimator, evaluator, estimatorParamMaps) = load(path, sc, className)
      val bestModelPath = new Path(path, "bestModel").toString
      val bestModel = DefaultParamsReader.loadParamsInstance[Model[_]](bestModelPath, sc)
      val cv = new MyValidatorModel(metadata.uid, bestModel)
      cv.set(cv.estimator, estimator)
        .set(cv.evaluator, evaluator)
      .set(cv.estimatorParamMaps, estimatorParamMaps)
    }
  }
}
