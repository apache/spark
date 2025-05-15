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

package org.apache.spark.sql.connect.ml

import scala.jdk.CollectionConverters.ListHasAsScala

import org.apache.spark.connect.proto
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.{Vectors, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.SessionHolder

trait FakeArrayParams extends Params {
  final val arrayString: StringArrayParam =
    new StringArrayParam(this, "arrayString", "array string")

  final def getArrayString: Array[String] = $(arrayString)

  final val arrayDouble: DoubleArrayParam =
    new DoubleArrayParam(this, "arrayDouble", "array double")

  final def getArrayDouble: Array[Double] = $(arrayDouble)

  final val arrayInt: IntArrayParam = new IntArrayParam(this, "arrayInt", "array int")

  final def getArrayInt: Array[Int] = $(arrayInt)

  final val int: IntParam = new IntParam(this, "int", "int")

  final def getInt: Int = $(int)

  final val float: FloatParam = new FloatParam(this, "float", "float")

  final def getFloat: Float = $(float)

  final val boolean: BooleanParam = new BooleanParam(this, "boolean", "boolean")

  final def getBoolean: Boolean = $(boolean)

  final val double: DoubleParam = new DoubleParam(this, "double", "double")

  final def getDouble: Double = $(double)
}

class FakedML(override val uid: String) extends FakeArrayParams {
  def this() = this(Identifiable.randomUID("FakedML"))

  override def copy(extra: ParamMap): Params = this
}

class MLSuite extends MLHelper {

  test("reconcileParam") {
    val fakedML = new FakedML
    val params = proto.MlParams
      .newBuilder()
      .putParams("boolean", proto.Expression.Literal.newBuilder().setBoolean(true).build())
      .putParams("double", proto.Expression.Literal.newBuilder().setDouble(1.0).build())
      .putParams("int", proto.Expression.Literal.newBuilder().setInteger(10).build())
      .putParams("float", proto.Expression.Literal.newBuilder().setFloat(10.0f).build())
      .putParams("arrayString", getArrayStrings)
      .putParams(
        "arrayInt",
        proto.Expression.Literal
          .newBuilder()
          .setArray(
            proto.Expression.Literal.Array
              .newBuilder()
              .setElementType(proto.DataType
                .newBuilder()
                .setInteger(proto.DataType.Integer.getDefaultInstance)
                .build())
              .addElements(proto.Expression.Literal.newBuilder().setInteger(1))
              .addElements(proto.Expression.Literal.newBuilder().setInteger(2))
              .build())
          .build())
      .putParams(
        "arrayDouble",
        proto.Expression.Literal
          .newBuilder()
          .setArray(
            proto.Expression.Literal.Array
              .newBuilder()
              .setElementType(proto.DataType
                .newBuilder()
                .setDouble(proto.DataType.Double.getDefaultInstance)
                .build())
              .addElements(proto.Expression.Literal.newBuilder().setDouble(11.0))
              .addElements(proto.Expression.Literal.newBuilder().setDouble(12.0))
              .build())
          .build())
      .build()
    MLUtils.setInstanceParams(fakedML, params)
    assert(fakedML.getInt === 10)
    assert(fakedML.getFloat === 10.0)
    assert(fakedML.getArrayInt === Array(1, 2))
    assert(fakedML.getArrayDouble === Array(11.0, 12.0))
    assert(fakedML.getArrayString === Array("a", "b", "c"))
    assert(fakedML.getBoolean === true)
    assert(fakedML.getDouble === 1.0)
  }

  def trainLogisticRegressionModel(sessionHolder: SessionHolder): String = {
    val fitCommand = proto.MlCommand
      .newBuilder()
      .setFit(
        proto.MlCommand.Fit
          .newBuilder()
          .setDataset(createLocalRelationProto)
          .setEstimator(getLogisticRegression)
          .setParams(getMaxIter))
      .build()
    val fitResult = MLHandler.handleMlCommand(sessionHolder, fitCommand)
    fitResult.getOperatorInfo.getObjRef.getId
  }

  // Estimator/Model works
  test("LogisticRegression works") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)

    // estimator read/write
    val ret = readWrite(sessionHolder, getLogisticRegression, getMaxIter)
    assert(ret.getOperatorInfo.getParams.getParamsMap.get("maxIter").getInteger == 2)

    def verifyModel(modelId: String, hasSummary: Boolean = false): Unit = {
      val model = sessionHolder.mlCache.get(modelId)
      // Model is cached
      assert(model != null)
      assert(model.isInstanceOf[LogisticRegressionModel])
      val lrModel = model.asInstanceOf[LogisticRegressionModel]
      assert(lrModel.getMaxIter === 2)

      // Fetch double attribute
      val interceptCommand = fetchCommand(modelId, "intercept")
      val interceptResult = MLHandler.handleMlCommand(sessionHolder, interceptCommand)
      assert(interceptResult.getParam.getDouble === lrModel.intercept)

      // Fetch Vector attribute
      val coefficientsCommand = fetchCommand(modelId, "coefficients")
      val coefficientsResult = MLHandler.handleMlCommand(sessionHolder, coefficientsCommand)
      val deserializedCoefficients =
        MLUtils.deserializeVector(coefficientsResult.getParam.getStruct)
      assert(deserializedCoefficients === lrModel.coefficients)

      // Fetch Matrix attribute
      val coefficientsMatrixCommand = fetchCommand(modelId, "coefficientMatrix")
      val coefficientsMatrixResult =
        MLHandler.handleMlCommand(sessionHolder, coefficientsMatrixCommand)
      val deserializedCoefficientsMatrix =
        MLUtils.deserializeMatrix(coefficientsMatrixResult.getParam.getStruct)
      assert(lrModel.coefficientMatrix === deserializedCoefficientsMatrix)

      // Predict with sparse vector
      val sparseVector = Vectors.dense(Array(0.0, 2.0)).toSparse
      val predictCommand = proto.MlCommand
        .newBuilder()
        .setFetch(
          proto.Fetch
            .newBuilder()
            .setObjRef(proto.ObjectRef.newBuilder().setId(modelId))
            .addMethods(
              proto.Fetch.Method
                .newBuilder()
                .setMethod("predict")
                .addArgs(proto.Fetch.Method.Args
                  .newBuilder()
                  .setParam(Serializer.serializeParam(sparseVector)))))
        .build()
      val predictResult = MLHandler.handleMlCommand(sessionHolder, predictCommand)
      val predictValue = predictResult.getParam.getDouble
      assert(lrModel.predict(sparseVector) === predictValue)

      // The loaded model doesn't have summary
      if (hasSummary) {
        // Fetch summary attribute
        val accuracyCommand = proto.MlCommand
          .newBuilder()
          .setFetch(
            proto.Fetch
              .newBuilder()
              .setObjRef(proto.ObjectRef.newBuilder().setId(modelId))
              .addMethods(proto.Fetch.Method.newBuilder().setMethod("summary"))
              .addMethods(proto.Fetch.Method.newBuilder().setMethod("accuracy")))
          .build()
        val accuracyResult = MLHandler.handleMlCommand(sessionHolder, accuracyCommand)
        assert(lrModel.summary.accuracy === accuracyResult.getParam.getDouble)

        val weightedFMeasureCommand = proto.MlCommand
          .newBuilder()
          .setFetch(
            proto.Fetch
              .newBuilder()
              .setObjRef(proto.ObjectRef.newBuilder().setId(modelId))
              .addMethods(proto.Fetch.Method.newBuilder().setMethod("summary"))
              .addMethods(
                proto.Fetch.Method
                  .newBuilder()
                  .setMethod("weightedFMeasure")
                  .addArgs(proto.Fetch.Method.Args
                    .newBuilder()
                    .setParam(Serializer.serializeParam(2.5)))))
          .build()
        val weightedFMeasureResult =
          MLHandler.handleMlCommand(sessionHolder, weightedFMeasureCommand)
        assert(
          lrModel.summary.weightedFMeasure(2.5) ===
            weightedFMeasureResult.getParam.getDouble)
      }
    }

    val modelId = trainLogisticRegressionModel(sessionHolder)
    verifyModel(modelId, hasSummary = true)

    // model read/write
    val ret1 = readWrite(
      sessionHolder,
      modelId,
      "org.apache.spark.ml.classification.LogisticRegressionModel")
    verifyModel(ret1.getOperatorInfo.getObjRef.getId)
  }

  test("Exception: Unsupported ML operator") {
    intercept[MlUnsupportedException] {
      val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
      val command = proto.MlCommand
        .newBuilder()
        .setFit(
          proto.MlCommand.Fit
            .newBuilder()
            .setDataset(createLocalRelationProto)
            .setEstimator(
              proto.MlOperator
                .newBuilder()
                .setName("org.apache.spark.ml.NotExistingML")
                .setUid("FakedUid")
                .setType(proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR)))
        .build()
      MLHandler.handleMlCommand(sessionHolder, command)
    }
  }

  test("access the attribute which is not in allowed list") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val modelId = trainLogisticRegressionModel(sessionHolder)

    val fakeAttributeCmd = fetchCommand(modelId, "notExistingAttribute")
    val e = intercept[MLAttributeNotAllowedException] {
      MLHandler.handleMlCommand(sessionHolder, fakeAttributeCmd)
    }
    val msg = e.getMessage
    assert(msg.contains("notExistingAttribute"))
    assert(msg.contains("org.apache.spark.ml.classification.LogisticRegressionModel"))
  }

  test("Model must be registered into ServiceLoader when loading") {
    val thrown = intercept[MlUnsupportedException] {
      val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
      val readCmd = proto.MlCommand
        .newBuilder()
        .setRead(
          proto.MlCommand.Read
            .newBuilder()
            .setOperator(proto.MlOperator
              .newBuilder()
              .setName("org.apache.spark.sql.connect.ml.NotImplementingMLReadble")
              .setType(proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR))
            .setPath("/tmp/fake"))
        .build()
      MLHandler.handleMlCommand(sessionHolder, readCmd)
    }
    assert(
      thrown.message.contains("Unsupported read for " +
        "org.apache.spark.sql.connect.ml.NotImplementingMLReadble"))
  }

  test("RegressionEvaluator works") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)

    val evalCmd = proto.MlCommand
      .newBuilder()
      .setEvaluate(
        proto.MlCommand.Evaluate
          .newBuilder()
          .setDataset(createRegressionEvaluationLocalRelationProto)
          .setEvaluator(getRegressorEvaluator)
          .setParams(
            proto.MlParams
              .newBuilder()
              .putParams(
                "predictionCol",
                proto.Expression.Literal.newBuilder().setString("raw").build())))
      .build()
    val evalResult = MLHandler.handleMlCommand(sessionHolder, evalCmd)
    assert(
      evalResult.getParam.getDouble > 2.841 &&
        evalResult.getParam.getDouble < 2.843)

    val ret = readWrite(sessionHolder, getRegressorEvaluator, getMetricName)
    assert(
      ret.getOperatorInfo.getParams.getParamsMap.get("metricName").getString ==
        "mae")
  }

  // Transformer works
  test("VectorAssembler works") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)

    val transformerRelation = proto.MlRelation
      .newBuilder()
      .setTransform(
        proto.MlRelation.Transform
          .newBuilder()
          .setTransformer(getVectorAssembler)
          .setParams(getVectorAssemblerParams)
          .setInput(createMultiColumnLocalRelationProto))
      .build()

    val transRet = MLHandler.transformMLRelation(transformerRelation, sessionHolder)
    Seq("a", "b", "c", "features").foreach(n => assert(transRet.schema.names.contains(n)))
    assert(transRet.schema("features").dataType.isInstanceOf[VectorUDT])
    val rows = transRet.collect()
    assert(rows.mkString(",") === "[1,0,3,[1.0,0.0,3.0]]")

    val ret = readWrite(sessionHolder, getVectorAssembler, getVectorAssemblerParams)
    assert(ret.getOperatorInfo.getParams.getParamsMap.get("outputCol").getString == "features")
    assert(ret.getOperatorInfo.getParams.getParamsMap.get("handleInvalid").getString == "skip")
    assert(
      ret.getOperatorInfo.getParams.getParamsMap
        .get("inputCols")
        .getArray
        .getElementsList
        .asScala
        .map(_.getString)
        .toArray sameElements Array("a", "b", "c"))
  }

  test("tree model training early stop for limiting model size") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    sessionHolder.session.conf
      .set(Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_ENABLED.key, "true")

    for (estimator <- Seq(getDecisionTreeClassifier, getRandomForestClassifier)) {
      for (maxModelSize <- Seq(20000, 50000)) {
        sessionHolder.session.conf.set(
          Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_MAX_MODEL_SIZE.key,
          maxModelSize.toString)
        trainTreeModel(sessionHolder, estimator)
        val lastModelSize = org.apache.spark.ml.tree.impl.RandomForest.lastEarlyStoppedModelSize
        assert(lastModelSize < maxModelSize)
        assert(lastModelSize >= maxModelSize.toDouble / 2.5)
      }
    }
  }

  test("GBT tree model training early stop for limiting model size") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    sessionHolder.session.conf
      .set(Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_ENABLED.key, "true")

    for (maxModelSize <- Seq(20000, 50000, 130000)) {
      sessionHolder.session.conf.set(
        Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_MAX_MODEL_SIZE.key,
        maxModelSize.toString)
      trainTreeModel(sessionHolder, getGBTClassifier)
      val lastModelSize =
        org.apache.spark.ml.tree.impl.GradientBoostedTrees.lastEarlyStoppedModelSize
      assert(lastModelSize < maxModelSize)
      assert(lastModelSize >= maxModelSize.toDouble / 2.5)
    }
  }

  test("MLCache offloading works") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    sessionHolder.session.conf
      .set(Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_ENABLED.key, "true")

    val memorySizeBytes = 1024 * 16
    sessionHolder.session.conf.set(
      Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_MAX_IN_MEMORY_SIZE.key,
      memorySizeBytes)
    val modelIdList = scala.collection.mutable.ListBuffer[String]()
    modelIdList.append(trainLogisticRegressionModel(sessionHolder))
    assert(sessionHolder.mlCache.cachedModel.size() == 1)
    assert(sessionHolder.mlCache.totalMLCacheInMemorySizeBytes.get() > 0)
    val modelSizeBytes = sessionHolder.mlCache.totalMLCacheInMemorySizeBytes.get()
    val maxNumModels = memorySizeBytes / modelSizeBytes.toInt

    // All models will be kept if the total size is less than the memory limit.
    for (i <- 1 until maxNumModels) {
      modelIdList.append(trainLogisticRegressionModel(sessionHolder))
      assert(sessionHolder.mlCache.cachedModel.size() == i + 1)
      assert(sessionHolder.mlCache.totalMLCacheInMemorySizeBytes.get() > 0)
      assert(sessionHolder.mlCache.totalMLCacheInMemorySizeBytes.get() <= memorySizeBytes)
    }

    // Old models will be offloaded
    // if new ones are added and the total size exceeds the memory limit.
    for (_ <- 0 until 3) {
      modelIdList.append(trainLogisticRegressionModel(sessionHolder))
      assert(sessionHolder.mlCache.cachedModel.size() == maxNumModels)
      assert(sessionHolder.mlCache.totalMLCacheInMemorySizeBytes.get() > 0)
      assert(sessionHolder.mlCache.totalMLCacheInMemorySizeBytes.get() <= memorySizeBytes)
    }

    // Assert all models can be loaded back from disk after they are offloaded.
    for (modelId <- modelIdList) {
      assert(sessionHolder.mlCache.get(modelId) != null)
    }
  }

  test("Model size limit") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    sessionHolder.session.conf
      .set(Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_MAX_MODEL_SIZE.key, "4000")
    intercept[MLModelSizeOverflowException] {
      trainLogisticRegressionModel(sessionHolder)
    }
    sessionHolder.session.conf
      .set(Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_MAX_MODEL_SIZE.key, "8000")
    sessionHolder.session.conf
      .set(Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_MAX_STORAGE_SIZE.key, "10000")
    trainLogisticRegressionModel(sessionHolder)
    intercept[MLCacheSizeOverflowException] {
      trainLogisticRegressionModel(sessionHolder)
    }
  }

  def trainTreeModel(
      sessionHolder: SessionHolder,
      estimator: proto.MlOperator.Builder): String = {
    val fitCommand = proto.MlCommand
      .newBuilder()
      .setFit(
        proto.MlCommand.Fit
          .newBuilder()
          .setDataset(createRelationProtoForTreeModel(128, 10000))
          .setEstimator(estimator)
          .setParams(getMaxDepth(30)))
      .build()
    val fitResult = MLHandler.handleMlCommand(sessionHolder, fitCommand)
    fitResult.getOperatorInfo.getObjRef.getId
  }
}
