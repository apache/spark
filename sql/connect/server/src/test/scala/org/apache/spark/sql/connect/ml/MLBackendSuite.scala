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

import org.apache.spark.SparkEnv
import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.util.Utils

class MLBackendSuite extends MLHelper {

  def withSparkConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val conf = SparkEnv.get.conf
    pairs.foreach { kv => conf.set(kv._1, kv._2) }
    try f
    finally {
      pairs.foreach { kv => conf.remove(kv._1) }
    }
  }

  private def getLogisticRegressionBuilder: proto.MlOperator.Builder = {
    val name = "org.apache.spark.ml.classification.LogisticRegression"
    proto.MlOperator
      .newBuilder()
      .setName(name)
      .setUid(name)
      .setType(proto.MlOperator.OperatorType.ESTIMATOR)
  }

  private def getMaxIterBuilder: proto.MlParams.Builder = {
    proto.MlParams
      .newBuilder()
      .putParams(
        "maxIter",
        proto.Expression.Literal
          .newBuilder()
          .setInteger(2)
          .build())
  }

  test("ML backend: estimator works") {
    withSparkConf(
      Connect.CONNECT_ML_BACKEND_CLASSES.key ->
        "org.apache.spark.sql.connect.ml.MyMlBackend") {
      val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)

      val fitCommand = proto.MlCommand
        .newBuilder()
        .setFit(
          proto.MlCommand.Fit
            .newBuilder()
            .setDataset(createLocalRelationProto)
            .setEstimator(getLogisticRegressionBuilder)
            .setParams(getMaxIterBuilder))
        .build()
      val fitResult = MLHandler.handleMlCommand(sessionHolder, fitCommand)
      val modelId = fitResult.getOperatorInfo.getObjRef.getId
      assert(sessionHolder.mlCache.get(modelId).isInstanceOf[MyLogisticRegressionModel])
      val model = sessionHolder.mlCache.get(modelId).asInstanceOf[MyLogisticRegressionModel]
      assert(model.intercept == 3.5f)
      assert(model.coefficients == 4.6f)

      val ret = readWrite(sessionHolder, getLogisticRegressionBuilder, getMaxIterBuilder)

      assert(ret.getOperatorInfo.getParams.getParamsMap.containsKey("fakeParam"))
      assert(ret.getOperatorInfo.getParams.getParamsMap.containsKey("maxIter"))
      assert(ret.getOperatorInfo.getParams.getParamsMap.get("maxIter").getInteger == 2)
      assert(ret.getOperatorInfo.getParams.getParamsMap.get("fakeParam").getInteger == 101010)
    }
  }

  test("ML backend: model works") {
    withSparkConf(
      Connect.CONNECT_ML_BACKEND_CLASSES.key ->
        "org.apache.spark.sql.connect.ml.MyMlBackend") {
      val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)

      val tempDir = Utils.createTempDir(namePrefix = this.getClass.getName)
      try {
        // Train a model
        val fitCommand = proto.MlCommand
          .newBuilder()
          .setFit(
            proto.MlCommand.Fit
              .newBuilder()
              .setDataset(createLocalRelationProto)
              .setEstimator(getLogisticRegressionBuilder)
              .setParams(getMaxIterBuilder))
          .build()
        val fitRet = MLHandler.handleMlCommand(sessionHolder, fitCommand)

        val ret = readWrite(
          sessionHolder,
          fitRet.getOperatorInfo.getObjRef.getId,
          "org.apache.spark.ml.classification.LogisticRegressionModel")
        assert(
          ret.getOperatorInfo.getParams.getParamsMap.get("maxIter").getInteger
            == 2)
        assert(
          ret.getOperatorInfo.getParams.getParamsMap.get("fakeParam").getInteger
            == 101010)

        // Fetch double attribute
        val fakeAttrCmd = fetchCommand(ret.getOperatorInfo.getObjRef.getId, "predictRaw")
        val fakeAttrRet = MLHandler.handleMlCommand(sessionHolder, fakeAttrCmd)
        assert(fakeAttrRet.getParam.getDouble === 1.11)
      } finally {
        Utils.deleteRecursively(tempDir)
      }
    }
  }

  test("ML backend: evaluator works") {
    withSparkConf(
      Connect.CONNECT_ML_BACKEND_CLASSES.key ->
        "org.apache.spark.sql.connect.ml.MyMlBackend") {
      val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)

      val evalCmd = proto.MlCommand
        .newBuilder()
        .setEvaluate(
          proto.MlCommand.Evaluate
            .newBuilder()
            .setDataset(createRegressionEvaluationLocalRelationProto)
            .setEvaluator(getRegressorEvaluator))
        .build()
      val evalResult = MLHandler.handleMlCommand(sessionHolder, evalCmd)
      assert(evalResult.getParam.getDouble == 1.11)

      val ret = readWrite(sessionHolder, getRegressorEvaluator, getMetricName)

      assert(ret.getOperatorInfo.getParams.getParamsMap.containsKey("fakeParam"))
      assert(ret.getOperatorInfo.getParams.getParamsMap.containsKey("metricName"))
      assert(
        ret.getOperatorInfo.getParams.getParamsMap.get("metricName").getString
          == "mae")
      assert(
        ret.getOperatorInfo.getParams.getParamsMap.get("fakeParam").getInteger
          == 101010)
    }
  }

  test("ML backend: transformer works") {
    withSparkConf(
      Connect.CONNECT_ML_BACKEND_CLASSES.key ->
        "org.apache.spark.sql.connect.ml.MyMlBackend") {
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
      // MyVectorAssembler has hacked the transform function
      Seq("a", "b", "c", "new").foreach(n => assert(transRet.schema.names.contains(n)))

      val ret = readWrite(sessionHolder, getVectorAssembler, getVectorAssemblerParams)
      assert(
        ret.getOperatorInfo.getParams.getParamsMap.get("handleInvalid").getString
          == "skip")
      assert(
        ret.getOperatorInfo.getParams.getParamsMap.get("fakeParam").getInteger
          == 101010)
      assert(
        ret.getOperatorInfo.getParams.getParamsMap
          .get("inputCols")
          .getArray
          .getElementsList
          .asScala
          .map(_.getString)
          .toArray sameElements Array("a", "b", "c"))
    }
  }

}
