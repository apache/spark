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

import java.io.File
import java.util.Optional

import org.apache.spark.SparkFunSuite
import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Transformer}
import org.apache.spark.ml.classification.ClassificationModel
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasHandleInvalid, HasInputCols, HasMaxIter, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, MLReadable, MLReader}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connect.planner.SparkConnectPlanTest
import org.apache.spark.sql.connect.plugin.MLBackendPlugin
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, Metadata, StructField, StructType}
import org.apache.spark.util.Utils

trait MLHelper extends SparkFunSuite with SparkConnectPlanTest {

  def createLocalRelationProto: proto.Relation = {
    val udt = new VectorUDT()
    val rows = Seq(
      InternalRow(1.0f, udt.serialize(Vectors.dense(Array(1.0, 2.0)))),
      InternalRow(1.0f, udt.serialize(Vectors.dense(Array(2.0, -1.0)))),
      InternalRow(0.0f, udt.serialize(Vectors.dense(Array(-3.0, -2.0)))),
      InternalRow(0.0f, udt.serialize(Vectors.dense(Array(-1.0, -2.0)))))

    val schema = StructType(
      Seq(
        StructField("label", FloatType),
        StructField("features", new VectorUDT(), false, Metadata.empty)))

    val inputRows = rows.map { row =>
      val proj = UnsafeProjection.create(schema)
      proj(row).copy()
    }
    createLocalRelationProto(DataTypeUtils.toAttributes(schema), inputRows, "UTC", Some(schema))
  }

  def createRegressionEvaluationLocalRelationProto: proto.Relation = {
    // The test refers to
    // https://github.com/apache/spark/blob/master/python/pyspark/ml/evaluation.py#L331
    val rows = Seq(
      InternalRow(-28.98343821, -27.0),
      InternalRow(20.21491975, 21.5),
      InternalRow(-25.98418959, -22.0),
      InternalRow(30.69731842, 33.0),
      InternalRow(74.69283752, 71.0))
    val schema = StructType(Seq(StructField("raw", DoubleType), StructField("label", DoubleType)))
    val inputRows = rows.map { row =>
      val proj = UnsafeProjection.create(schema)
      proj(row).copy()
    }
    createLocalRelationProto(schema, inputRows)
  }

  def createMultiColumnLocalRelationProto: proto.Relation = {
    val rows = Seq(InternalRow(1, 0, 3))
    val schema = StructType(
      Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)))
    val inputRows = rows.map { row =>
      val proj = UnsafeProjection.create(schema)
      proj(row).copy()
    }
    createLocalRelationProto(schema, inputRows)
  }

  def createRelationProtoForTreeModel(numFeatures: Int, numInstances: Int): proto.Relation = {
    val udt = new VectorUDT()
    val rows = new Array[InternalRow](numInstances)
    for (i <- 0 until numInstances) {
      val label = if (i < numInstances / 10) {
        0.0f
      } else if (i < numInstances / 2) {
        1.0f
      } else if (i < numInstances * 0.9) {
        0.0f
      } else {
        1.0f
      }
      val features = Array.tabulate[Double](numFeatures)(index =>
        scala.math.cos(i.toDouble * scala.math.pow(2, index)))
      rows(i) = InternalRow(label, udt.serialize(Vectors.dense(features)))
    }
    val schema = StructType(
      Seq(
        StructField("label", FloatType),
        StructField("features", new VectorUDT(), false, Metadata.empty)))

    val inputRows = rows.toSeq.map { row =>
      val proj = UnsafeProjection.create(schema)
      proj(row).copy()
    }
    createLocalRelationProto(DataTypeUtils.toAttributes(schema), inputRows, "UTC", Some(schema))
  }

  def getLogisticRegression: proto.MlOperator.Builder =
    proto.MlOperator
      .newBuilder()
      .setName("org.apache.spark.ml.classification.LogisticRegression")
      .setUid("LogisticRegression")
      .setType(proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR)

  def getDecisionTreeClassifier: proto.MlOperator.Builder =
    proto.MlOperator
      .newBuilder()
      .setName("org.apache.spark.ml.classification.DecisionTreeClassifier")
      .setUid("DecisionTreeClassifier")
      .setType(proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR)

  def getRandomForestClassifier: proto.MlOperator.Builder =
    proto.MlOperator
      .newBuilder()
      .setName("org.apache.spark.ml.classification.RandomForestClassifier")
      .setUid("RandomForestClassifier")
      .setType(proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR)

  def getGBTClassifier: proto.MlOperator.Builder =
    proto.MlOperator
      .newBuilder()
      .setName("org.apache.spark.ml.classification.GBTClassifier")
      .setUid("GBTClassifier")
      .setType(proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR)

  def getMaxIter: proto.MlParams.Builder =
    proto.MlParams
      .newBuilder()
      .putParams("maxIter", proto.Expression.Literal.newBuilder().setInteger(2).build())

  def getMaxDepth(maxDepth: Int): proto.MlParams.Builder =
    proto.MlParams
      .newBuilder()
      .putParams("maxDepth", proto.Expression.Literal.newBuilder().setInteger(maxDepth).build())

  def getRegressorEvaluator: proto.MlOperator.Builder =
    proto.MlOperator
      .newBuilder()
      .setName("org.apache.spark.ml.evaluation.RegressionEvaluator")
      .setUid("RegressionEvaluator")
      .setType(proto.MlOperator.OperatorType.OPERATOR_TYPE_EVALUATOR)

  def getMetricName: proto.MlParams.Builder =
    proto.MlParams
      .newBuilder()
      .putParams("metricName", proto.Expression.Literal.newBuilder().setString("mae").build())

  def fetchCommand(modelId: String, method: String): proto.MlCommand = {
    proto.MlCommand
      .newBuilder()
      .setFetch(
        proto.Fetch
          .newBuilder()
          .setObjRef(proto.ObjectRef.newBuilder().setId(modelId))
          .addMethods(proto.Fetch.Method.newBuilder().setMethod(method)))
      .build()
  }

  def getArrayStrings: proto.Expression.Literal =
    proto.Expression.Literal
      .newBuilder()
      .setArray(
        proto.Expression.Literal.Array
          .newBuilder()
          .setElementType(proto.DataType
            .newBuilder()
            .setString(proto.DataType.String.getDefaultInstance)
            .build())
          .addElements(proto.Expression.Literal.newBuilder().setString("a"))
          .addElements(proto.Expression.Literal.newBuilder().setString("b"))
          .addElements(proto.Expression.Literal.newBuilder().setString("c"))
          .build())
      .build()

  def getVectorAssembler: proto.MlOperator.Builder =
    proto.MlOperator
      .newBuilder()
      .setUid("vec")
      .setName("org.apache.spark.ml.feature.VectorAssembler")
      .setType(proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER)

  def getVectorAssemblerParams: proto.MlParams.Builder =
    proto.MlParams
      .newBuilder()
      .putParams("handleInvalid", proto.Expression.Literal.newBuilder().setString("skip").build())
      .putParams("outputCol", proto.Expression.Literal.newBuilder().setString("features").build())
      .putParams("inputCols", getArrayStrings)

  def readWrite(
      sessionHolder: SessionHolder,
      operator: proto.MlOperator.Builder,
      params: proto.MlParams.Builder): proto.MlCommandResult = {
    // read/write
    val tempDir = Utils.createTempDir(namePrefix = this.getClass.getName)
    try {
      val path = new File(tempDir, Identifiable.randomUID("test")).getPath
      val writeCmd = proto.MlCommand
        .newBuilder()
        .setWrite(
          proto.MlCommand.Write
            .newBuilder()
            .setOperator(operator)
            .setParams(params)
            .setPath(path)
            .setShouldOverwrite(true))
        .build()
      MLHandler.handleMlCommand(sessionHolder, writeCmd)

      val readCmd = proto.MlCommand
        .newBuilder()
        .setRead(
          proto.MlCommand.Read
            .newBuilder()
            .setOperator(operator)
            .setPath(path))
        .build()

      MLHandler.handleMlCommand(sessionHolder, readCmd)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  def readWrite(
      sessionHolder: SessionHolder,
      modelId: String,
      clsName: String): proto.MlCommandResult = {
    val tempDir = Utils.createTempDir(namePrefix = this.getClass.getName)
    try {
      val path = new File(tempDir, Identifiable.randomUID("test")).getPath
      val writeCmd = proto.MlCommand
        .newBuilder()
        .setWrite(
          proto.MlCommand.Write
            .newBuilder()
            .setObjRef(proto.ObjectRef.newBuilder().setId(modelId))
            .setPath(path)
            .setShouldOverwrite(true))
        .build()
      MLHandler.handleMlCommand(sessionHolder, writeCmd)

      val readCmd = proto.MlCommand
        .newBuilder()
        .setRead(
          proto.MlCommand.Read
            .newBuilder()
            .setOperator(
              proto.MlOperator
                .newBuilder()
                .setName(clsName)
                .setType(proto.MlOperator.OperatorType.OPERATOR_TYPE_MODEL))
            .setPath(path))
        .build()

      MLHandler.handleMlCommand(sessionHolder, readCmd)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

}

class MyMlBackend extends MLBackendPlugin {

  override def transform(mlName: String): Optional[String] = {
    mlName match {
      case "org.apache.spark.ml.classification.LogisticRegression" =>
        Optional.of("org.apache.spark.sql.connect.ml.MyLogisticRegression")
      case "org.apache.spark.ml.classification.LogisticRegressionModel" =>
        Optional.of("org.apache.spark.sql.connect.ml.MyLogisticRegressionModel")
      case "org.apache.spark.ml.evaluation.RegressionEvaluator" =>
        Optional.of("org.apache.spark.sql.connect.ml.MyRegressionEvaluator")
      case "org.apache.spark.ml.feature.VectorAssembler" =>
        Optional.of("org.apache.spark.sql.connect.ml.MyVectorAssembler")
      case _ => Optional.empty()
    }
  }
}

trait HasFakedParam extends Params {
  final val fakeParam: IntParam = new IntParam(this, "fakeParam", "faked parameter")
}

class MyVectorAssembler(override val uid: String)
    extends Transformer
    with HasInputCols
    with HasOutputCol
    with HasHandleInvalid
    with HasFakedParam
    with DefaultParamsWritable {
  set(fakeParam, 101010)
  private[spark] def this() = this(Identifiable.randomUID("MyVectorAssembler"))
  override def transform(dataset: Dataset[_]): DataFrame =
    dataset.withColumn("new", lit(1))
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
  override def transformSchema(schema: StructType): StructType = schema
}

object MyVectorAssembler extends DefaultParamsReadable[MyVectorAssembler] {
  override def load(path: String): MyVectorAssembler = super.load(path)
}

class MyRegressionEvaluator(override val uid: String)
    extends Evaluator
    with DefaultParamsWritable
    with HasFakedParam {

  def this() = this(Identifiable.randomUID("MyRegressionEvaluator"))

  // keep same as RegressionEvaluator
  val metricName: Param[String] = {
    new Param(this, "metricName", "metric name in evaluation (mse|rmse|r2|mae|var)")
  }

  set(fakeParam, 101010)

  override def evaluate(dataset: Dataset[_]): Double = 1.11

  override def copy(extra: ParamMap): Evaluator = defaultCopy(extra)
}

object MyRegressionEvaluator extends DefaultParamsReadable[MyRegressionEvaluator] {
  override def load(path: String): MyRegressionEvaluator = super.load(path)
}

class MyLogisticRegressionModel(
    override val uid: String,
    val intercept: Float,
    val coefficients: Float)
    extends ClassificationModel[Double, MyLogisticRegressionModel]
    with HasMaxIter
    with HasFakedParam
    with DefaultParamsWritable {

  private[spark] def this() = this("MyLogisticRegressionModel", 1.0f, 1.0f)

  def setFakeParam(v: Int): this.type = set(fakeParam, v)

  def setMaxIter(v: Int): this.type = set(maxIter, v)

  override def copy(extra: ParamMap): MyLogisticRegressionModel = {
    copyValues(new MyLogisticRegressionModel(uid, intercept, coefficients), extra)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.toDF()
  }

  // fake a function
  def predictRaw: Double = 1.11

  override def predictRaw(features: Double): Vector = Vectors.dense(1.11)

  override def transformSchema(schema: StructType): StructType = schema

  override def numClasses: Int = 2
}

object MyLogisticRegressionModel extends MLReadable[MyLogisticRegressionModel] {

  // No need to load from file.
  override def read: MLReader[MyLogisticRegressionModel] =
    (_: String) => {
      new MyLogisticRegressionModel("MyLogisticRegressionModel", 3.5f, 4.6f)
        .setMaxIter(2)
        .setFakeParam(101010)
    }
}

class MyLogisticRegression(override val uid: String)
    extends Estimator[MyLogisticRegressionModel]
    with HasMaxIter
    with HasFakedParam
    with DefaultParamsWritable {
  set(fakeParam, 101010)

  def this() = this(Identifiable.randomUID("MyLogisticRegression"))

  override def fit(dataset: Dataset[_]): MyLogisticRegressionModel = {
    copyValues(new MyLogisticRegressionModel(uid, 3.5f, 4.6f))
  }

  override def copy(extra: ParamMap): MyLogisticRegression = {
    defaultCopy(extra).asInstanceOf[MyLogisticRegression]
  }

  override def transformSchema(schema: StructType): StructType = schema
}

object MyLogisticRegression extends DefaultParamsReadable[MyLogisticRegression] {
  override def load(path: String): MyLogisticRegression = super.load(path)
}

object NotImplementingMLReadble {
  def load(path: String): Unit = {}
}
