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

import org.apache.spark.SparkFunSuite
import org.apache.spark.connect.proto
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.{Vectors, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.planner.SparkConnectPlanTest
import org.apache.spark.sql.types.{FloatType, Metadata, StructField, StructType}
import org.apache.spark.util.Utils

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

class MLSuite extends SparkFunSuite with SparkConnectPlanTest {

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

  test("reconcileParam") {
    val fakedML = new FakedML
    val params = proto.MlParams
      .newBuilder()
      .putParams(
        "boolean",
        proto.Param
          .newBuilder()
          .setLiteral(proto.Expression.Literal.newBuilder().setBoolean(true))
          .build())
      .putParams(
        "double",
        proto.Param
          .newBuilder()
          .setLiteral(proto.Expression.Literal.newBuilder().setDouble(1.0))
          .build())
      .putParams(
        "int",
        proto.Param
          .newBuilder()
          .setLiteral(proto.Expression.Literal.newBuilder().setInteger(10))
          .build())
      .putParams(
        "float",
        proto.Param
          .newBuilder()
          .setLiteral(proto.Expression.Literal.newBuilder().setFloat(10.0f))
          .build())
      .putParams(
        "arrayString",
        proto.Param
          .newBuilder()
          .setLiteral(
            proto.Expression.Literal
              .newBuilder()
              .setArray(
                proto.Expression.Literal.Array
                  .newBuilder()
                  .setElementType(proto.DataType
                    .newBuilder()
                    .setString(proto.DataType.String.getDefaultInstance)
                    .build())
                  .addElements(proto.Expression.Literal.newBuilder().setString("hello"))
                  .addElements(proto.Expression.Literal.newBuilder().setString("world"))
                  .build())
              .build())
          .build())
      .putParams(
        "arrayInt",
        proto.Param
          .newBuilder()
          .setLiteral(
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
          .build())
      .putParams(
        "arrayDouble",
        proto.Param
          .newBuilder()
          .setLiteral(
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
          .build())
      .build()
    MLUtils.setInstanceParams(fakedML, params)
    assert(fakedML.getInt === 10)
    assert(fakedML.getFloat === 10.0)
    assert(fakedML.getArrayInt === Array(1, 2))
    assert(fakedML.getArrayDouble === Array(11.0, 12.0))
    assert(fakedML.getArrayString === Array("hello", "world"))
    assert(fakedML.getBoolean === true)
    assert(fakedML.getDouble === 1.0)
  }

  test("LogisticRegression works") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)

    def verifyModel(modelId: String, hasSummary: Boolean = false): Unit = {
      val model = sessionHolder.mlCache.get(modelId)
      // Model is cached
      assert(model != null)
      assert(model.isInstanceOf[LogisticRegressionModel])
      val lrModel = model.asInstanceOf[LogisticRegressionModel]
      assert(lrModel.getMaxIter === 2)

      // Fetch double attribute
      val interceptCommand = proto.MlCommand
        .newBuilder()
        .setFetch(
          proto.Fetch
            .newBuilder()
            .setObjRef(proto.ObjectRef.newBuilder().setId(modelId))
            .addMethods(proto.Fetch.Method.newBuilder().setMethod("intercept")))
        .build()
      val interceptResult = MLHandler.handleMlCommand(sessionHolder, interceptCommand)
      assert(interceptResult.getParam.getLiteral.getDouble === lrModel.intercept)

      // Fetch Vector attribute
      val coefficientsCommand = proto.MlCommand
        .newBuilder()
        .setFetch(
          proto.Fetch
            .newBuilder()
            .setObjRef(proto.ObjectRef.newBuilder().setId(modelId))
            .addMethods(proto.Fetch.Method.newBuilder().setMethod("coefficients")))
        .build()
      val coefficientsResult = MLHandler.handleMlCommand(sessionHolder, coefficientsCommand)
      val deserializedCoefficients =
        MLUtils.deserializeVector(coefficientsResult.getParam.getVector)
      assert(deserializedCoefficients === lrModel.coefficients)

      // Fetch Matrix attribute
      val coefficientsMatrixCommand = proto.MlCommand
        .newBuilder()
        .setFetch(
          proto.Fetch
            .newBuilder()
            .setObjRef(proto.ObjectRef.newBuilder().setId(modelId))
            .addMethods(proto.Fetch.Method.newBuilder().setMethod("coefficientMatrix")))
        .build()
      val coefficientsMatrixResult =
        MLHandler.handleMlCommand(sessionHolder, coefficientsMatrixCommand)
      val deserializedCoefficientsMatrix =
        MLUtils.deserializeMatrix(coefficientsMatrixResult.getParam.getMatrix)
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
      val predictValue = predictResult.getParam.getLiteral.getDouble
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
        assert(lrModel.summary.accuracy === accuracyResult.getParam.getLiteral.getDouble)

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
            weightedFMeasureResult.getParam.getLiteral.getDouble)
      }
    }

    try {
      val fitCommand = proto.MlCommand
        .newBuilder()
        .setFit(
          proto.MlCommand.Fit
            .newBuilder()
            .setDataset(createLocalRelationProto)
            .setEstimator(
              proto.MlOperator
                .newBuilder()
                .setName("org.apache.spark.ml.classification.LogisticRegression")
                .setUid("LogisticRegression")
                .setType(proto.MlOperator.OperatorType.ESTIMATOR))
            .setParams(
              proto.MlParams
                .newBuilder()
                .putParams(
                  "maxIter",
                  proto.Param
                    .newBuilder()
                    .setLiteral(proto.Expression.Literal
                      .newBuilder()
                      .setInteger(2))
                    .build())))
        .build()
      val fitResult = MLHandler.handleMlCommand(sessionHolder, fitCommand)
      val modelId = fitResult.getOperatorInfo.getObjRef.getId

      verifyModel(modelId, true)

      // read/write
      val tempDir = Utils.createTempDir(namePrefix = this.getClass.getName)
      try {
        val path = new File(tempDir, Identifiable.randomUID("LogisticRegression")).getPath
        val writeCmd = proto.MlCommand
          .newBuilder()
          .setWrite(
            proto.MlCommand.Write
              .newBuilder()
              .setPath(path)
              .setObjRef(proto.ObjectRef.newBuilder().setId(modelId)))
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
                  .setName("org.apache.spark.ml.classification.LogisticRegressionModel")
                  .setType(proto.MlOperator.OperatorType.MODEL))
              .setPath(path))
          .build()

        val readResult = MLHandler.handleMlCommand(sessionHolder, readCmd)
        verifyModel(readResult.getOperatorInfo.getObjRef.getId)

      } finally {
        Utils.deleteRecursively(tempDir)
      }

    } finally {
      sessionHolder.mlCache.clear()
    }
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
                .setType(proto.MlOperator.OperatorType.ESTIMATOR)))
        .build()
      MLHandler.handleMlCommand(sessionHolder, command)
    }
  }
}
