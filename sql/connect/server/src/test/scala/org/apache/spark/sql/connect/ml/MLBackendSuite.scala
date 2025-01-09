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

import org.apache.spark.{SparkEnv, SparkFunSuite}
import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.{Vectors, VectorUDT}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasMaxIter
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.planner.SparkConnectPlanTest
import org.apache.spark.sql.types.{FloatType, Metadata, StructField, StructType}

class MyLogisticRegressionModel(override val uid: String,
                                val intercept: Float,
                                val coefficients: Float)
  extends Model[MyLogisticRegressionModel] with HasMaxIter {

  override def copy(extra: ParamMap): MyLogisticRegressionModel =
    defaultCopy(extra).asInstanceOf[MyLogisticRegressionModel]

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.toDF()
  }

  override def transformSchema(schema: StructType): StructType = schema
}

class MyLogisticRegression(override val uid: String) extends Estimator[MyLogisticRegressionModel]
  with HasMaxIter {
  setDefault(maxIter, 100)
  def this() = this(Identifiable.randomUID("MyLogisticRegression"))

  override def fit(dataset: Dataset[_]): MyLogisticRegressionModel = {
    new MyLogisticRegressionModel(uid, 3.5f, 4.6f)
  }

  override def copy(extra: ParamMap): MyLogisticRegression = {
    defaultCopy(extra).asInstanceOf[MyLogisticRegression]
  }

  override def transformSchema(schema: StructType): StructType = schema
}

class MLBackendSuite extends SparkFunSuite with SparkConnectPlanTest {

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

  def withSparkConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val conf = SparkEnv.get.conf
    pairs.foreach { kv => conf.set(kv._1, kv._2) }
    try f
    finally {
      pairs.foreach { kv => conf.remove(kv._1) }
    }
  }

  test("MlBackendSuite") {
    withSparkConf(
      (Connect.CONNECT_EXTENSIONS_ML_OVERRIDES.key,
        "org.apache.spark.ml.classification.LogisticRegression=" +
          "org.apache.spark.sql.connect.ml.MyLogisticRegression")) {
      val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
      val myEstClass = classOf[MyLogisticRegression]
      try {
        MLUtils.addEstimator(myEstClass.getName, myEstClass)

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
        assert(sessionHolder.mlCache.get(modelId).isInstanceOf[MyLogisticRegressionModel])
        val model = sessionHolder.mlCache.get(modelId).asInstanceOf[MyLogisticRegressionModel]
        assert(model.intercept == 3.5f)
        assert(model.coefficients == 4.6f)
      } finally {
        MLUtils.removeEstimator(myEstClass.getName)
      }
    }
  }
}
