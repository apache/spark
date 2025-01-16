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

import java.util.Optional

import org.apache.spark.SparkFunSuite
import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.{Vectors, VectorUDT}
import org.apache.spark.ml.param.{IntParam, ParamMap, Params}
import org.apache.spark.ml.param.shared.HasMaxIter
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, MLReadable, MLReader}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connect.planner.SparkConnectPlanTest
import org.apache.spark.sql.connect.plugin.MLBackendPlugin
import org.apache.spark.sql.types.{FloatType, Metadata, StructField, StructType}

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
}

class MyMlBackend extends MLBackendPlugin {

  override def transform(mlName: String): Optional[String] = {
    mlName match {
      case "org.apache.spark.ml.classification.LogisticRegression" =>
        Optional.of("org.apache.spark.sql.connect.ml.MyLogisticRegression")
      case "org.apache.spark.ml.classification.LogisticRegressionModel" =>
        Optional.of("org.apache.spark.sql.connect.ml.MyLogisticRegressionModel")
      case _ => Optional.empty()
    }
  }
}

trait HasFakedParam extends Params {
  final val fakeParam: IntParam = new IntParam(this, "fakeParam", "faked parameter")
}

class MyLogisticRegressionModel(
    override val uid: String,
    val intercept: Float,
    val coefficients: Float)
    extends Model[MyLogisticRegressionModel]
    with HasMaxIter
    with HasFakedParam
    with DefaultParamsWritable {

  def setFakeParam(v: Int): this.type = set(fakeParam, v)

  def setMaxIter(v: Int): this.type = set(maxIter, v)

  override def copy(extra: ParamMap): MyLogisticRegressionModel = {
    copyValues(new MyLogisticRegressionModel(uid, intercept, coefficients), extra)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.toDF()
  }

  override def transformSchema(schema: StructType): StructType = schema
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
