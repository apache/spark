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

package org.apache.spark.ml.feature

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Params for [[MaxAbsScaler]] and [[MaxAbsScalerModel]].
 */
private[feature] trait MaxAbsScalerParams extends Params with HasInputCol with HasOutputCol {

   /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }
}

/**
 * Rescale each feature individually to range [-1, 1] by dividing through the largest maximum
 * absolute value in each feature. It does not shift/center the data, and thus does not destroy
 * any sparsity.
 */
@Since("2.0.0")
class MaxAbsScaler @Since("2.0.0") (@Since("2.0.0") override val uid: String)
  extends Estimator[MaxAbsScalerModel] with MaxAbsScalerParams with DefaultParamsWritable {

  @Since("2.0.0")
  def this() = this(Identifiable.randomUID("maxAbsScal"))

  /** @group setParam */
  @Since("2.0.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): MaxAbsScalerModel = {
    transformSchema(dataset.schema, logging = true)

    val Row(max: Vector, min: Vector) = dataset
      .select(Summarizer.metrics("max", "min").summary(col($(inputCol))).as("summary"))
      .select("summary.max", "summary.min")
      .first()

    val maxAbs = Array.tabulate(max.size) { i => math.max(math.abs(min(i)), math.abs(max(i))) }

    copyValues(new MaxAbsScalerModel(uid, Vectors.dense(maxAbs).compressed).setParent(this))
  }

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("2.0.0")
  override def copy(extra: ParamMap): MaxAbsScaler = defaultCopy(extra)
}

@Since("2.0.0")
object MaxAbsScaler extends DefaultParamsReadable[MaxAbsScaler] {

  @Since("2.0.0")
  override def load(path: String): MaxAbsScaler = super.load(path)
}

/**
 * Model fitted by [[MaxAbsScaler]].
 *
 */
@Since("2.0.0")
class MaxAbsScalerModel private[ml] (
    @Since("2.0.0") override val uid: String,
    @Since("2.0.0") val maxAbs: Vector)
  extends Model[MaxAbsScalerModel] with MaxAbsScalerParams with MLWritable {

  import MaxAbsScalerModel._

  /** @group setParam */
  @Since("2.0.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    val scale = maxAbs.toArray.map { v => if (v == 0) 1.0 else 1 / v }
    val func = StandardScalerModel.getTransformFunc(
      Array.empty, scale, false, true)
    val transformer = udf(func)

    dataset.withColumn($(outputCol), transformer(col($(inputCol))),
      outputSchema($(outputCol)).metadata)
  }

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema)
    if ($(outputCol).nonEmpty) {
      outputSchema = SchemaUtils.updateAttributeGroupSize(outputSchema,
        $(outputCol), maxAbs.size)
    }
    outputSchema
  }

  @Since("2.0.0")
  override def copy(extra: ParamMap): MaxAbsScalerModel = {
    val copied = new MaxAbsScalerModel(uid, maxAbs)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new MaxAbsScalerModelWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"MaxAbsScalerModel: uid=$uid, numFeatures=${maxAbs.size}"
  }
}

@Since("2.0.0")
object MaxAbsScalerModel extends MLReadable[MaxAbsScalerModel] {

  private[MaxAbsScalerModel]
  class MaxAbsScalerModelWriter(instance: MaxAbsScalerModel) extends MLWriter {

    private case class Data(maxAbs: Vector)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)
      val data = new Data(instance.maxAbs)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).write.parquet(dataPath)
    }
  }

  private class MaxAbsScalerModelReader extends MLReader[MaxAbsScalerModel] {

    private val className = classOf[MaxAbsScalerModel].getName

    override def load(path: String): MaxAbsScalerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)
      val dataPath = new Path(path, "data").toString
      val Row(maxAbs: Vector) = sparkSession.read.parquet(dataPath)
        .select("maxAbs")
        .head()
      val model = new MaxAbsScalerModel(metadata.uid, maxAbs)
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("2.0.0")
  override def read: MLReader[MaxAbsScalerModel] = new MaxAbsScalerModelReader

  @Since("2.0.0")
  override def load(path: String): MaxAbsScalerModel = super.load(path)
}
