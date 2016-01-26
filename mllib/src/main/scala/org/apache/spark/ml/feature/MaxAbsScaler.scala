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
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Params for [[MaxAbsScaler]] and [[MaxAbsScalerModel]].
 */
private[feature] trait MaxAbsScalerParams extends Params with HasInputCol with HasOutputCol {

   /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    validateParams()
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${$(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }
}

/**
 * :: Experimental ::
 * Rescale each feature individually such that the maximal absolute value of each feature in
 * the training set will be 1.0. It does not shift/center the data, and thus does not destroy
 * any sparsity.
 */
@Experimental
class MaxAbsScaler(override val uid: String)
  extends Estimator[MaxAbsScalerModel] with MaxAbsScalerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("minMaxScal"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: DataFrame): MaxAbsScalerModel = {
    transformSchema(dataset.schema, logging = true)
    val input = dataset.select($(inputCol)).map { case Row(v: Vector) => v }
    val summary = Statistics.colStats(input)
    val maxAbs = summary.min.toArray.zip(summary.max.toArray).map { case (min, max) => 
      val absMin = math.abs(min)
      val absMax = math.abs(max)
      if(absMax > absMin) absMax else absMin
    }    

    copyValues(new MaxAbsScalerModel(uid, Vectors.dense(maxAbs)).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): MaxAbsScaler = defaultCopy(extra)
}

@Since("1.6.0")
object MaxAbsScaler extends DefaultParamsReadable[MaxAbsScaler] {

  @Since("1.6.0")
  override def load(path: String): MaxAbsScaler = super.load(path)
}

/**
 * :: Experimental ::
 * Model fitted by [[MaxAbsScaler]].
 *
 */
@Experimental
class MaxAbsScalerModel private[ml] (
    override val uid: String,
    val maxAbs: Vector)
  extends Model[MaxAbsScalerModel] with MaxAbsScalerParams with MLWritable {

  import MaxAbsScalerModel._

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    val reScale = udf { (vector: Vector) =>
      val brz = vector.toBreeze / maxAbs.toBreeze
      Vectors.fromBreeze(brz)
    }
    dataset.withColumn($(outputCol), reScale(col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): MaxAbsScalerModel = {
    val copied = new MaxAbsScalerModel(uid, maxAbs)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new MinMaxScalerModelWriter(this)
}

@Since("1.6.0")
object MaxAbsScalerModel extends MLReadable[MaxAbsScalerModel] {

  private[MaxAbsScalerModel]
  class MinMaxScalerModelWriter(instance: MaxAbsScalerModel) extends MLWriter {

    private case class Data(maxAbs: Vector)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = new Data(instance.maxAbs)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class MinMaxScalerModelReader extends MLReader[MaxAbsScalerModel] {

    private val className = classOf[MaxAbsScalerModel].getName

    override def load(path: String): MaxAbsScalerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val Row(maxAbs: Vector) = sqlContext.read.parquet(dataPath)
        .select("maxAbs")
        .head()
      val model = new MaxAbsScalerModel(metadata.uid, maxAbs)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[MaxAbsScalerModel] = new MinMaxScalerModelReader

  @Since("1.6.0")
  override def load(path: String): MaxAbsScalerModel = super.load(path)
}
