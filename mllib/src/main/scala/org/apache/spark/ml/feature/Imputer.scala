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
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * :: Experimental ::
 *
 */
@Experimental
class Imputer private[ml](
    override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with MLWritable {

  def this() = this(Identifiable.randomUID("tokenizer"))

  import Imputer._

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  val strategy: Param[String] = new Param(this, "strategy", "strategy for imputation")

  /** @group getParam */
  def getStrategy: String = $(strategy)

  /** @group setParam */
  def setStrategy(value: String): this.type = set(strategy, value)

  setDefault(strategy -> "mean")

  override def transform(dataset: DataFrame): DataFrame = {
    val reScale = udf { (vector: Vector) =>
      if (vector == null) {
        val replacement = $(strategy) match {
          case "mean" =>
            val input = dataset.select($(inputCol)).rdd.filter(r => !r.anyNull)
              .map { case Row(v: Vector) => v }
            val summary = Statistics.colStats(input)
            summary.mean
          case "median" =>
            val df = dataset.select($(inputCol))
            df.registerTempTable("medianTable")
            val median = df.sqlContext
              .sql(s"select percentile(${$(inputCol)}, 0.5) from medianTable")
              .head().getAs[Vector](0)
            median
          case "most" =>
            val input = dataset.select($(inputCol)).rdd.filter(r => !r.anyNull)
              .map { case Row(v: Vector) => v }
            val most = input.map(v => (v, 1)).reduceByKey(_ + _).sortBy(-_._2).take(1)(0)._1
            most
        }
        replacement
      }
      else vector
    }

    dataset.withColumn($(outputCol), reScale(col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateParams()
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${$(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): Imputer = {
    val copied = new Imputer(uid)
    copyValues(copied, extra)
  }

  @Since("1.6.0")
  override def write: MLWriter = new ImputerWriter(this)
}

@Since("1.6.0")
object Imputer extends MLReadable[Imputer] {

  private[Imputer]
  class ImputerWriter(instance: Imputer) extends MLWriter {

    private case class Data(strategy: String)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = new Data(instance.getStrategy)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class ImputerReader extends MLReader[Imputer] {

    private val className = classOf[Imputer].getName

    override def load(path: String): Imputer = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val Row(strategy: String) = sqlContext.read.parquet(dataPath)
        .select("strategy")
        .head()
      val model = new Imputer(metadata.uid).setStrategy(strategy)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[Imputer] = new ImputerReader

  @Since("1.6.0")
  override def load(path: String): Imputer = super.load(path)
}
