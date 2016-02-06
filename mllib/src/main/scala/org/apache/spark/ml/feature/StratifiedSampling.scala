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
import org.apache.spark.ml.feature.StratifiedSampling.StratifiedSamplingWriter
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.Utils

/**
 * :: Experimental ::
 *
 * Stratified sampling on the DataFrame according to the keys in a specific label column. User
 * can set 'fraction' to set different sampling rate for each key.
 *
 * @param withReplacement can elements be sampled multiple times (replaced when sampled out)
 * @param fraction expected size of the sample as a fraction of the items
 *  without replacement: probability that each element is chosen; fraction must be [0, 1]
 *  with replacement: expected number of times each element is chosen; fraction must be >= 0
 */
@Experimental
final class StratifiedSampling private(
    override val uid: String,
    val withReplacement: Boolean,
    val fraction: Map[String, Double])
  extends Transformer with HasLabelCol with HasSeed with DefaultParamsWritable {

  @Since("2.0.0")
  def this(withReplacement: Boolean, fraction: Map[String, Double]) =
    this(Identifiable.randomUID("stratifiedSampling"), withReplacement, fraction)

  /** @group setParam */
  @Since("2.0.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  @Since("2.0.0")
  def setLabel(value: String): this.type = set(labelCol, value)

  setDefault(seed -> Utils.random.nextLong)

  @Since("2.0.0")
  override def transform(data: DataFrame): DataFrame = {
    transformSchema(data.schema)
    val schema = data.schema
    val colId = schema.fieldIndex($(labelCol))
    val result = data.rdd.map(r => (r.get(colId), r))
      .sampleByKey(withReplacement, fraction.toMap, $(seed))
      .map(_._2)
    data.sqlContext.createDataFrame(result, schema)
  }

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(labelCol), StringType)
    schema
  }

  @Since("2.0.0")
  override def write: MLWriter = new StratifiedSamplingWriter(this)

  @Since("2.0.0")
  override def copy(extra: ParamMap): StratifiedSampling = {
    val copied = new StratifiedSampling(uid, withReplacement, fraction)
    copyValues(copied, extra)
  }
}

@Since("2.0.0")
object StratifiedSampling extends DefaultParamsReadable[StratifiedSampling] {

  private case class Data(withReplacement: Boolean, fraction: Map[String, Double])

  private[StratifiedSampling]
  class StratifiedSamplingWriter(instance: StratifiedSampling) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = new Data(instance.withReplacement, instance.fraction)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class StratifiedSamplingReader extends MLReader[StratifiedSampling] {

    private val className = classOf[StratifiedSampling].getName

    override def load(path: String): StratifiedSampling = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val Row(withReplacement: Boolean, fraction: Map[String, Double]) = sqlContext.read
        .parquet(dataPath)
        .select("withReplacement", "fraction")
        .head()
      val model = new StratifiedSampling(metadata.uid, withReplacement, fraction)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("2.0.0")
  override def read: MLReader[StratifiedSampling] = new StratifiedSamplingReader

  @Since("2.0.0")
  override def load(path: String): StratifiedSampling = super.load(path)
}
