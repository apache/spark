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

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, DataFrameStatFunctions}
import org.apache.spark.sql.types.{StringType, StructType}

/**
 * :: Experimental ::
 *
 * Stratified sampling on the DataFrame according to the keys in a specific label column. User
 * can set 'fraction' to assign different sampling rate for each key.
 *
 * @see [[DataFrameStatFunctions#sampleBy(java.lang.String, java.util.Map, long)]]
 *
 * @param withReplacement can elements be sampled multiple times (replaced when sampled out)
 * @param fraction expected size of the sample as a fraction of the items
 *  without replacement: probability that each element is chosen; fraction must be [0, 1]
 *  with replacement: expected number of times each element is chosen; fraction must be >= 0
 */
@Experimental
final class StratifiedSampler private (
    override val uid: String,
    val withReplacement: Boolean,
    val fraction: Map[String, Double])
  extends Transformer with HasLabelCol with HasSeed with DefaultParamsWritable {

  import StratifiedSampler._

  @Since("2.0.0")
  def this(withReplacement: Boolean, fraction: Map[String, Double]) =
    this(Identifiable.randomUID("stratifiedSampling"), withReplacement, fraction)

  @Since("2.0.0")
  def this(withReplacement: Boolean, fraction: java.util.Map[String, Double]) =
    this(Identifiable.randomUID("stratifiedSampling"), withReplacement, fraction.asScala.toMap)

  /** @group setParam */
  @Since("2.0.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  @Since("2.0.0")
  def setLabel(value: String): this.type = set(labelCol, value)

  @Since("2.0.0")
  override def transform(data: DataFrame): DataFrame = {
    transformSchema(data.schema, logging = true)
    val schema = data.schema
    if(withReplacement){
      val colId = schema.fieldIndex($(labelCol))
      val result = data.rdd.map(r => (r.get(colId), r))
        .sampleByKey(withReplacement, fraction.toMap, $(seed))
        .map(_._2)
      data.sqlContext.createDataFrame(result, schema)
    }
    else {
      data.stat.sampleBy($(labelCol), fraction, $(seed))
    }
  }

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(labelCol), StringType)
    schema
  }

  @Since("2.0.0")
  override def write: MLWriter = new StratifiedSamplingWriter(this)

  @Since("2.0.0")
  override def copy(extra: ParamMap): StratifiedSampler = {
    val copied = new StratifiedSampler(uid, withReplacement, fraction)
    copyValues(copied, extra)
  }
}

@Since("2.0.0")
object StratifiedSampler extends DefaultParamsReadable[StratifiedSampler] {

  private case class Data(withReplacement: Boolean, fraction: Map[String, Double])

  private[StratifiedSampler]
  class StratifiedSamplingWriter(instance: StratifiedSampler) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = new Data(instance.withReplacement, instance.fraction)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class StratifiedSamplingReader extends MLReader[StratifiedSampler] {

    private val className = classOf[StratifiedSampler].getName

    override def load(path: String): StratifiedSampler = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read
        .parquet(dataPath)
        .select("withReplacement", "fraction")
        .head()
      val withReplacement = data.getBoolean(0)
      val fraction = data.getAs[Map[String, Double]](1)
      val model = new StratifiedSampler(metadata.uid, withReplacement, fraction)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("2.0.0")
  override def read: MLReader[StratifiedSampler] = new StratifiedSamplingReader

  @Since("2.0.0")
  override def load(path: String): StratifiedSampler = super.load(path)
}
