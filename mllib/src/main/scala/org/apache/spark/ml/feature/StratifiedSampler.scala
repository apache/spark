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
import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{BooleanParam, ParamMap}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, DataFrameStatFunctions, Dataset}
import org.apache.spark.sql.types.StructType

/**
 * :: Experimental ::
 *
 * Stratified sampling on the DataFrame according to the keys in a specific label column. User
 * can set 'fraction' to set different sampling rate for each key.
 *
 * @param fractions sampling fraction for each stratum. @see [[DataFrameStatFunctions.sampleBy]].
 *                 Supported stratum types are Int, String and Boolean
 */
@Experimental
final class StratifiedSampler private (
    override val uid: String,
    val fractions: Map[_, Double])
  extends Transformer with HasLabelCol with HasSeed with DefaultParamsWritable {

  import StratifiedSampler._

  @Since("2.0.0")
  def this(fraction: Map[_, Double]) =
    this(Identifiable.randomUID("stratifiedSampling"), fraction)

  @Since("2.0.0")
  def this(fraction: java.util.Map[_, Double]) =
    this(fraction.asScala.toMap)

  /** @group setParam */
  @Since("2.0.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  @Since("2.0.0")
  def setLabel(value: String): this.type = set(labelCol, value)

  /**
   * If true, sampling will be skipped and all the records will be returned.
   * Used in prediction pipeline
   * Default: false
   * @group param
   */
  val skip: BooleanParam = new BooleanParam(this, "skip",
    "If true, sampling will be skipped and all the records will be returned. " +
    "Used in prediction pipeline")

  /** @group getParam */
  def getSkip: Boolean = $(skip)

  /** @group setParam */
  def setSkip(value: Boolean): this.type = set(skip, value)

  setDefault(skip -> false)

  @Since("2.0.0")
  override def transform(data: Dataset[_]): DataFrame = {
    transformSchema(data.schema, logging = true)
    if (!$(skip)) {
      data.stat.sampleBy($(labelCol), fractions, $(seed))
    } else {
      data.toDF()
    }
  }

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    require(fractions.nonEmpty, "fraction should not be empty")
    require(fractions.keySet.forall(_.isInstanceOf[String])
      || fractions.keySet.forall(_.isInstanceOf[Int])
      || fractions.keySet.forall(_.isInstanceOf[Boolean]),
      s"only support stratum of type String, Int and Boolean")
    require(fractions.values.forall(v => v >= 0 && v <= 1), "sampling rate should be in [0, 1]")
    schema
  }

  @Since("2.0.0")
  override def write: MLWriter = new StratifiedSamplingWriter(this)

  @Since("2.0.0")
  override def copy(extra: ParamMap): StratifiedSampler = {
    val copied = new StratifiedSampler(uid, fractions)
    copyValues(copied, extra)
  }
}

@Since("2.0.0")
object StratifiedSampler extends DefaultParamsReadable[StratifiedSampler] {

  private[StratifiedSampler]
  class StratifiedSamplingWriter(instance: StratifiedSampler) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString
      val map = instance.fractions
      val df = map.keys.head match {
        case s: String =>
          sqlContext.createDataFrame(map.asInstanceOf[Map[String, Double]].toSeq)
        case i: Int =>
          sqlContext.createDataFrame(map.asInstanceOf[Map[Int, Double]].toSeq)
        case b: Boolean =>
          sqlContext.createDataFrame(map.asInstanceOf[Map[Boolean, Double]].toSeq)
        case _ => throw new SparkException("wrong type")
      }
      df.toDF("key", "value").repartition(1).write.parquet(dataPath)
    }
  }

  private class StratifiedSamplingReader extends MLReader[StratifiedSampler] {
    private val className = classOf[StratifiedSampler].getName
    override def load(path: String): StratifiedSampler = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val fraction = sqlContext.read.parquet(dataPath).select("key", "value")
        .rdd.map(r => (r.get(0), r.getDouble(1))).collectAsMap().toMap
      val model = new StratifiedSampler(metadata.uid, fraction.asInstanceOf[Map[_, Double]])
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("2.0.0")
  override def read: MLReader[StratifiedSampler] = new StratifiedSamplingReader

  @Since("2.0.0")
  override def load(path: String): StratifiedSampler = super.load(path)
}


