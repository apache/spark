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

package org.apache.spark.ml.fpm

import scala.reflect.ClassTag

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.fpm.{PrefixSpan => mllibPrefixSpan}
import org.apache.spark.mllib.fpm.PrefixSpan.FreqSequence
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/**
 * Common params for PrefixSpan and PrefixSpanModel
 */
private[fpm] trait PrefixSpanParams extends Params {

  /**
   * Sequence column name.
   * Default: "sequence"
   * @group param
   */
  @Since("2.4.0")
  val sequenceCol: Param[String] = new Param[String](this, "sequenceCol", "sequence column name")
  setDefault(sequenceCol -> "sequence")

  /** @group getParam */
  @Since("2.2.0")
  def getSequenceCol: String = $(sequenceCol)

  /**
   * Minimal support level of the sequential pattern. Any pattern that appears
   * more than (minSupport * size-of-the-dataset) times will be output.
   * Default: 0.1
   * @group param
   */
  @Since("2.4.0")
  val minSupport: DoubleParam = new DoubleParam(this, "minSupport",
    "the minimal support level of a sequential pattern",
    ParamValidators.inRange(0.0, 1.0))
  setDefault(minSupport -> 0.1)

  /** @group getParam */
  @Since("2.4.0")
  def getMinSupport: Double = $(minSupport)

  /**
   * The maximal length of the sequential pattern, any pattern that appears
   * less than maxPatternLength will be output
   * Default: 10
   * @group param
   */
  @Since("2.4.0")
  val maxPatternLength: IntParam = new IntParam(this, "maxPatternLength",
    "the maximal length of the sequential pattern",
    ParamValidators.inRange(1, Int.MaxValue))
  setDefault(maxPatternLength -> 10)

  /**
   * The maximum number of items (including delimiters used in the internal
   * storage format) allowed in a projected database before local
   * processing. If a projected database exceeds this size, another
   * iteration of distributed prefix growth is run.
   * Default: 32000000L
   * @group param
   */
  @Since("2.4.0")
  val maxLocalProjDBSize: LongParam = new LongParam(this, "maxLocalProjDBSize",
    "The maximum number of items (including delimiters used in the internal " +
    "storage format) allowed in a projected database before local processin")
  setDefault(maxLocalProjDBSize -> 32000000L)

  /**
   * Validates and transforms the input schema.
   * @param schema input schema
   * @return output schema
   */
  @Since("2.2.0")
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val inputType = schema($(sequenceCol)).dataType
    require(inputType.isInstanceOf[ArrayType] &&
      inputType.asInstanceOf[ArrayType].elementType.isInstanceOf[ArrayType],
      s"The input column must be ArrayType[ArrayType], but got $inputType.")
    schema
  }
}

/**
 * :: Experimental ::
 * A parallel PrefixSpan algorithm to mine frequent sequential patterns.
 * The PrefixSpan algorithm is described in J. Pei, et al., PrefixSpan: Mining Sequential Patterns
 * Efficiently by Prefix-Projected Pattern Growth
 * (see <a href="http://doi.org/10.1109/ICDE.2001.914830">here</a>).
 *
 * @see <a href="https://en.wikipedia.org/wiki/Sequential_Pattern_Mining">Sequential Pattern Mining
 * (Wikipedia)</a>
 */
@Since("2.4.0")
@Experimental
class PrefixSpan @Since("2.4.0") (@Since("2.4.0") override val uid: String)
  extends Estimator[PrefixSpanModel] with PrefixSpanParams with DefaultParamsWritable {

  @Since("2.4.0")
  def this() = this(Identifiable.randomUID("prefixspan"))

  /** @group setParam */
  @Since("2.4.0")
  def setMinSupport(value: Double): this.type = set(minSupport, value)

  /** @group setParam */
  @Since("2.4.0")
  def setMaxPatternLength(value: Int): this.type = set(maxPatternLength, value)

  /** @group setParam */
  @Since("2.4.0")
  def setMaxLocalProjDBSize(value: Long): this.type = set(maxLocalProjDBSize, value)

  @Since("2.4.0")
  override def fit(dataset: Dataset[_]): PrefixSpanModel = {
    transformSchema(dataset.schema, logging = true)
    genericFit(dataset)
  }

  private def genericFit[T: ClassTag](dataset: Dataset[_]): PrefixSpanModel = {
    val handlePersistence = dataset.storageLevel == StorageLevel.NONE

    val data = dataset.select($(sequenceCol))
    val sequences = data.where(col($(sequenceCol)).isNotNull).rdd
      .map(r => r.getAs[Seq[Seq[Any]]](0).map(_.toArray).toArray)
    val mllibPrefixSpan = new mllibPrefixSpan()
      .setMinSupport($(minSupport))
      .setMaxPatternLength($(maxPatternLength))
      .setMaxLocalProjDBSize($(maxLocalProjDBSize))
    if (handlePersistence) {
      sequences.persist(StorageLevel.MEMORY_AND_DISK)
    }
    val rows = mllibPrefixSpan.run(sequences).freqSequences.map(f => Row(f.sequence, f.freq))
    val schema = StructType(Seq(
      StructField("sequence", dataset.schema($(sequenceCol)).dataType, nullable = false),
      StructField("freq", LongType, nullable = false)))
    val freqSequences = dataset.sparkSession.createDataFrame(rows, schema)

    if (handlePersistence) {
      sequences.unpersist()
    }

    copyValues(new PrefixSpanModel(uid, freqSequences)).setParent(this)
  }

  @Since("2.4.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("2.4.0")
  override def copy(extra: ParamMap): PrefixSpan = defaultCopy(extra)
}

@Since("2.4.0")
object PrefixSpan extends DefaultParamsReadable[PrefixSpan] {

  @Since("2.4.0")
  override def load(path: String): PrefixSpan = super.load(path)
}

/**
 * :: Experimental ::
 * Model fitted by PrefixSpan.
 */
@Since("2.4.0")
@Experimental
class PrefixSpanModel private[ml] (
    @Since("2.4.0") override val uid: String,
    @Since("2.4.0") @transient val freqSequences: DataFrame)
  extends Model[PrefixSpanModel] with PrefixSpanParams with MLWritable {

  @Since("2.4.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    dataset.toDF()
  }

  @Since("2.4.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("2.4.0")
  override def copy(extra: ParamMap): PrefixSpanModel = {
    val copied = new PrefixSpanModel(uid, freqSequences)
    copyValues(copied, extra).setParent(this.parent)
  }

  @Since("2.4.0")
  override def write: MLWriter = new PrefixSpanModel.PrefixSpanModelWriter(this)
}

@Since("2.4.0")
object PrefixSpanModel extends MLReadable[PrefixSpanModel] {

  @Since("2.4.0")
  override def read: MLReader[PrefixSpanModel] = new PrefixSpanModelReader

  @Since("2.4.0")
  override def load(path: String): PrefixSpanModel = super.load(path)

  /** [[MLWriter]] instance for [[PrefixSpanModel]] */
  private[PrefixSpanModel]
  class PrefixSpanModelWriter(instance: PrefixSpanModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString
      instance.freqSequences.write.parquet(dataPath)
    }
  }

  private class PrefixSpanModelReader extends MLReader[PrefixSpanModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[PrefixSpanModel].getName

    override def load(path: String): PrefixSpanModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val frequentItems = sparkSession.read.parquet(dataPath)
      val model = new PrefixSpanModel(metadata.uid, frequentItems)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}
