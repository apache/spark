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

import org.apache.spark.annotation.Since
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.fpm.{PrefixSpan => mllibPrefixSpan}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, LongType, StructField, StructType}

/**
 * A parallel PrefixSpan algorithm to mine frequent sequential patterns.
 * The PrefixSpan algorithm is described in J. Pei, et al., PrefixSpan: Mining Sequential Patterns
 * Efficiently by Prefix-Projected Pattern Growth
 * (see <a href="https://doi.org/10.1109/ICDE.2001.914830">here</a>).
 * This class is not yet an Estimator/Transformer, use `findFrequentSequentialPatterns` method to
 * run the PrefixSpan algorithm.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Sequential_Pattern_Mining">Sequential Pattern Mining
 * (Wikipedia)</a>
 */
@Since("2.4.0")
final class PrefixSpan(@Since("2.4.0") override val uid: String) extends Params {

  @Since("2.4.0")
  def this() = this(Identifiable.randomUID("prefixSpan"))

  /**
   * Param for the minimal support level (default: `0.1`).
   * Sequential patterns that appear more than (minSupport * size-of-the-dataset) times are
   * identified as frequent sequential patterns.
   * @group param
   */
  @Since("2.4.0")
  val minSupport = new DoubleParam(this, "minSupport", "The minimal support level of the " +
    "sequential pattern. Sequential pattern that appears more than " +
    "(minSupport * size-of-the-dataset) " +
    "times will be output.", ParamValidators.gtEq(0.0))

  /** @group getParam */
  @Since("2.4.0")
  def getMinSupport: Double = $(minSupport)

  /** @group setParam */
  @Since("2.4.0")
  def setMinSupport(value: Double): this.type = set(minSupport, value)

  /**
   * Param for the maximal pattern length (default: `10`).
   * @group param
   */
  @Since("2.4.0")
  val maxPatternLength = new IntParam(this, "maxPatternLength",
    "The maximal length of the sequential pattern.",
    ParamValidators.gt(0))

  /** @group getParam */
  @Since("2.4.0")
  def getMaxPatternLength: Int = $(maxPatternLength)

  /** @group setParam */
  @Since("2.4.0")
  def setMaxPatternLength(value: Int): this.type = set(maxPatternLength, value)

  /**
   * Param for the maximum number of items (including delimiters used in the internal storage
   * format) allowed in a projected database before local processing (default: `32000000`).
   * If a projected database exceeds this size, another iteration of distributed prefix growth
   * is run.
   * @group param
   */
  @Since("2.4.0")
  val maxLocalProjDBSize = new LongParam(this, "maxLocalProjDBSize",
    "The maximum number of items (including delimiters used in the internal storage format) " +
    "allowed in a projected database before local processing. If a projected database exceeds " +
    "this size, another iteration of distributed prefix growth is run.",
    ParamValidators.gt(0))

  /** @group getParam */
  @Since("2.4.0")
  def getMaxLocalProjDBSize: Long = $(maxLocalProjDBSize)

  /** @group setParam */
  @Since("2.4.0")
  def setMaxLocalProjDBSize(value: Long): this.type = set(maxLocalProjDBSize, value)

  /**
   * Param for the name of the sequence column in dataset (default "sequence"), rows with
   * nulls in this column are ignored.
   * @group param
   */
  @Since("2.4.0")
  val sequenceCol = new Param[String](this, "sequenceCol", "The name of the sequence column in " +
    "dataset, rows with nulls in this column are ignored.")

  /** @group getParam */
  @Since("2.4.0")
  def getSequenceCol: String = $(sequenceCol)

  /** @group setParam */
  @Since("2.4.0")
  def setSequenceCol(value: String): this.type = set(sequenceCol, value)

  setDefault(minSupport -> 0.1, maxPatternLength -> 10, maxLocalProjDBSize -> 32000000,
    sequenceCol -> "sequence")

  /**
   * Finds the complete set of frequent sequential patterns in the input sequences of itemsets.
   *
   * @param dataset A dataset or a dataframe containing a sequence column which is
   *                {{{ArrayType(ArrayType(T))}}} type, T is the item type for the input dataset.
   * @return A `DataFrame` that contains columns of sequence and corresponding frequency.
   *         The schema of it will be:
   *          - `sequence: ArrayType(ArrayType(T))` (T is the item type)
   *          - `freq: Long`
   */
  @Since("2.4.0")
  def findFrequentSequentialPatterns(dataset: Dataset[_]): DataFrame = instrumented { instr =>
    instr.logDataset(dataset)
    import org.apache.spark.util.ArrayImplicits._
    instr.logParams(this, params.toImmutableArraySeq: _*)

    val sequenceColParam = $(sequenceCol)
    val inputType = dataset.schema(sequenceColParam).dataType
    require(inputType.isInstanceOf[ArrayType] &&
      inputType.asInstanceOf[ArrayType].elementType.isInstanceOf[ArrayType],
      s"The input column must be ArrayType and the array element type must also be ArrayType, " +
      s"but got $inputType.")

    val data = dataset.select(sequenceColParam)
    val sequences = data.where(col(sequenceColParam).isNotNull).rdd
      .map(r => r.getSeq[scala.collection.Seq[Any]](0).map(_.toArray).toArray)

    val mllibPrefixSpan = new mllibPrefixSpan()
      .setMinSupport($(minSupport))
      .setMaxPatternLength($(maxPatternLength))
      .setMaxLocalProjDBSize($(maxLocalProjDBSize))

    val rows = mllibPrefixSpan.run(sequences).freqSequences.map(f => Row(f.sequence, f.freq))
    val schema = StructType(Array(
      StructField("sequence", dataset.schema(sequenceColParam).dataType, nullable = false),
      StructField("freq", LongType, nullable = false)))
    val freqSequences = dataset.sparkSession.createDataFrame(rows, schema)

    freqSequences
  }

  @Since("2.4.0")
  override def copy(extra: ParamMap): PrefixSpan = defaultCopy(extra)

}
