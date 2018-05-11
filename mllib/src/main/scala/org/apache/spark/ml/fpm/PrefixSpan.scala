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

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.mllib.fpm.{PrefixSpan => mllibPrefixSpan}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, LongType, StructField, StructType}

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
object PrefixSpan {

  /**
   * :: Experimental ::
   * Finds the complete set of frequent sequential patterns in the input sequences of itemsets.
   *
   * @param dataset A dataset or a dataframe containing a sequence column which is
   *                {{{Seq[Seq[_]]}}} type
   * @param sequenceCol the name of the sequence column in dataset, rows with nulls in this column
   *                    are ignored
   * @param minSupport the minimal support level of the sequential pattern, any pattern that
   *                   appears more than (minSupport * size-of-the-dataset) times will be output
   *                  (recommended value: `0.1`).
   * @param maxPatternLength the maximal length of the sequential pattern
   *                         (recommended value: `10`).
   * @param maxLocalProjDBSize The maximum number of items (including delimiters used in the
   *                           internal storage format) allowed in a projected database before
   *                           local processing. If a projected database exceeds this size, another
   *                           iteration of distributed prefix growth is run
   *                           (recommended value: `32000000`).
   * @return A `DataFrame` that contains columns of sequence and corresponding frequency.
   *         The schema of it will be:
   *          - `sequence: Seq[Seq[T]]` (T is the item type)
   *          - `freq: Long`
   */
  @Since("2.4.0")
  def findFrequentSequentialPatterns(
      dataset: Dataset[_],
      sequenceCol: String,
      minSupport: Double,
      maxPatternLength: Int,
      maxLocalProjDBSize: Long): DataFrame = {

    val inputType = dataset.schema(sequenceCol).dataType
    require(inputType.isInstanceOf[ArrayType] &&
      inputType.asInstanceOf[ArrayType].elementType.isInstanceOf[ArrayType],
      s"The input column must be ArrayType and the array element type must also be ArrayType, " +
      s"but got $inputType.")


    val data = dataset.select(sequenceCol)
    val sequences = data.where(col(sequenceCol).isNotNull).rdd
      .map(r => r.getAs[Seq[Seq[Any]]](0).map(_.toArray).toArray)

    val mllibPrefixSpan = new mllibPrefixSpan()
      .setMinSupport(minSupport)
      .setMaxPatternLength(maxPatternLength)
      .setMaxLocalProjDBSize(maxLocalProjDBSize)

    val rows = mllibPrefixSpan.run(sequences).freqSequences.map(f => Row(f.sequence, f.freq))
    val schema = StructType(Seq(
      StructField("sequence", dataset.schema(sequenceCol).dataType, nullable = false),
      StructField("freq", LongType, nullable = false)))
    val freqSequences = dataset.sparkSession.createDataFrame(rows, schema)

    freqSequences
  }

}
