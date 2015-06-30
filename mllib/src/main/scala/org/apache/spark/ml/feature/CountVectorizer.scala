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

import scala.collection.mutable

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * :: Experimental ::
 * Converts a text document to a sparse vector of token counts.
 * @param vocabulary An Array over terms. Only the terms in the vocabulary will be counted.
 */
@Experimental
class CountVectorizer (override val uid: String, vocabulary: Array[String]) extends HashingTF{

  def this(vocabulary: Array[String]) = this(Identifiable.randomUID("countVectorizer"), vocabulary)

  /**
   * Corpus-specific stop words filter. Terms with count less than the given threshold are ignored.
   * Default: 1
   * @group param
   */
  val minTermCounts: IntParam = new IntParam(this, "minTermCounts",
    "lower bound of effective term counts (>= 1)", ParamValidators.gtEq(1))

  /** @group setParam */
  def setMinTermCounts(value: Int): this.type = set(minTermCounts, value)

  /** @group getParam */
  def getMinTermCounts: Int = $(minTermCounts)

  setDefault(minTermCounts -> 1, numFeatures -> vocabulary.size)

  override def transform(dataset: DataFrame): DataFrame = {
    val dict = vocabulary.zipWithIndex.toMap
    val t = udf { terms: Seq[String] =>
      val termCounts = mutable.HashMap.empty[Int, Double]
      terms.foreach { term =>
        val index = dict.getOrElse(term, -1)
        if (index >= 0) {
          termCounts.put(index, termCounts.getOrElse(index, 0.0) + 1.0)
        }
      }
      Vectors.sparse(dict.size, termCounts.filter(_._2 >= $(minTermCounts)).toSeq)
    }
    dataset.withColumn($(outputCol), t(col($(inputCol))))
  }

  override def copy(extra: ParamMap): CountVectorizer = {
    val copied = new CountVectorizer(uid, vocabulary)
    copyValues(copied, extra)
  }
}
