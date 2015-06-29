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
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.{Vectors, VectorUDT, Vector}
import org.apache.spark.sql.types.{StringType, ArrayType, DataType}

/**
 * :: Experimental ::
 * Converts a text document to a sparse vector of token counts.
 * @param vocabulary An Array over terms. Only the terms in the vocabulary will be counted.
 */
@Experimental
class CountVectorizer (override val uid: String, vocabulary: Array[String])
  extends UnaryTransformer[Seq[String], Vector, CountVectorizer] {

  def this(vocabulary: Array[String]) = this(Identifiable.randomUID("countVectorizer"), vocabulary)

  /**
   * Corpus-specific stop words filter. Terms with count less than the given threshold are ignored.
   * Default: 1
   * @group param
   */
  val minTermCounts: IntParam = new IntParam(this, "minTermCounts",
    "lower bound of effective term counts (>= 0)", ParamValidators.gtEq(1))

  /** @group setParam */
  def setMinTermCounts(value: Int): this.type = set(minTermCounts, value)

  /** @group getParam */
  def getMinTermCounts: Int = $(minTermCounts)

  setDefault(minTermCounts -> 1)

  override protected def createTransformFunc: Seq[String] => Vector = {
    val dict = vocabulary.zipWithIndex.toMap
    document =>
      val termCounts = mutable.HashMap.empty[Int, Double]
      document.foreach { term =>
        val index = dict.getOrElse(term, -1)
        if (index >= 0) {
          termCounts.put(index, termCounts.getOrElse(index, 0.0) + 1.0)
        }
      }
      Vectors.sparse(dict.size, termCounts.filter(_._2 >= $(minTermCounts)).toSeq)
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType.sameType(ArrayType(StringType)),
      s"Input type must be Array type but got $inputType.")
  }

  override protected def outputDataType: DataType = new VectorUDT()

  override def copy(extra: ParamMap): CountVectorizer = {
    val copied = new CountVectorizer(uid, vocabulary)
    copyValues(copied, extra)
  }
}
