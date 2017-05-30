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
import org.apache.spark.ml.param.{ParamMap, ParamValidators, IntParam}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.{Vectors, VectorUDT, Vector}
import org.apache.spark.sql.types.{StringType, ArrayType, DataType}

/**
 * :: Experimental ::
 * Converts a text document to a sparse vector of token counts.
 * @param vocabulary An Array over terms. Only the terms in the vocabulary will be counted.
 */
@Experimental
class CountVectorizerModel (override val uid: String, val vocabulary: Array[String])
  extends UnaryTransformer[Seq[String], Vector, CountVectorizerModel] {

  def this(vocabulary: Array[String]) =
    this(Identifiable.randomUID("cntVec"), vocabulary)

  /**
   * Corpus-specific filter to ignore scarce words in a document. For each document, terms with
   * frequency (count) less than the given threshold are ignored.
   * Default: 1
   * @group param
   */
  val minTermFreq: IntParam = new IntParam(this, "minTermFreq",
    "minimum frequency (count) filter used to neglect scarce words (>= 1). For each document, " +
      "terms with frequency less than the given threshold are ignored.", ParamValidators.gtEq(1))

  /** @group setParam */
  def setMinTermFreq(value: Int): this.type = set(minTermFreq, value)

  /** @group getParam */
  def getMinTermFreq: Int = $(minTermFreq)

  setDefault(minTermFreq -> 1)

  override protected def createTransformFunc: Seq[String] => Vector = {
    val dict = vocabulary.zipWithIndex.toMap
    document =>
      val termCounts = mutable.HashMap.empty[Int, Double]
      document.foreach { term =>
        dict.get(term) match {
          case Some(index) => termCounts.put(index, termCounts.getOrElse(index, 0.0) + 1.0)
          case None => // ignore terms not in the vocabulary
        }
      }
      Vectors.sparse(dict.size, termCounts.filter(_._2 >= $(minTermFreq)).toSeq)
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType.sameType(ArrayType(StringType)),
      s"Input type must be ArrayType(StringType) but got $inputType.")
  }

  override protected def outputDataType: DataType = new VectorUDT()

  override def copy(extra: ParamMap): CountVectorizerModel = {
    val copied = new CountVectorizerModel(uid, vocabulary)
    copyValues(copied, extra)
  }
}
