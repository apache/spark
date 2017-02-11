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

import org.apache.spark.annotation.Since
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

/**
 * A feature transformer that converts the input array of strings into an array of n-grams. Null
 * values in the input array are ignored.
 * It returns an array of n-grams where each n-gram is represented by a space-separated string of
 * words.
 *
 * When the input is empty, an empty array is returned.
 * When the input array length is less than n (number of elements per n-gram), no n-grams are
 * returned.
 */
@Since("1.5.0")
class NGram @Since("1.5.0") (@Since("1.5.0") override val uid: String)
  extends UnaryTransformer[Seq[String], Seq[String], NGram] with DefaultParamsWritable {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("ngram"))

  /**
   * Minimum n-gram length, greater than or equal to 1.
   * Default: 2, bigram features
   * @group param
   */
  @Since("1.5.0")
  val n: IntParam = new IntParam(this, "n", "number elements per n-gram (>=1)",
    ParamValidators.gtEq(1))

  /** @group setParam */
  @Since("1.5.0")
  def setN(value: Int): this.type = set(n, value)

  /** @group getParam */
  @Since("1.5.0")
  def getN: Int = $(n)

  setDefault(n -> 2)

  override protected def createTransformFunc: Seq[String] => Seq[String] = {
    _.iterator.sliding($(n)).withPartial(false).map(_.mkString(" ")).toSeq
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType.sameType(ArrayType(StringType)),
      s"Input type must be ArrayType(StringType) but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, false)
}

@Since("1.6.0")
object NGram extends DefaultParamsReadable[NGram] {

  @Since("1.6.0")
  override def load(path: String): NGram = super.load(path)
}
