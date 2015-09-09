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

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

/**
 * :: Experimental ::
 * A tokenizer that converts the input string to lowercase and then splits it by white spaces.
 *
 * @see [[RegexTokenizer]]
 */
@Experimental
class Tokenizer(override val uid: String) extends UnaryTransformer[String, Seq[String], Tokenizer] {

  def this() = this(Identifiable.randomUID("tok"))

  override protected def createTransformFunc: String => Seq[String] = {
    _.toLowerCase.split("\\s")
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, true)

  override def copy(extra: ParamMap): Tokenizer = defaultCopy(extra)
}

/**
 * :: Experimental ::
 * A regex based tokenizer that extracts tokens either by using the provided regex pattern to split
 * the text (default) or repeatedly matching the regex (if `gaps` is false).
 * Optional parameters also allow filtering tokens using a minimal length.
 * It returns an array of strings that can be empty.
 */
@Experimental
class RegexTokenizer(override val uid: String)
  extends UnaryTransformer[String, Seq[String], RegexTokenizer] {

  def this() = this(Identifiable.randomUID("regexTok"))

  /**
   * Minimum token length, >= 0.
   * Default: 1, to avoid returning empty strings
   * @group param
   */
  val minTokenLength: IntParam = new IntParam(this, "minTokenLength", "minimum token length (>= 0)",
    ParamValidators.gtEq(0))

  /** @group setParam */
  def setMinTokenLength(value: Int): this.type = set(minTokenLength, value)

  /** @group getParam */
  def getMinTokenLength: Int = $(minTokenLength)

  /**
   * Indicates whether regex splits on gaps (true) or matches tokens (false).
   * Default: true
   * @group param
   */
  val gaps: BooleanParam = new BooleanParam(this, "gaps", "Set regex to match gaps or tokens")

  /** @group setParam */
  def setGaps(value: Boolean): this.type = set(gaps, value)

  /** @group getParam */
  def getGaps: Boolean = $(gaps)

  /**
   * Regex pattern used to match delimiters if [[gaps]] is true or tokens if [[gaps]] is false.
   * Default: `"\\s+"`
   * @group param
   */
  val pattern: Param[String] = new Param(this, "pattern", "regex pattern used for tokenizing")

  /** @group setParam */
  def setPattern(value: String): this.type = set(pattern, value)

  /** @group getParam */
  def getPattern: String = $(pattern)

  setDefault(minTokenLength -> 1, gaps -> true, pattern -> "\\s+")

  override protected def createTransformFunc: String => Seq[String] = { str =>
    val re = $(pattern).r
    val tokens = if ($(gaps)) re.split(str).toSeq else re.findAllIn(str).toSeq
    val minLength = $(minTokenLength)
    tokens.filter(_.length >= minLength)
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, true)

  override def copy(extra: ParamMap): RegexTokenizer = defaultCopy(extra)
}
