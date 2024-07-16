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
import org.apache.spark.util.ArrayImplicits._

/**
 * A tokenizer that converts the input string to lowercase and then splits it by white spaces.
 *
 * @see [[RegexTokenizer]]
 */
@Since("1.2.0")
class Tokenizer @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends UnaryTransformer[String, Seq[String], Tokenizer] with DefaultParamsWritable {

  @Since("1.2.0")
  def this() = this(Identifiable.randomUID("tok"))

  override protected def createTransformFunc: String => Seq[String] = {
    // scalastyle:off caselocale
    _.toLowerCase.split("\\s").toImmutableArraySeq
    // scalastyle:on caselocale
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType,
      s"Input type must be ${StringType.catalogString} type but got ${inputType.catalogString}.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, true)

  @Since("1.4.1")
  override def copy(extra: ParamMap): Tokenizer = defaultCopy(extra)
}

@Since("1.6.0")
object Tokenizer extends DefaultParamsReadable[Tokenizer] {

  @Since("1.6.0")
  override def load(path: String): Tokenizer = super.load(path)
}

/**
 * A regex based tokenizer that extracts tokens either by using the provided regex pattern to split
 * the text (default) or repeatedly matching the regex (if `gaps` is false).
 * Optional parameters also allow filtering tokens using a minimal length.
 * It returns an array of strings that can be empty.
 */
@Since("1.4.0")
class RegexTokenizer @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends UnaryTransformer[String, Seq[String], RegexTokenizer] with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("regexTok"))

  /**
   * Minimum token length, greater than or equal to 0.
   * Default: 1, to avoid returning empty strings
   * @group param
   */
  @Since("1.4.0")
  val minTokenLength: IntParam = new IntParam(this, "minTokenLength", "minimum token length (>= 0)",
    ParamValidators.gtEq(0))

  /** @group setParam */
  @Since("1.4.0")
  def setMinTokenLength(value: Int): this.type = set(minTokenLength, value)

  /** @group getParam */
  @Since("1.4.0")
  def getMinTokenLength: Int = $(minTokenLength)

  /**
   * Indicates whether regex splits on gaps (true) or matches tokens (false).
   * Default: true
   * @group param
   */
  @Since("1.4.0")
  val gaps: BooleanParam = new BooleanParam(this, "gaps", "Set regex to match gaps or tokens")

  /** @group setParam */
  @Since("1.4.0")
  def setGaps(value: Boolean): this.type = set(gaps, value)

  /** @group getParam */
  @Since("1.4.0")
  def getGaps: Boolean = $(gaps)

  /**
   * Regex pattern used to match delimiters if [[gaps]] is true or tokens if [[gaps]] is false.
   * Default: `"\\s+"`
   * @group param
   */
  @Since("1.4.0")
  val pattern: Param[String] = new Param(this, "pattern", "regex pattern used for tokenizing")

  /** @group setParam */
  @Since("1.4.0")
  def setPattern(value: String): this.type = set(pattern, value)

  /** @group getParam */
  @Since("1.4.0")
  def getPattern: String = $(pattern)

  /**
   * Indicates whether to convert all characters to lowercase before tokenizing.
   * Default: true
   * @group param
   */
  @Since("1.6.0")
  final val toLowercase: BooleanParam = new BooleanParam(this, "toLowercase",
    "whether to convert all characters to lowercase before tokenizing.")

  /** @group setParam */
  @Since("1.6.0")
  def setToLowercase(value: Boolean): this.type = set(toLowercase, value)

  /** @group getParam */
  @Since("1.6.0")
  def getToLowercase: Boolean = $(toLowercase)

  setDefault(minTokenLength -> 1, gaps -> true, pattern -> "\\s+", toLowercase -> true)

  override protected def createTransformFunc: String => Seq[String] = {
    val re = $(pattern).r
    val localToLowercase = $(toLowercase)
    val localGaps = $(gaps)
    val localMinTokenLength = $(minTokenLength)

    (originStr: String) => {
      // scalastyle:off caselocale
      val str = if (localToLowercase) originStr.toLowerCase() else originStr
      // scalastyle:on caselocale
      val tokens = if (localGaps) re.split(str).toImmutableArraySeq else re.findAllIn(str).toSeq
      tokens.filter(_.length >= localMinTokenLength)
    }
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, true)

  @Since("1.4.1")
  override def copy(extra: ParamMap): RegexTokenizer = defaultCopy(extra)

  @Since("3.0.0")
  override def toString: String = {
    s"RegexTokenizer: uid=$uid, minTokenLength=${$(minTokenLength)}, gaps=${$(gaps)}, " +
      s"pattern=${$(pattern)}, toLowercase=${$(toLowercase)}"
  }
}

@Since("1.6.0")
object RegexTokenizer extends DefaultParamsReadable[RegexTokenizer] {

  @Since("1.6.0")
  override def load(path: String): RegexTokenizer = super.load(path)
}
