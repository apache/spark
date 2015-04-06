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

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{ParamMap, IntParam, BooleanParam, Param}
import org.apache.spark.sql.types.{DataType, StringType, ArrayType}

/**
 * :: AlphaComponent ::
 * A tokenizer that converts the input string to lowercase and then splits it by white spaces.
 */
@AlphaComponent
class Tokenizer extends UnaryTransformer[String, Seq[String], Tokenizer] {

  override protected def createTransformFunc(paramMap: ParamMap): String => Seq[String] = {
    _.toLowerCase.split("\\s")
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, false)
}

/**
 * :: AlphaComponent ::
 * A regex based tokenizer that extracts tokens either by repeatedly matching the regex(default) 
 * or using it to split the text (set matching to false). Optional parameters also allow to fold
 * the text to lowercase prior to it being tokenized and to filer tokens using a minimal length. 
 * It returns an array of strings that can be empty.
 * The default parameters are regex = "\\p{L}+|[^\\p{L}\\s]+", matching = true, 
 * lowercase = false, minTokenLength = 1
 */
@AlphaComponent
class RegexTokenizer extends UnaryTransformer[String, Seq[String], RegexTokenizer] {

  /**
   * param for minimum token length, default is one to avoid returning empty strings
   * @group param
   */
  val minTokenLength: IntParam = new IntParam(this, "minLength", "minimum token length", Some(1))

  /** @group setParam */
  def setMinTokenLength(value: Int): this.type = set(minTokenLength, value)

  /** @group getParam */
  def getMinTokenLength: Int = get(minTokenLength)

  /**
   * param sets regex as splitting on gaps (true) or matching tokens (false)
   * @group param
   */
  val gaps: BooleanParam = new BooleanParam(
    this, "gaps", "Set regex to match gaps or tokens", Some(false))

  /** @group setParam */
  def setGaps(value: Boolean): this.type = set(gaps, value)

  /** @group getParam */
  def getGaps: Boolean = get(gaps)

  /**
   * param sets regex pattern used by tokenizer 
   * @group param
   */
  val pattern: Param[String] = new Param(
    this, "pattern", "regex pattern used for tokenizing", Some("\\p{L}+|[^\\p{L}\\s]+"))

  /** @group setParam */
  def setPattern(value: String): this.type = set(pattern, value)

  /** @group getParam */
  def getPattern: String = get(pattern)

  override protected def createTransformFunc(paramMap: ParamMap): String => Seq[String] = { str =>
    val re = paramMap(pattern).r
    val tokens = if (paramMap(gaps)) re.split(str).toSeq else re.findAllIn(str).toSeq
    val minLength = paramMap(minTokenLength)
    tokens.filter(_.length >= minLength)
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, false)
}
