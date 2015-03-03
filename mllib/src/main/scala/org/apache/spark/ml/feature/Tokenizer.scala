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

  val lowerCase = new BooleanParam(this, "lowerCase", "Folds case to lower case", Some(false))
  def setLowercase(value: Boolean) = set(lowerCase, value)
  def getLowercase: Boolean = get(lowerCase)

  val minTokenLength = new IntParam(this, "minLength", "minimum token length", Some(1))
  def setMinTokenLength(value: Int) = set(minTokenLength, value)
  def getMinTokenLength: Int = get(minTokenLength)

  val matching = new BooleanParam(this, "matching", "Sets regex to matching or split", Some(true))
  def setMatching(value: Boolean) = set(matching, value)
  def getMatching: Boolean = get(matching)

  val regex = new Param(this, "regex", "regex used for tokenizing", Some("\\p{L}+|[^\\p{L}\\s]+"))
  def setRegex(value: String) = set(regex, value)
  def getRegex: String = get(regex)

  override protected def createTransformFunc(paramMap: ParamMap): String => Seq[String] = { x =>

    val str = if (paramMap(lowerCase)) x.toLowerCase else x

    val re = paramMap(regex)
    val tokens = if(paramMap(matching))(re.r.findAllIn(str)).toList else str.split(re).toList

    tokens.filter(_.length >= paramMap(minTokenLength))
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, false)
}
