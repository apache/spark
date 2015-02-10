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
import org.apache.spark.ml.param.{ParamMap,IntParam,BooleanParam}
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
 * A regex based tokenizer that extracts tokens using a regex.
 * Optional additional parameters include enabling lowercase stabdarization, a minimum character
 * size for tokens as well as an array of stop words to remove from the results.
 */
@AlphaComponent
class RegexTokenizer extends UnaryTransformer[String, Seq[String], Tokenizer] {

  val lowercase = new BooleanParam(this, "numFeatures", "number of features", Some(true))
  def setLowercase(value: Boolean) = set(lowercase, value)
  def getLowercase: Boolean = get(lowercase)

  val minLength = new IntParam(this, "numFeatures", "number of features", Some(0))
  def setMinLength(value: Int) = set(minLength, value)
  def getMinLength: Int = get(minLength)

  val regEx = "\\p{L}+|[^\\p{L}\\s]+".r
  // def setRegex(value: scala.util.matching.Regex) = set(regEx, value)
  // def getRegex: scala.util.matching.Regex = get(regEx)

  val stopWords = Array[String]()
  // def setStopWords(value: Array[String]) = set(stopWords, value)
  // def getStopWords: Array[String] = get(stopWords)


  override protected def createTransformFunc(paramMap: ParamMap): String => Seq[String] = { x =>

    var string = x
    if (paramMap(lowercase)) {
      string = string.toLowerCase
    }
    var tokens = (regEx findAllIn string).toList
    
    if(paramMap(minLength) > 0){
      tokens = tokens.filter(_.length > paramMap(minLength))
    }
    if(stopWords.length > 0){
      tokens = tokens.filter(!stopWords.contains(_))
    }
    tokens
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, false)
}
