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

import org.scalatest.FunSuite

import org.apache.spark.SparkException
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

case class TextData(rawText: String, wantedTokens: Seq[String])

class TokenizerSuite extends FunSuite with MLlibTestSparkContext {
  
  @transient var sqlContext: SQLContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(sc)
  }

  test("RegexTokenizer") {
    val myRegExTokenizer = new RegexTokenizer()
      .setInputCol("rawText")
      .setOutputCol("tokens")

    var dataset = sqlContext.createDataFrame(
      sc.parallelize(Seq(
        TextData("Test for tokenization.", Seq("Test", "for", "tokenization", ".")),
        TextData("Te,st. punct", Seq("Te", ",", "st", ".", "punct"))
    )))
    testRegexTokenizer(myRegExTokenizer, dataset)

    dataset = sqlContext.createDataFrame(
      sc.parallelize(Seq(
        TextData("Test for tokenization.", Seq("Test", "for", "tokenization")),
        TextData("Te,st. punct", Seq("punct"))
    )))
    myRegExTokenizer.asInstanceOf[RegexTokenizer]
      .setMinTokenLength(3)
    testRegexTokenizer(myRegExTokenizer, dataset)

    myRegExTokenizer.asInstanceOf[RegexTokenizer]
      .setPattern("\\s")
      .setGaps(true)
      .setMinTokenLength(0)
    dataset = sqlContext.createDataFrame(
      sc.parallelize(Seq(
        TextData("Test for tokenization.", Seq("Test", "for", "tokenization.")),
        TextData("Te,st.  punct", Seq("Te,st.", "", "punct"))
    )))
    testRegexTokenizer(myRegExTokenizer, dataset)
  }

  test("Tokenizer") {
    val oldTokenizer =  new Tokenizer()
      .setInputCol("rawText")
      .setOutputCol("tokens")
    var dataset = sqlContext.createDataFrame(
      sc.parallelize(Seq(
        TextData("Test for tokenization.", Seq("test", "for", "tokenization.")),
        TextData("Te,st.  punct", Seq("te,st.", "", "punct"))
    )))
    testTokenizer(oldTokenizer, dataset)
  }

  def testTokenizer(t: Tokenizer, dataset: DataFrame): Unit = {
    t.transform(dataset)
      .select("tokens", "wantedTokens")
      .collect().foreach { 
        case Row(tokens: Seq[Any], wantedTokens: Seq[Any]) =>
          assert(tokens === wantedTokens)
      }
  }

  def testRegexTokenizer(t: RegexTokenizer, dataset: DataFrame): Unit = {
    t.transform(dataset)
      .select("tokens", "wantedTokens")
      .collect().foreach { 
        case Row(tokens: Seq[Any], wantedTokens: Seq[Any]) =>
          assert(tokens === wantedTokens)
      }
  }

}
