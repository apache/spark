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

import scala.beans.BeanInfo

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}

@BeanInfo
case class TokenizerTestData(rawText: String, wantedTokens: Array[String])

class TokenizerSuite extends SparkFunSuite {

  test("params") {
    ParamsSuite.checkParams(new Tokenizer)
  }
}

class RegexTokenizerSuite extends SparkFunSuite with MLlibTestSparkContext {
  import org.apache.spark.ml.feature.RegexTokenizerSuite._

  test("params") {
    ParamsSuite.checkParams(new RegexTokenizer)
  }

  test("RegexTokenizer") {
    val tokenizer0 = new RegexTokenizer()
      .setGaps(false)
      .setPattern("\\w+|\\p{Punct}")
      .setInputCol("rawText")
      .setOutputCol("tokens")
    val dataset0 = sqlContext.createDataFrame(Seq(
      TokenizerTestData("Test for tokenization.", Array("Test", "for", "tokenization", ".")),
      TokenizerTestData("Te,st. punct", Array("Te", ",", "st", ".", "punct"))
    ))
    testRegexTokenizer(tokenizer0, dataset0)

    val dataset1 = sqlContext.createDataFrame(Seq(
      TokenizerTestData("Test for tokenization.", Array("Test", "for", "tokenization")),
      TokenizerTestData("Te,st. punct", Array("punct"))
    ))
    tokenizer0.setMinTokenLength(3)
    testRegexTokenizer(tokenizer0, dataset1)

    val tokenizer2 = new RegexTokenizer()
      .setInputCol("rawText")
      .setOutputCol("tokens")
    val dataset2 = sqlContext.createDataFrame(Seq(
      TokenizerTestData("Test for tokenization.", Array("Test", "for", "tokenization.")),
      TokenizerTestData("Te,st.  punct", Array("Te,st.", "punct"))
    ))
    testRegexTokenizer(tokenizer2, dataset2)
  }
}

object RegexTokenizerSuite extends SparkFunSuite {

  def testRegexTokenizer(t: RegexTokenizer, dataset: DataFrame): Unit = {
    t.transform(dataset)
      .select("tokens", "wantedTokens")
      .collect()
      .foreach { case Row(tokens, wantedTokens) =>
        assert(tokens === wantedTokens)
      }
  }
}
