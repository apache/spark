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

import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.sql.{DataFrame, Row}

case class TokenizerTestData(rawText: String, wantedTokens: Array[String]) {
  def getRawText: String = rawText
  def getWantedTokens: Array[String] = wantedTokens
}

class TokenizerSuite extends MLTest with DefaultReadWriteTest {

  test("params") {
    ParamsSuite.checkParams(new Tokenizer)
  }

  test("read/write") {
    val t = new Tokenizer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
    testDefaultReadWrite(t)
  }
}

class RegexTokenizerSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  def testRegexTokenizer(t: RegexTokenizer, dataframe: DataFrame): Unit = {
    testTransformer[(String, Seq[String])](dataframe, t, "tokens", "wantedTokens") {
      case Row(tokens, wantedTokens) =>
        assert(tokens === wantedTokens)
    }
  }

  test("params") {
    ParamsSuite.checkParams(new RegexTokenizer)
  }

  test("RegexTokenizer") {
    val tokenizer0 = new RegexTokenizer()
      .setGaps(false)
      .setPattern("\\w+|\\p{Punct}")
      .setInputCol("rawText")
      .setOutputCol("tokens")
    val dataset0 = Seq(
      TokenizerTestData("Test for tokenization.", Array("test", "for", "tokenization", ".")),
      TokenizerTestData("Te,st. punct", Array("te", ",", "st", ".", "punct"))
    ).toDF()
    testRegexTokenizer(tokenizer0, dataset0)

    val dataset1 = Seq(
      TokenizerTestData("Test for tokenization.", Array("test", "for", "tokenization")),
      TokenizerTestData("Te,st. punct", Array("punct"))
    ).toDF()
    tokenizer0.setMinTokenLength(3)
    testRegexTokenizer(tokenizer0, dataset1)

    val tokenizer2 = new RegexTokenizer()
      .setInputCol("rawText")
      .setOutputCol("tokens")
    val dataset2 = Seq(
      TokenizerTestData("Test for tokenization.", Array("test", "for", "tokenization.")),
      TokenizerTestData("Te,st.  punct", Array("te,st.", "punct"))
    ).toDF()
    testRegexTokenizer(tokenizer2, dataset2)
  }

  test("RegexTokenizer with toLowercase false") {
    val tokenizer = new RegexTokenizer()
      .setInputCol("rawText")
      .setOutputCol("tokens")
      .setToLowercase(false)
    val dataset = Seq(
      TokenizerTestData("JAVA SCALA", Array("JAVA", "SCALA")),
      TokenizerTestData("java scala", Array("java", "scala"))
    ).toDF()
    testRegexTokenizer(tokenizer, dataset)
  }

  test("read/write") {
    val t = new RegexTokenizer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setMinTokenLength(2)
      .setGaps(false)
      .setPattern("hi")
      .setToLowercase(false)
    testDefaultReadWrite(t)
  }
}

