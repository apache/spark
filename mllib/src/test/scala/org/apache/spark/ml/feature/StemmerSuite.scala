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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}

class StemmerSuite extends SparkFunSuite with MLlibTestSparkContext  with DefaultReadWriteTest {

  test("params") {
    ParamsSuite.checkParams(new Tokenizer)
  }

  test("read/write") {
    val t = new Stemmer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
    testDefaultReadWrite(t)
  }

  test("stem plurals") {
    val plurals = Map(
      "caresses" -> "caress",
      "ponies" -> "poni",
      "ties" -> "ti",
      "caress" -> "caress",
      "cats" -> "cat")
    val dataset = sqlContext.createDataFrame(
      plurals.map(kv => Array(kv._1) -> Array(kv._2)).toSeq).toDF("tokens", "expected")
    StemmerSuite.testStemmer(dataset)
  }

  test("stem past participles") {
    val participles = Map(
      "plastered" -> "plaster",
      "bled" -> "bled",
      "motoring" -> "motor",
      "sing" -> "sing",

      "conflated" -> "conflat",
      "troubled" -> "troubl",
      "sized" -> "size",
      "hopping" -> "hop",
      "tanned" -> "tan",
      "falling" -> "fall",
      "hissing" -> "hiss",
      "fizzed" -> "fizz",
      "failing" -> "fail",
      "filing" -> "file",

      "happy" -> "happi",
      "sky" -> "sky"
    )
    val dataset = sqlContext.createDataFrame(
      participles.map(kv => Array(kv._1) -> Array(kv._2)).toSeq).toDF("tokens", "expected")
    StemmerSuite.testStemmer(dataset)
  }

  test("change suffixes") {
    val changes = Map(
      "relational" -> "relat",
      "conditional" -> "condit",
      "rational" -> "ration",
      "valenci" -> "valenc",
      "hesitanci" -> "hesit",
      "digitizer" -> "digit",
      "conformabli" -> "conform",
      "radicalli" -> "radic",
      "differentli" -> "differ",
      "vileli" -> "vile",
      "analogousli" -> "analog",
      "vietnamization" -> "vietnam",
      "predication" -> "predic",
      "operator" -> "oper",
      "feudalism" -> "feudal",
      "decisiveness" -> "decis",
      "hopefulness" -> "hope",
      "callousness" -> "callous",
      "formaliti" -> "formal",
      "sensitiviti" -> "sensit",
      "sensibiliti" -> "sensibl",

      "triplicate" -> "triplic",
      "formative" -> "form",
      "formalize" -> "formal",
      "electriciti" -> "electr",
      "electrical" -> "electr",
      "hopeful" -> "hope",
      "goodness" -> "good",

      "revival" -> "reviv",
      "allowance" -> "allow",
      "inference" -> "infer",
      "airliner" -> "airlin",
      "gyroscopic" -> "gyroscop",
      "adjustable" -> "adjust",
      "defensible" -> "defens",
      "irritant" -> "irrit",
      "replacement" -> "replac",
      "adjustment" -> "adjust",
      "dependent" -> "depend",
      "adoption" -> "adopt",
      "homologou" -> "homolog",
      "communism" -> "commun",
      "activate" -> "activ",
      "angulariti" -> "angular",
      "homologous" -> "homolog",
      "effective" -> "effect",
      "bowdlerize" -> "bowdler",

      "probate" -> "probat",
      "rate" -> "rate",
      "cease" -> "ceas",
      "controll" -> "control",
      "roll" -> "roll"
    )
    val dataset = sqlContext.createDataFrame(
      changes.map(kv => Array(kv._1) -> Array(kv._2)).toSeq).toDF("tokens", "expected")
    StemmerSuite.testStemmer(dataset)
  }
}

private object StemmerSuite extends SparkFunSuite {

  def testStemmer(dataset: DataFrame): Unit = {

    val stemmer = new Stemmer()
      .setInputCol("tokens")
      .setOutputCol("stemmed")

    stemmer.transform(dataset).select("expected", "stemmed").collect()
      .foreach { case Row(tokens, wantedTokens) => assert(tokens === wantedTokens) }
  }
}
