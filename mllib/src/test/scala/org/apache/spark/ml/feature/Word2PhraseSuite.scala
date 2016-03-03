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
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.Utils
import org.apache.spark.mllib.util.MLlibTestSparkContext

class Word2PhraseSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  /* test("Word2Phrase Read/Write") {

    val wordDataFrame = sqlContext.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic regression models are neat")
    )).toDF("label", "inputWords")

    var t = new Word2Phrase().setInputCol("inputWords").setOutputCol("out")
    
    val newInstance = testDefaultReadWrite(t)
    assert(newInstance === t)
  } */

  test("Word2PhraseModel Read/Write") {

    val wordDataFrame = sqlContext.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic regression models are neat")
    )).toDF("label", "inputWords")

    var t = new Word2Phrase().setInputCol("inputWords").setOutputCol("out")

    var model = t.fit(wordDataFrame)
    
    val newInstance = testDefaultReadWrite(model)
    assert(newInstance.bigram_list === model.bigram_list)
  }


}