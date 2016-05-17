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
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.feature.{Word2VecModel => OldWord2VecModel}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row

class Word2VecSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("params") {
    ParamsSuite.checkParams(new Word2Vec)
    val model = new Word2VecModel("w2v", new OldWord2VecModel(Map("a" -> Array(0.0f))))
    ParamsSuite.checkParams(model)
  }

  test("Word2Vec") {

    val spark = this.spark
    import spark.implicits._

    val sentence = "a b " * 100 + "a c " * 10
    val numOfWords = sentence.split(" ").size
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))

    val codes = Map(
      "a" -> Array(-0.2811822295188904, -0.6356269121170044, -0.3020961284637451),
      "b" -> Array(1.0309048891067505, -1.29472815990448, 0.22276712954044342),
      "c" -> Array(-0.08456747233867645, 0.5137411952018738, 0.11731560528278351)
    )

    val expected = doc.map { sentence =>
      Vectors.dense(sentence.map(codes.apply).reduce((word1, word2) =>
        word1.zip(word2).map { case (v1, v2) => v1 + v2 }
      ).map(_ / numOfWords))
    }

    val docDF = doc.zip(expected).toDF("text", "expected")

    val model = new Word2Vec()
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(model)

    // These expectations are just magic values, characterizing the current
    // behavior.  The test needs to be updated to be more general, see SPARK-11502
    val magicExp = Vectors.dense(0.30153007534417237, -0.6833061711354689, 0.5116530778733167)
    model.transform(docDF).select("result", "expected").collect().foreach {
      case Row(vector1: Vector, vector2: Vector) =>
        assert(vector1 ~== magicExp absTol 1E-5, "Transformed vector is different with expected.")
    }
  }

  test("getVectors") {

    val spark = this.spark
    import spark.implicits._

    val sentence = "a b " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))

    val codes = Map(
      "a" -> Array(-0.2811822295188904, -0.6356269121170044, -0.3020961284637451),
      "b" -> Array(1.0309048891067505, -1.29472815990448, 0.22276712954044342),
      "c" -> Array(-0.08456747233867645, 0.5137411952018738, 0.11731560528278351)
    )
    val expectedVectors = codes.toSeq.sortBy(_._1).map { case (w, v) => Vectors.dense(v) }

    val docDF = doc.zip(doc).toDF("text", "alsotext")

    val model = new Word2Vec()
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    val realVectors = model.getVectors.sort("word").select("vector").rdd.map {
      case Row(v: Vector) => v
    }.collect()
    // These expectations are just magic values, characterizing the current
    // behavior.  The test needs to be updated to be more general, see SPARK-11502
    val magicExpected = Seq(
      Vectors.dense(0.3326166272163391, -0.5603077411651611, -0.2309209555387497),
      Vectors.dense(0.32463887333869934, -0.9306551218032837, 1.393115520477295),
      Vectors.dense(-0.27150997519493103, 0.4372006058692932, -0.13465698063373566)
    )

    realVectors.zip(magicExpected).foreach {
      case (real, expected) =>
        assert(real ~== expected absTol 1E-5, "Actual vector is different from expected.")
    }
  }

  test("findSynonyms") {

    val spark = this.spark
    import spark.implicits._

    val sentence = "a b " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))
    val docDF = doc.zip(doc).toDF("text", "alsotext")

    val model = new Word2Vec()
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    val expectedSimilarity = Array(0.2608488929093532, -0.8271274846926078)
    val (synonyms, similarity) = model.findSynonyms("a", 2).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collect().unzip

    assert(synonyms.toArray === Array("b", "c"))
    expectedSimilarity.zip(similarity).map {
      case (expected, actual) => assert(math.abs((expected - actual) / expected) < 1E-5)
    }
  }

  test("window size") {

    val spark = this.spark
    import spark.implicits._

    val sentence = "a q s t q s t b b b s t m s t m q " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))
    val docDF = doc.zip(doc).toDF("text", "alsotext")

    val model = new Word2Vec()
      .setVectorSize(3)
      .setWindowSize(2)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    val (synonyms, similarity) = model.findSynonyms("a", 6).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collect().unzip

    // Increase the window size
    val biggerModel = new Word2Vec()
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .setWindowSize(10)
      .fit(docDF)

    val (synonymsLarger, similarityLarger) = model.findSynonyms("a", 6).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collect().unzip
    // The similarity score should be very different with the larger window
    assert(math.abs(similarity(5) - similarityLarger(5) / similarity(5)) > 1E-5)
  }

  test("Word2Vec read/write") {
    val t = new Word2Vec()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setMaxIter(2)
      .setMinCount(8)
      .setNumPartitions(1)
      .setSeed(42L)
      .setStepSize(0.01)
      .setVectorSize(100)
    testDefaultReadWrite(t)
  }

  test("Word2VecModel read/write") {
    val word2VecMap = Map(
      ("china", Array(0.50f, 0.50f, 0.50f, 0.50f)),
      ("japan", Array(0.40f, 0.50f, 0.50f, 0.50f)),
      ("taiwan", Array(0.60f, 0.50f, 0.50f, 0.50f)),
      ("korea", Array(0.45f, 0.60f, 0.60f, 0.60f))
    )
    val oldModel = new OldWord2VecModel(word2VecMap)
    val instance = new Word2VecModel("myWord2VecModel", oldModel)
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.getVectors.collect() === instance.getVectors.collect())
  }
}

