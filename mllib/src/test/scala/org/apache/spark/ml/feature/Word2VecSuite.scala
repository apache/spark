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

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.feature.{Word2VecModel => OldWord2VecModel}
import org.apache.spark.sql.Row
import org.apache.spark.util.Utils

class Word2VecSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new Word2Vec)
    val model = new Word2VecModel("w2v", new OldWord2VecModel(Map("a" -> Array(0.0f))))
    ParamsSuite.checkParams(model)
  }

  test("Word2Vec") {
    val sentence = "a b " * 100 + "a c " * 10
    val numOfWords = sentence.split(" ").length
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

    val w2v = new Word2Vec()
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
    val model = w2v.fit(docDF)

    val transformed = model.transform(docDF)
    checkVectorSizeOnDF(transformed, "result", model.getVectorSize)

    MLTestingUtils.checkCopyAndUids(w2v, model)

    // These expectations are just magic values, characterizing the current
    // behavior.  The test needs to be updated to be more general, see SPARK-11502
    val magicExp = Vectors.dense(-0.11654884266582402, 0.3115301721475341, -0.6879349987615239)
    testTransformer[(Seq[String], Vector)](docDF, model, "result", "expected") {
      case Row(vector1: Vector, vector2: Vector) =>
        assert(vector1 ~== magicExp absTol 1E-5, "Transformed vector is different with expected.")
    }
  }

  test("getVectors") {
    val sentence = "a b " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))
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
      Vectors.dense(0.12662248313426971, 0.6108677387237549, -0.006755620241165161),
      Vectors.dense(-0.3870747685432434, 0.023309476673603058, -1.567158818244934),
      Vectors.dense(-0.08617416769266129, -0.09897610545158386, 0.6113300323486328)
    )

    realVectors.zip(magicExpected).foreach {
      case (real, expected) =>
        assert(real ~== expected absTol 1E-5, "Actual vector is different from expected.")
    }
  }

  test("findSynonyms") {

    val sentence = "a b " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))
    val docDF = doc.zip(doc).toDF("text", "alsotext")

    val model = new Word2Vec()
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    val expected = Map(("b", -0.024012837558984756), ("c", -0.19355152547359467))
    val findSynonymsResult = model.findSynonyms("a", 2).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collectAsMap()

    expected.foreach {
      case (expectedSynonym, expectedSimilarity) =>
        assert(findSynonymsResult.contains(expectedSynonym))
        assert(expectedSimilarity ~== findSynonymsResult(expectedSynonym) absTol 1E-5)
    }

    val findSynonymsArrayResult = model.findSynonymsArray("a", 2).toMap
    findSynonymsResult.foreach {
      case (expectedSynonym, expectedSimilarity) =>
        assert(findSynonymsArrayResult.contains(expectedSynonym))
        assert(expectedSimilarity ~== findSynonymsArrayResult(expectedSynonym) absTol 1E-5)
    }
  }

  test("window size") {

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

  test("Word2Vec read/write numPartitions calculation") {
    val smallModelNumPartitions = Word2VecModel.Word2VecModelWriter.calculateNumberOfPartitions(
      Utils.byteStringAsBytes("64m"), numWords = 10, vectorSize = 5)
    assert(smallModelNumPartitions === 1)
    val largeModelNumPartitions = Word2VecModel.Word2VecModelWriter.calculateNumberOfPartitions(
      Utils.byteStringAsBytes("64m"), numWords = 1000000, vectorSize = 5000)
    assert(largeModelNumPartitions > 1)
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
      .setMaxSentenceLength(500)
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
    assert(newInstance.getVectors.collect().sortBy(_.getString(0)) ===
      instance.getVectors.collect().sortBy(_.getString(0)))
  }

  test("Word2Vec works with input that is non-nullable (NGram)") {

    val sentence = "a q s t q s t b b b s t m s t m q "
    val docDF = sc.parallelize(Seq(sentence, sentence)).map(_.split(" ")).toDF("text")

    val ngram = new NGram().setN(2).setInputCol("text").setOutputCol("ngrams")
    val ngramDF = ngram.transform(docDF)

    val model = new Word2Vec()
      .setVectorSize(2)
      .setInputCol("ngrams")
      .setOutputCol("result")
      .fit(ngramDF)

    // Just test that this transformation succeeds
    testTransformerByGlobalCheckFunc[(Seq[String], Seq[String])](ngramDF, model, "result") { _ => }
  }

}

