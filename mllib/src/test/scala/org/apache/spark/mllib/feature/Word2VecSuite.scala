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

package org.apache.spark.mllib.feature

import java.io.File
import java.io.FileOutputStream
import java.nio.ByteBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.Utils

class Word2VecSuite extends SparkFunSuite with MLlibTestSparkContext {

  // TODO: add more tests

  test("Word2Vec") {
    val sentence = "a b " * 100 + "a c " * 10
    val localDoc = Seq(sentence, sentence)
    val doc = sc.parallelize(localDoc)
      .map(line => line.split(" ").toSeq)
    val model = new Word2Vec().setVectorSize(10).setSeed(42L).fit(doc)
    val syms = model.findSynonyms("a", 2)
    assert(syms.length == 2)
    assert(syms(0)._1 == "b")
    assert(syms(1)._1 == "c")

    // Test that model built using Word2Vec, i.e wordVectors and wordIndec
    // and a Word2VecMap give the same values.
    val word2VecMap = model.getVectors
    val newModel = new Word2VecModel(word2VecMap)
    assert(newModel.getVectors.mapValues(_.toSeq) === word2VecMap.mapValues(_.toSeq))
  }

  test("Word2Vec throws exception when vocabulary is empty") {
    intercept[IllegalArgumentException] {
      val sentence = "a b c"
      val localDoc = Seq(sentence, sentence)
      val doc = sc.parallelize(localDoc)
        .map(line => line.split(" ").toSeq)
      new Word2Vec().setMinCount(10).fit(doc)
    }
  }

  test("Word2VecModel") {
    val num = 2
    val word2VecMap = Map(
      ("china", Array(0.50f, 0.50f, 0.50f, 0.50f)),
      ("japan", Array(0.40f, 0.50f, 0.50f, 0.50f)),
      ("taiwan", Array(0.60f, 0.50f, 0.50f, 0.50f)),
      ("korea", Array(0.45f, 0.60f, 0.60f, 0.60f))
    )
    val model = new Word2VecModel(word2VecMap)
    val syms = model.findSynonyms("china", num)
    assert(syms.length == num)
    assert(syms(0)._1 == "taiwan")
    assert(syms(1)._1 == "japan")
  }

  test("model load / save") {

    val word2VecMap = Map(
      ("china", Array(0.50f, 0.50f, 0.50f, 0.50f)),
      ("japan", Array(0.40f, 0.50f, 0.50f, 0.50f)),
      ("taiwan", Array(0.60f, 0.50f, 0.50f, 0.50f)),
      ("korea", Array(0.45f, 0.60f, 0.60f, 0.60f))
    )
    val model = new Word2VecModel(word2VecMap)

    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    try {
      model.save(sc, path)
      val sameModel = Word2VecModel.load(sc, path)
      assert(sameModel.getVectors.mapValues(_.toSeq) === model.getVectors.mapValues(_.toSeq))
    } finally {
      Utils.deleteRecursively(tempDir)
    }

  }

  test("big model load / save") {
    // backupping old values
    val oldBufferConfValue = spark.conf.get("spark.kryoserializer.buffer.max", "64m")
    val oldBufferMaxConfValue = spark.conf.get("spark.kryoserializer.buffer", "64k")

    // setting test values to trigger partitioning
    spark.conf.set("spark.kryoserializer.buffer", "50b")
    spark.conf.set("spark.kryoserializer.buffer.max", "50b")

    // create a model bigger than 50 Bytes
    val word2VecMap = Map((0 to 10).map(i => s"$i" -> Array.fill(10)(0.1f)): _*)
    val model = new Word2VecModel(word2VecMap)

    // est. size of this model, given the formula:
    // (floatSize * vectorSize + 15) * numWords
    // (4 * 10 + 15) * 10 = 550
    // therefore it should generate multiple partitions
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    try {
      model.save(sc, path)
      val sameModel = Word2VecModel.load(sc, path)
      assert(sameModel.getVectors.mapValues(_.toSeq) === model.getVectors.mapValues(_.toSeq))
    }
    catch {
      case t: Throwable => fail("exception thrown persisting a model " +
        "that spans over multiple partitions", t)
    } finally {
      Utils.deleteRecursively(tempDir)
      spark.conf.set("spark.kryoserializer.buffer", oldBufferConfValue)
      spark.conf.set("spark.kryoserializer.buffer.max", oldBufferMaxConfValue)
    }

  }

  test("load Google word2vec model") {
    val tempDir = Utils.createTempDir()
    val modelFile = new File(tempDir, "google-word2vec-mllib-00000.bin")

    // This byte array is pre-trained model by Google word2vec.
    // training text:
    //   "a a a a a b b b b b b c c c c c c c"
    // Parameters for training:
    //   -cbow 1 -size 5 -window 5 -negative 0 -hs 0 -sample 1e-4 -threads 4 -binary 1 -iter 3
    val testModelBytes = Array[Byte](52, 32, 53, 10, 60, 47, 115, 62, 32, 51, -13, -93, 61, -51,
      4, -75, 61, 51, -29, -100, -67, -51, 68, -122, -67, 102, -26, -33, 60, 10, 99, 32, -51, 124,
      119, 61, 102, 38, -102, 60, 0, -128, -118, 59, -102, -103, -109, -67, -51, -68, 53, 61, 10,
      98, 32, 0, 112, -78, -67, -102, -71, -52, 60, 51, 51, 118, -68, 51, -45, -100, -68, 0, -48,
      -121, -67, 10, 97, 32, 0, 48, 26, -67, -51, 76, 83, 61, 102, -42, 119, 61, -102, 57, 115,
      61, -51, -36, 2, 61, 10)

    val bbuf = ByteBuffer.wrap(testModelBytes)
    // write model to file
    val file = new FileOutputStream(modelFile)
    val channel = file.getChannel
    channel.write(bbuf)
    channel.close()
    file.close()

    try {
      val model = Word2VecModel.loadGoogleModel(sc, modelFile.getAbsolutePath)

      val num = 2
      val syms = model.findSynonyms("a", num)
      assert(syms.length == num)
      assert(syms(0)._1 == "b" && (syms(0)._2 ~== 0.031741 absTol 1e-5))
      assert(syms(1)._1 == "c" && (syms(1)._2 ~== -0.333541 absTol 1e-5))
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  test("test similarity for word vectors with large values is not Infinity or NaN") {
    val vecA = Array(-4.331467827487745E21, -5.26707742075006E21,
      5.63551690626524E21, 2.833692188614257E21, -1.9688159903619345E21, -4.933950659913092E21,
      -2.7401535502536787E21, -1.418671793782632E20).map(_.toFloat)
    val vecB = Array(-3.9850175451103232E16, -3.4829783883841536E16,
      9.421469251534848E15, 4.4069684466679808E16, 7.20936298872832E15, -4.2883302830374912E16,
      -3.605579947835392E16, -2.8151294422155264E16).map(_.toFloat)
    val vecC = Array(-1.9227381025734656E16, -3.907009342603264E16,
      2.110207626838016E15, -4.8770066610651136E16, -1.9734964555743232E16, -3.2206001247617024E16,
      2.7725358220443648E16, 3.1618718156980224E16).map(_.toFloat)
    val wordMapIn = Map(
      ("A", vecA),
      ("B", vecB),
      ("C", vecC)
    )

    val model = new Word2VecModel(wordMapIn)
    model.findSynonyms("A", 5).foreach { pair =>
      assert(!(pair._2.isInfinite || pair._2.isNaN))
    }
  }

}
