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

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.internal.SQLConf._
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

  test("findSynonyms doesn't reject similar word vectors when called with a vector") {
    val num = 2
    val word2VecMap = Map(
      ("china", Array(0.50f, 0.50f, 0.50f, 0.50f)),
      ("japan", Array(0.40f, 0.50f, 0.50f, 0.50f)),
      ("taiwan", Array(0.60f, 0.50f, 0.50f, 0.50f)),
      ("korea", Array(0.45f, 0.60f, 0.60f, 0.60f))
    )
    val model = new Word2VecModel(word2VecMap)
    val syms = model.findSynonyms(Vectors.dense(Array(0.52, 0.5, 0.5, 0.5)), num)
    assert(syms.length == num)
    assert(syms(0)._1 == "china")
    assert(syms(1)._1 == "taiwan")
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
    val oldBufferConfValue = spark.conf.get(KRYO_SERIALIZER_BUFFER_SIZE.key, "64m")
    val oldBufferMaxConfValue = spark.conf.get(KRYO_SERIALIZER_MAX_BUFFER_SIZE.key, "64k")
    val oldSetCommandRejectsSparkCoreConfs = spark.conf.get(
      SET_COMMAND_REJECTS_SPARK_CORE_CONFS.key, "true")

    // setting test values to trigger partitioning

    // this is needed to set configurations which are also defined to SparkConf
    spark.conf.set(SET_COMMAND_REJECTS_SPARK_CORE_CONFS.key, "false")
    spark.conf.set(KRYO_SERIALIZER_BUFFER_SIZE.key, "50b")

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
      spark.conf.set(KRYO_SERIALIZER_BUFFER_SIZE.key, oldBufferConfValue)
      spark.conf.set(KRYO_SERIALIZER_MAX_BUFFER_SIZE.key, oldBufferMaxConfValue)
      spark.conf.set(SET_COMMAND_REJECTS_SPARK_CORE_CONFS.key, oldSetCommandRejectsSparkCoreConfs)
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
