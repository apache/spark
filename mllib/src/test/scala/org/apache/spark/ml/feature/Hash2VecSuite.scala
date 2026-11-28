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

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

class Hash2VecSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  private val seedValue = 7L
  private val indexSeed = java.lang.Long.hashCode(seedValue)
  private val signSeed = Murmur3_x86_32.hashLong(seedValue, indexSeed)

  private def estimator: Hash2Vec = new Hash2Vec()
    .setInputCol("seq")
    .setOutputCol("out")
    .setVectorSize(16)
    .setWindowSize(2)
    .setSeed(seedValue)

  private def vectorsOf[K](model: Hash2VecModel): Map[K, Vector] =
    model.getVectors.collect().map(r => r.getAs[K](0) -> r.getAs[Vector](1)).toMap

  private def expectedLongs(
      sequences: Seq[Seq[Long]],
      size: Int,
      weights: Array[Double]): Map[Long, Array[Double]] = {
    val result = scala.collection.mutable.Map.empty[Long, Array[Double]]
    val window = weights.length - 1
    for (sequence <- sequences; i <- sequence.indices) {
      val target = result.getOrElseUpdate(sequence(i), new Array[Double](size))
      for (c <- math.max(0, i - window) to math.min(sequence.length - 1, i + window) if c != i) {
        val index = Utils.nonNegativeMod(Murmur3_x86_32.hashLong(sequence(c), indexSeed), size)
        val sign = if ((Murmur3_x86_32.hashLong(sequence(c), signSeed) & 1) == 1) 1.0 else -1.0
        target(index) += sign * weights(math.abs(c - i))
      }
    }
    result.toMap
  }

  private def assertVectors(actual: Map[Long, Vector], expected: Map[Long, Array[Double]]): Unit = {
    assert(actual.keySet === expected.keySet)
    expected.foreach { case (word, values) =>
      assert(actual(word) ~== Vectors.dense(values) absTol 1e-12)
    }
  }

  private def assertFitFails(df: DataFrame, message: String): Unit = {
    val e = intercept[SparkException](estimator.fit(df))
    assert(e.getMessage.contains(message) ||
      Option(e.getCause).exists(_.getMessage.contains(message)))
  }

  test("params") {
    ParamsSuite.checkParams(new Hash2Vec)
    val model = estimator.fit(Seq(Seq(1L, 2L)).toDF("seq"))
    ParamsSuite.checkParams(model)
  }

  test("invalid params") {
    intercept[IllegalArgumentException](new Hash2Vec().setVectorSize(0))
    intercept[IllegalArgumentException](new Hash2Vec().setWindowSize(0))
    intercept[IllegalArgumentException](new Hash2Vec().setNumPartitions(0))
    intercept[IllegalArgumentException](new Hash2Vec().setDecayFunction("unknown"))
    intercept[IllegalArgumentException](new Hash2Vec().setDecayScale(0.0))
    intercept[IllegalArgumentException](new Hash2Vec().setNormalization("unknown"))
    intercept[IllegalArgumentException](new Hash2Vec().setFinalStorageLevel("unknown"))
  }

  test("vectors match a reference computation, including repeated tokens") {
    val sequences = Seq(Seq(1L, 2L, 1L, 3L, 2L), Seq(3L, 4L, 3L, 4L), Seq(5L), Seq(1L, 1L, 1L))
    Seq("constant", "gaussian", "exponential", "linear", "harmonic").foreach { decay =>
      val model = estimator.setNormalization("none").setDecayFunction(decay)
        .fit(sequences.toDF("seq"))
      val weights = Hash2Vec.decayWeights(decay, 2.0, 2)
      assertVectors(vectorsOf[Long](model), expectedLongs(sequences, 16, weights))
    }
  }

  test("result does not depend on input or output partitioning") {
    val sequences = Seq.tabulate(200)(i => Seq.tabulate(10)(j => ((i * 7 + j * j) % 23).toLong))
    val expected = expectedLongs(sequences, 16, Hash2Vec.decayWeights("gaussian", 2.0, 2))
    Seq((1, 1), (3, 7), (8, 2)).foreach { case (inputPartitions, outputPartitions) =>
      val df = sc.parallelize(sequences, inputPartitions).toDF("seq")
      val model = estimator.setNormalization("none").setNumPartitions(outputPartitions).fit(df)
      assert(model.getVectors.rdd.getNumPartitions === outputPartitions)
      assertVectors(vectorsOf[Long](model), expected)
    }
  }

  test("integral element types produce the same vectors") {
    val longModel = estimator.fit(Seq(Seq(1L, 2L, 3L, 2L)).toDF("seq"))
    val longs = vectorsOf[Long](longModel)
    val ints = estimator.fit(Seq(Seq(1, 2, 3, 2)).toDF("seq"))
    val shorts = estimator.fit(Seq(Seq[Short](1, 2, 3, 2)).toDF("seq"))
    val bytes = estimator.fit(Seq(Seq[Byte](1, 2, 3, 2)).toDF("seq"))
    Seq(longModel, ints, shorts, bytes).foreach { model =>
      assert(model.getVectors.schema("word").dataType === LongType)
      assert(vectorsOf[Long](model) === longs)
    }
  }

  test("string tokens") {
    // scalastyle:off nonascii
    val sequences = Seq(Seq("a", "b", "a", "c"), Seq("\u00e9", "b", "\u4e2d"))
    // scalastyle:on nonascii
    val model = estimator.setNormalization("none").fit(sequences.toDF("seq"))
    assert(model.getVectors.schema("word").dataType === StringType)
    val vectors = vectorsOf[String](model)
    assert(vectors.keySet === sequences.flatten.toSet)

    def hash(token: String, seed: Int): Int = {
      val s = UTF8String.fromString(token)
      Murmur3_x86_32.hashUnsafeBytes2(s.getBaseObject, s.getBaseOffset, s.numBytes(), seed)
    }
    val weight = math.exp(-1.0 / 8.0)
    val expected = new Array[Double](16)
    val sign = if ((hash("b", signSeed) & 1) == 1) 1.0 else -1.0
    expected(Utils.nonNegativeMod(hash("b", indexSeed), 16)) += sign * weight
    val contextOfSingleNeighbor = estimator.setNormalization("none")
      .fit(Seq(Seq("x", "b")).toDF("seq"))
    assert(vectorsOf[String](contextOfSingleNeighbor)("x") ~==
      Vectors.dense(expected) absTol 1e-12)
  }

  test("normalization") {
    val df = Seq(Seq(1L, 2L, 3L, 4L, 1L), Seq(5L)).toDF("seq")
    val raw = vectorsOf[Long](estimator.setNormalization("none").fit(df))

    val l2 = vectorsOf[Long](estimator.setNormalization("l2").fit(df))
    l2.foreach { case (word, vector) =>
      assert(vector.size === 16)
      if (word == 5L) {
        assert(vector.numNonzeros === 0)
      } else {
        assert(Vectors.norm(vector, 2) ~== 1.0 absTol 1e-12)
      }
    }

    val withMagnitude = vectorsOf[Long](estimator.setNormalization("l2WithMagnitude").fit(df))
    withMagnitude.foreach { case (word, vector) =>
      assert(vector.size === 17)
      assert(Vectors.dense(vector.toArray.take(16)) ~== l2(word) absTol 1e-12)
      val expected = math.log(Vectors.norm(raw(word), 2) + 1.0) / 4.0
      assert(vector(16) ~== expected absTol 1e-12)
    }
  }

  test("empty sequences are ignored") {
    val model = estimator.fit(Seq(Seq(1L, 2L), Seq.empty[Long]).toDF("seq"))
    assert(vectorsOf[Long](model).keySet === Set(1L, 2L))

    val empty = estimator.fit(Seq(Seq.empty[Long]).toDF("seq"))
    assert(empty.getVectors.isEmpty)
  }

  test("fit fails on null sequences and null elements") {
    assertFitFails(Seq(Some(Seq(1L)), None).toDF("seq"), "null sequences")
    assertFitFails(Seq(Some(Seq("a")), None).toDF("seq"), "null sequences")
    assertFitFails(spark.sql("SELECT array(1L, NULL, 2L) AS seq"), "null elements")
    assertFitFails(spark.sql("SELECT array('a', NULL, 'b') AS seq"), "null elements")
  }

  test("transform averages token vectors like Word2Vec") {
    val model = estimator.fit(Seq(Seq("a", "b", "c"), Seq("b", "c", "a")).toDF("seq"))
    val vectors = vectorsOf[String](model)
    def average(tokens: String*)(length: Int): Vector = {
      val sum = new Array[Double](16)
      tokens.foreach(t => vectors(t).toArray.indices.foreach(i => sum(i) += vectors(t)(i)))
      Vectors.dense(sum.map(_ / length))
    }

    val input = Seq(
      (1, Seq("a", "b")),
      (2, Seq("a", "unknown")),
      (3, Seq("unknown")),
      (4, Seq("a", "a")),
      (5, Seq.empty[String])).toDF("id", "seq")
    val result = model.transform(input).select("id", "seq", "out").collect()
      .map(r => r.getInt(0) -> r.getAs[Vector](2)).toMap

    assert(result.size === 5)
    assert(result(1) ~== average("a", "b")(2) absTol 1e-12)
    assert(result(2) ~== average("a")(2) absTol 1e-12)
    assert(result(3) ~== Vectors.zeros(16) absTol 1e-12)
    assert(result(4) ~== average("a", "a")(2) absTol 1e-12)
    assert(result(5) === Vectors.sparse(16, Array.emptyIntArray, Array.emptyDoubleArray))
  }

  test("transform counts null elements and fails on null sequences") {
    val model = estimator.fit(Seq(Seq(1L, 2L)).toDF("seq"))
    val one = vectorsOf[Long](model)(1L).toArray

    val result = model.transform(spark.sql("SELECT array(1L, NULL) AS seq"))
      .select("out").head().getAs[Vector](0)
    assert(result ~== Vectors.dense(one.map(_ / 2)) absTol 1e-12)

    val nullSequence = Seq(Option.empty[Seq[Long]]).toDF("seq")
    val e = intercept[Exception](model.transform(nullSequence).collect())
    assert(e.getMessage.contains("null sequences"))
  }

  test("transform keeps input columns, integral casts and output metadata") {
    val model = estimator.setNormalization("l2WithMagnitude")
      .fit(Seq(Seq(1L, 2L, 3L)).toDF("seq"))
    val input = Seq((Seq(1, 2), Map("k" -> 1), "x"), (Seq(3, 3), Map("k" -> 2), "y"))
      .toDF("seq", "a.b", "c")
    val output = model.transform(input)

    assert(output.columns.toSeq === Seq("seq", "a.b", "c", "out"))
    assert(AttributeGroup.fromStructField(output.schema("out")).size === 17)
    val rows = output.select("c", "`a.b`", "out").collect()
    assert(rows.map(_.getString(0)).toSet === Set("x", "y"))
    assert(rows.forall(_.getAs[Vector](2).size === 17))
    assert(rows.find(_.getString(0) == "y").get.getAs[Map[String, Int]](1) === Map("k" -> 2))
  }

  test("transform rejects a different kind of tokens") {
    val model = estimator.fit(Seq(Seq(1L, 2L)).toDF("seq"))
    intercept[IllegalArgumentException](model.transform(Seq(Seq("a")).toDF("seq")))
  }

  test("different seeds produce different vectors") {
    val df = Seq(Seq(1L, 2L, 3L, 4L, 5L, 6L)).toDF("seq")
    val first = vectorsOf[Long](estimator.fit(df))
    val same = vectorsOf[Long](estimator.fit(df))
    val other = vectorsOf[Long](estimator.setSeed(seedValue + 1).fit(df))
    assert(first === same)
    assert(first !== other)
  }

  test("the fitted vectors are persisted") {
    val model = estimator.fit(Seq(Seq(1L, 2L)).toDF("seq"))
    assert(model.getVectors.storageLevel.useMemory)
    val notPersisted = estimator.setFinalStorageLevel("NONE").fit(Seq(Seq(1L, 2L)).toDF("seq"))
    assert(!notPersisted.getVectors.storageLevel.isValid ||
      !notPersisted.getVectors.storageLevel.useMemory)
  }

  test("model estimated size") {
    val model = estimator.fit(Seq(Seq(1L, 2L, 3L)).toDF("seq"))
    assert(model.estimatedSize > 3L * 16 * 8)
  }

  test("read/write") {
    def checkModelData(left: Hash2VecModel, right: Hash2VecModel): Unit = {
      val leftRows = left.getVectors.collect().map(r => r.get(0) -> r.getAs[Vector](1)).toMap
      val rightRows = right.getVectors.collect().map(r => r.get(0) -> r.getAs[Vector](1)).toMap
      assert(leftRows === rightRows)
    }
    val params = Map[String, Any](
      "inputCol" -> "seq", "outputCol" -> "out", "vectorSize" -> 8, "windowSize" -> 3,
      "decayFunction" -> "harmonic", "normalization" -> "l2WithMagnitude", "seed" -> 11L)
    Seq(
      Seq(Seq(1L, 2L, 3L), Seq(3L, 4L)).toDF("seq"),
      Seq(Seq("a", "b", "c"), Seq("c", "d")).toDF("seq")).foreach { df =>
      testEstimatorAndModelReadWrite(new Hash2Vec(), df, params, params, checkModelData)
    }
  }

  test("toString") {
    val model = estimator.fit(Seq(Seq(1L, 2L)).toDF("seq"))
    assert(model.toString === s"Hash2VecModel: uid=${model.uid}, vectorSize=16")
    assert(model.copy(org.apache.spark.ml.param.ParamMap.empty).getVectors eq model.getVectors)
  }
}
