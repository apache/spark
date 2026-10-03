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

package org.apache.spark.graphframes.embeddings

import scala.util.Random

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType

class Hash2VecSuite extends SparkFunSuite with GraphFrameTestSparkContext with BeforeAndAfterAll {
  private var longSequences: DataFrame = _
  private var stringSequences: DataFrame = _
  private var uniqueElementsCnt: Int = _

  private def approxEqual(left: Double, right: Double, err: Double = 1e-6.toDouble): Boolean =
    math.abs(left - right) < err

  override def beforeAll(): Unit = {
    super.beforeAll()

    val rng = new Random(42L)

    val sequences =
      (1 to 100).map(idx => (idx, (1 to 20).map(_ => rng.nextInt(30).toLong).toSeq)).toSeq

    uniqueElementsCnt = sequences.flatMap(f => f._2).distinct.length

    val strSequences = sequences.map(f => (f._1, f._2.map(_.toString()))).toSeq

    longSequences = spark.createDataFrame(sequences).toDF("id", "seq")
    stringSequences = spark.createDataFrame(strSequences).toDF("id", "seq")
  }

  test("hash2vec long input") {
    val hash2vecResults = new Hash2Vec().setSequenceCol("seq").run(longSequences)
    assert(hash2vecResults.schema.fields.length === 2)
    assert(hash2vecResults.schema.fields.map(_.name).toSeq === Seq("id", "vector"))
    assert(hash2vecResults.schema("id").dataType === LongType)
    assert(hash2vecResults.schema("vector").dataType === VectorType)
    val collected = hash2vecResults.collect()
    assert(collected.length === uniqueElementsCnt)
  }

  test("hash2vec string input") {
    val hash2vecResults = new Hash2Vec().setSequenceCol("seq").run(stringSequences)
    assert(hash2vecResults.schema.fields.length === 2)
    assert(hash2vecResults.schema.fields.map(_.name).toSeq === Seq("id", "vector"))
    assert(hash2vecResults.schema("id").dataType === StringType)
    assert(hash2vecResults.schema("vector").dataType === VectorType)
    val collected = hash2vecResults.collect()
    assert(collected.length === uniqueElementsCnt)
  }

  test("hash2vec reproducible with seed") {
    val hash2vecResults =
      new Hash2Vec().setSequenceCol("seq").setHashingSeed(42).run(longSequences)
    val hash2vecResults2 =
      new Hash2Vec().setSequenceCol("seq").setHashingSeed(42).run(longSequences)
    val hash2vecResults3 =
      new Hash2Vec().setSequenceCol("seq").setHashingSeed(43).run(longSequences)

    val shouldMatch = hash2vecResults
      .withColumnRenamed("vector", "left")
      .join(hash2vecResults2, Seq("id"), "inner")

    assert(shouldMatch.count() === hash2vecResults.count())
    assert(shouldMatch.filter(col("left") =!= col("vector")).count() === 0)

    val shouldNotMatch = hash2vecResults
      .withColumnRenamed("vector", "left")
      .join(hash2vecResults3, Seq("id"), "inner")

    assert(shouldNotMatch.count() === uniqueElementsCnt)
    assert(shouldNotMatch.filter(col("left") =!= col("vector")).count() > 0)
  }

  test("hash2vec L2") {
    val hash2vecResults =
      new Hash2Vec()
        .setDoNormalization(true, false)
        .setSequenceCol("seq")
        .run(longSequences)
        .collect()

    def naiveL2norm(vector: DenseVector): Double = {
      val squaredSum = vector.values.map(el => el * el).sum[Double]
      math.sqrt(squaredSum)
    }

    assert(
      hash2vecResults
        .map(r => r.getAs[DenseVector](1))
        .map(v => naiveL2norm(v))
        .forall(f => approxEqual(f, 1.0)))
  }

  test("hash2vec safe L2") {
    val hash2vecResults = new Hash2Vec()
      .setDoNormalization(true, true)
      .setEmbeddingsDim(128)
      .setSequenceCol("seq")
      .run(longSequences)
      .collect()

    assert(hash2vecResults.forall(r => r.getAs[DenseVector](1).size === 129))
  }

  test("constant decay") {
    val hash2vecResults = new Hash2Vec()
      .setDecayFunction("constant")
      .setSequenceCol("seq")
      .run(longSequences)
    hash2vecResults.write.mode("overwrite").format("noop").save()
  }

  test("context longer than sequence") {
    val hash2vecResults = new Hash2Vec()
      .setSequenceCol("seq")
      .setContextSize(30)
      .run(longSequences)
    hash2vecResults.write.mode("overwrite").format("noop").save()
  }

  test("PagedMatrixDouble helper - page extension") {
    val dim = 50
    val matrix = new Hash2Vec.PagedMatrixDouble(dim)
    // Allocate enough vectors to force a second page (PAGE_SIZE = 4096)
    // We'll allocate two pages' worth to be sure.
    // For simplicity, allocate 2 * PAGE_SIZE vectors.
    val PAGE_SIZE = 1 << 12 // 4096
    val totalVectors = 2 * PAGE_SIZE
    val ids = (0 until totalVectors).map { _ =>
      matrix.allocateVector()
    }
    // IDs should be 0,1,2,...,totalVectors-1
    assert(ids.toSeq == (0 until totalVectors).toSeq)
    // Check that internal pages count grew
    // Since internal structure is private, we just verify no exception occurred.
  }

  test("PagedMatrixDouble helper - add and retrieve") {
    val dim = 10
    val matrix = new Hash2Vec.PagedMatrixDouble(dim)
    val id0 = matrix.allocateVector()
    val id1 = matrix.allocateVector()
    assert(id0 == 0)
    assert(id1 == 1)

    // Add values to vector 0 at different offsets
    matrix.add(id0, 0, 5.0)
    matrix.add(id0, 3, 2.0)
    matrix.add(id0, 9, -1.0)

    // Add values to vector 1
    matrix.add(id1, 0, 10.0)
    matrix.add(id1, 5, 3.0)

    // Retrieve and verify
    val vec0 = matrix.getVector(id0)
    assert(vec0.length == dim)
    assert(vec0(0) == 5.0)
    assert(vec0(3) == 2.0)
    assert(vec0(9) == -1.0)
    // Other positions should be zero
    (0 until dim).filterNot(idx => idx == 0 || idx == 3 || idx == 9).foreach { idx =>
      assert(vec0(idx) == 0.0)
    }

    val vec1 = matrix.getVector(id1)
    assert(vec1.length == dim)
    assert(vec1(0) == 10.0)
    assert(vec1(5) == 3.0)
    (0 until dim).filterNot(idx => idx == 0 || idx == 5).foreach { idx =>
      assert(vec1(idx) == 0.0)
    }
  }

  test("PagedMatrixDouble helper - cross-page addressing") {
    val dim = 20
    val matrix = new Hash2Vec.PagedMatrixDouble(dim)
    val PAGE_SIZE = 1 << 12
    // Allocate vectors up to the end of first page
    for (_ <- 0 until PAGE_SIZE) matrix.allocateVector()
    val firstPageLastId = PAGE_SIZE - 1
    // Allocate first vector of second page
    val secondPageFirstId = matrix.allocateVector()
    assert(secondPageFirstId == PAGE_SIZE)

    // Add to the last vector of first page
    matrix.add(firstPageLastId, 0, 100.0)
    matrix.add(firstPageLastId, dim - 1, 200.0)

    // Add to the first vector of second page
    matrix.add(secondPageFirstId, 0, 300.0)
    matrix.add(secondPageFirstId, 10, 400.0)

    // Retrieve and verify
    val vecFirstPage = matrix.getVector(firstPageLastId)
    assert(vecFirstPage(0) == 100.0)
    assert(vecFirstPage(dim - 1) == 200.0)
    (1 until dim - 1).foreach { idx =>
      assert(vecFirstPage(idx) == 0.0)
    }

    val vecSecondPage = matrix.getVector(secondPageFirstId)
    assert(vecSecondPage(0) == 300.0)
    assert(vecSecondPage(10) == 400.0)
    (1 until dim).filterNot(_ == 10).foreach { idx =>
      assert(vecSecondPage(idx) == 0.0)
    }
  }

  test("Hash2Vec - cosine distances reflect co-occurrence patterns") {
    // Create a tiny dataset where some words co-occur often, others rarely.
    // We'll use string sequences for simplicity.
    val sequences = Seq(
      Seq("apple", "banana", "apple", "cherry", "banana"),
      Seq("apple", "banana", "cherry", "banana"),
      Seq("apple", "banana", "apple", "banana", "banana"),
      Seq("cherry", "date", "cherry", "date"),
      Seq("date", "elderberry", "date"),
      Seq("elderberry", "fig", "elderberry"),
      Seq("fig", "fig", "fig") // fig appears often alone
    )

    val df = spark.createDataFrame(sequences.map(Tuple1(_))).toDF("seq")

    val embeddings = new Hash2Vec()
      .setSequenceCol("seq")
      .setEmbeddingsDim(128) // enough dimensions to capture patterns
      .setContextSize(2)
      .setDecayFunction("constant")
      .setHashingSeed(777)
      .setSignHashSeed(888)
      .run(df)

    // Collect embeddings into a local map
    val embMap = embeddings
      .collect()
      .map { row =>
        val id = row.getString(0)
        val vec = row.getAs[DenseVector](1)
        id -> vec
      }
      .toMap

    // Helper to compute cosine similarity between two vectors
    def cosineSimilarity(v1: DenseVector, v2: DenseVector): Double = {
      val a = v1.values
      val b = v2.values
      require(a.length == b.length)
      var dot = 0.0
      var norm1 = 0.0
      var norm2 = 0.0
      var i = 0
      while (i < a.length) {
        dot += a(i) * b(i)
        norm1 += a(i) * a(i)
        norm2 += b(i) * b(i)
        i += 1
      }
      dot / (math.sqrt(norm1) * math.sqrt(norm2))
    }

    // apple and banana co-occur very frequently -> high similarity
    val appleBananaSim = cosineSimilarity(embMap("apple"), embMap("banana"))
    // cherry and date also co-occur frequently (in the fourth sequence)
    val cherryDateSim = cosineSimilarity(embMap("cherry"), embMap("date"))
    // apple and fig almost never appear together -> low similarity
    val appleFigSim = cosineSimilarity(embMap("apple"), embMap("fig"))
    // banana and fig also rarely together
    val bananaFigSim = cosineSimilarity(embMap("banana"), embMap("fig"))
    // elderberry and fig co-occur (sixth sequence)
    val elderberryFigSim = cosineSimilarity(embMap("elderberry"), embMap("fig"))

    // Assert ordering of similarities matches expected co-occurrence patterns
    // apple-banana should be among the highest similarities
    assert(appleBananaSim > 0.3, s"apple-banana similarity $appleBananaSim should be > 0.3")
    // apple-fig should be low (close to zero or negative)
    assert(
      appleFigSim < appleBananaSim,
      s"apple-fig ($appleFigSim) should be < apple-banana ($appleBananaSim)")
    assert(
      bananaFigSim < appleBananaSim,
      s"banana-fig ($bananaFigSim) should be < apple-banana ($appleBananaSim)")
    // cherry-date similarity should be relatively high (they co-occur exclusively)
    assert(cherryDateSim > 0.2, s"cherry-date similarity $cherryDateSim should be > 0.2")
    // elderberry-fig should be higher than apple-fig (because they co-occur)
    assert(
      elderberryFigSim > appleFigSim,
      s"elderberry-fig ($elderberryFigSim) should be > apple-fig ($appleFigSim)")

    // Self-similarity should be 1.0 (or close after normalization)
    val appleSelf = cosineSimilarity(embMap("apple"), embMap("apple"))
    assert(math.abs(appleSelf - 1.0) < 1e-6, s"self-similarity should be ~1.0, got $appleSelf")
  }

  test("Hash2Vec - long-typed co-occurrence") {
    // Use numeric ids to test long sequences.
    val sequences = Seq(
      Seq(1L, 2L, 1L, 3L, 2L), // 1-2 frequent, 3 appears with 2
      Seq(1L, 2L, 3L, 2L),
      Seq(1L, 2L, 1L, 2L, 2L),
      Seq(3L, 4L, 3L, 4L), // 3-4 frequent pair
      Seq(4L, 5L, 4L),
      Seq(5L, 6L, 5L),
      Seq(6L, 6L, 6L))

    val df = spark.createDataFrame(sequences.map(Tuple1(_))).toDF("seq")

    val embeddings = new Hash2Vec()
      .setSequenceCol("seq")
      .setEmbeddingsDim(128)
      .setContextSize(2)
      .setDecayFunction("constant")
      .setHashingSeed(777)
      .setSignHashSeed(888)
      .run(df)

    val embMap = embeddings
      .collect()
      .map { row =>
        val id = row.getLong(0)
        val vec = row.getAs[DenseVector](1)
        id -> vec
      }
      .toMap

    def cosineSimilarity(v1: DenseVector, v2: DenseVector): Double = {
      val a = v1.values
      val b = v2.values
      var dot = 0.0
      var norm1 = 0.0
      var norm2 = 0.0
      var i = 0
      while (i < a.length) {
        dot += a(i) * b(i)
        norm1 += a(i) * a(i)
        norm2 += b(i) * b(i)
        i += 1
      }
      dot / (math.sqrt(norm1) * math.sqrt(norm2))
    }

    val sim12 = cosineSimilarity(embMap(1L), embMap(2L))
    val sim13 = cosineSimilarity(embMap(1L), embMap(3L))
    val sim34 = cosineSimilarity(embMap(3L), embMap(4L))
    val sim16 = cosineSimilarity(embMap(1L), embMap(6L))
    val sim56 = cosineSimilarity(embMap(5L), embMap(6L))

    // 1-2 co-occur very often
    assert(sim12 > 0.3, s"1-2 similarity $sim12 should be > 0.3")
    // 1-3 appear together less often than 1-2
    assert(sim13 < sim12, s"1-3 ($sim13) should be < 1-2 ($sim12)")
    // 3-4 are exclusive pair
    assert(sim34 > 0.2, s"3-4 similarity $sim34 should be > 0.2")
    // 1 and 6 almost never together
    assert(sim16 < 0.1, s"1-6 similarity $sim16 should be near zero")
    // 5-6 co-occur in a sequence
    assert(sim56 > sim16, s"5-6 ($sim56) should be > 1-6 ($sim16)")

    // Self similarity
    val self = cosineSimilarity(embMap(1L), embMap(1L))
    assert(math.abs(self - 1.0) < 1e-6, s"self-similarity should be ~1.0, got $self")
  }
}
