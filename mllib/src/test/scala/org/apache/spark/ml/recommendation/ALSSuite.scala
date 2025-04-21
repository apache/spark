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

package org.apache.spark.ml.recommendation

import java.io.File
import java.util.Random

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter

import org.apache.spark._
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{RMSE, TEST_SIZE, TRAINING_SIZE}
import org.apache.spark.ml.linalg.{BLAS, Vectors}
import org.apache.spark.ml.recommendation.ALS._
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.types._
import org.apache.spark.storage.{StorageLevel, StorageLevelMapper}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

class ALSSuite extends MLTest with DefaultReadWriteTest with Logging {

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc.setCheckpointDir(tempDir.getAbsolutePath)
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("LocalIndexEncoder") {
    val random = new Random
    for (numBlocks <- Seq(1, 2, 5, 10, 20, 50, 100)) {
      val encoder = new LocalIndexEncoder(numBlocks)
      val maxLocalIndex = Int.MaxValue / numBlocks
      val tests = Seq.fill(5)((random.nextInt(numBlocks), random.nextInt(maxLocalIndex))) ++
        Seq((0, 0), (numBlocks - 1, maxLocalIndex))
      tests.foreach { case (blockId, localIndex) =>
        val err = s"Failed with numBlocks=$numBlocks, blockId=$blockId, and localIndex=$localIndex."
        val encoded = encoder.encode(blockId, localIndex)
        assert(encoder.blockId(encoded) === blockId, err)
        assert(encoder.localIndex(encoded) === localIndex, err)
      }
    }
  }

  test("normal equation construction") {
    val k = 2
    val ne0 = new NormalEquation(k)
      .add(Array(1.0f, 2.0f), 3.0)
      .add(Array(4.0f, 5.0f), 12.0, 2.0) // weighted
    assert(ne0.k === k)
    assert(ne0.triK === k * (k + 1) / 2)
    // NumPy code that computes the expected values:
    // A = np.matrix("1 2; 4 5")
    // b = np.matrix("3; 6")
    // C = np.matrix(np.diag([1, 2]))
    // ata = A.transpose() * C * A
    // atb = A.transpose() * C * b
    assert(Vectors.dense(ne0.ata) ~== Vectors.dense(33.0, 42.0, 54.0) relTol 1e-8)
    assert(Vectors.dense(ne0.atb) ~== Vectors.dense(51.0, 66.0) relTol 1e-8)

    val ne1 = new NormalEquation(2)
      .add(Array(7.0f, 8.0f), 9.0)
    ne0.merge(ne1)
    // NumPy code that computes the expected values:
    // A = np.matrix("1 2; 4 5; 7 8")
    // b = np.matrix("3; 6; 9")
    // C = np.matrix(np.diag([1, 2, 1]))
    // ata = A.transpose() * C * A
    // atb = A.transpose() * C * b
    assert(Vectors.dense(ne0.ata) ~== Vectors.dense(82.0, 98.0, 118.0) relTol 1e-8)
    assert(Vectors.dense(ne0.atb) ~== Vectors.dense(114.0, 138.0) relTol 1e-8)

    intercept[IllegalArgumentException] {
      ne0.add(Array(1.0f), 2.0)
    }
    intercept[IllegalArgumentException] {
      ne0.add(Array(1.0f, 2.0f, 3.0f), 4.0)
    }
    intercept[IllegalArgumentException] {
      ne0.add(Array(1.0f, 2.0f), 0.0, -1.0)
    }
    intercept[IllegalArgumentException] {
      val ne2 = new NormalEquation(3)
      ne0.merge(ne2)
    }

    ne0.reset()
    assert(ne0.ata.forall(_ == 0.0))
    assert(ne0.atb.forall(_ == 0.0))
  }

  test("CholeskySolver") {
    val k = 2
    val ne0 = new NormalEquation(k)
      .add(Array(1.0f, 2.0f), 4.0)
      .add(Array(1.0f, 3.0f), 9.0)
      .add(Array(1.0f, 4.0f), 16.0)
    val ne1 = new NormalEquation(k)
      .merge(ne0)

    val chol = new CholeskySolver
    val x0 = chol.solve(ne0, 0.0).map(_.toDouble)
    // NumPy code that computes the expected solution:
    // A = np.matrix("1 2; 1 3; 1 4")
    // b = b = np.matrix("3; 6")
    // x0 = np.linalg.lstsq(A, b)[0]
    assert(Vectors.dense(x0) ~== Vectors.dense(-8.333333, 6.0) relTol 1e-6)

    assert(ne0.ata.forall(_ == 0.0))
    assert(ne0.atb.forall(_ == 0.0))

    val x1 = chol.solve(ne1, 1.5).map(_.toDouble)
    // NumPy code that computes the expected solution, where lambda is scaled by n:
    // x0 = np.linalg.solve(A.transpose() * A + 1.5 * np.eye(2), A.transpose() * b)
    assert(Vectors.dense(x1) ~== Vectors.dense(-0.1155556, 3.28) relTol 1e-6)
  }

  test("RatingBlockBuilder") {
    val emptyBuilder = new RatingBlockBuilder[Int]()
    assert(emptyBuilder.size === 0)
    val emptyBlock = emptyBuilder.build()
    assert(emptyBlock.srcIds.isEmpty)
    assert(emptyBlock.dstIds.isEmpty)
    assert(emptyBlock.ratings.isEmpty)

    val builder0 = new RatingBlockBuilder()
      .add(Rating(0, 1, 2.0f))
      .add(Rating(3, 4, 5.0f))
    assert(builder0.size === 2)
    val builder1 = new RatingBlockBuilder()
      .add(Rating(6, 7, 8.0f))
      .merge(builder0.build())
    assert(builder1.size === 3)
    val block = builder1.build()
    val ratings = Seq.tabulate(block.size) { i =>
      (block.srcIds(i), block.dstIds(i), block.ratings(i))
    }.toSet
    assert(ratings === Set((0, 1, 2.0f), (3, 4, 5.0f), (6, 7, 8.0f)))
  }

  test("UncompressedInBlock") {
    val encoder = new LocalIndexEncoder(10)
    val uncompressed = new UncompressedInBlockBuilder[Int](encoder)
      .add(0, Array(1, 0, 2), Array(0, 1, 4), Array(1.0f, 2.0f, 3.0f))
      .add(1, Array(3, 0), Array(2, 5), Array(4.0f, 5.0f))
      .build()
    assert(uncompressed.length === 5)
    val records = Seq.tabulate(uncompressed.length) { i =>
      val dstEncodedIndex = uncompressed.dstEncodedIndices(i)
      val dstBlockId = encoder.blockId(dstEncodedIndex)
      val dstLocalIndex = encoder.localIndex(dstEncodedIndex)
      (uncompressed.srcIds(i), dstBlockId, dstLocalIndex, uncompressed.ratings(i))
    }.toSet
    val expected =
      Set((1, 0, 0, 1.0f), (0, 0, 1, 2.0f), (2, 0, 4, 3.0f), (3, 1, 2, 4.0f), (0, 1, 5, 5.0f))
    assert(records === expected)

    val compressed = uncompressed.compress()
    assert(compressed.size === 5)
    assert(compressed.srcIds.toSeq === Seq(0, 1, 2, 3))
    assert(compressed.dstPtrs.toSeq === Seq(0, 2, 3, 4, 5))
    val decompressed = ArrayBuffer.empty[(Int, Int, Int, Float)]
    var i = 0
    while (i < compressed.srcIds.length) {
      var j = compressed.dstPtrs(i)
      while (j < compressed.dstPtrs(i + 1)) {
        val dstEncodedIndex = compressed.dstEncodedIndices(j)
        val dstBlockId = encoder.blockId(dstEncodedIndex)
        val dstLocalIndex = encoder.localIndex(dstEncodedIndex)
        decompressed += ((compressed.srcIds(i), dstBlockId, dstLocalIndex, compressed.ratings(j)))
        j += 1
      }
      i += 1
    }
    assert(decompressed.toSet === expected)
  }

  test("ALS validate input dataset") {
    import testImplicits._

    withClue("Valid Integer Ids") {
      val df = sc.parallelize(Seq(
        (123, 1, 0.5),
        (111, 2, 1.0)
      )).toDF("item", "user", "rating")
      new ALS().setMaxIter(1).fit(df)
    }

    withClue("Valid Long Ids") {
      val df = sc.parallelize(Seq(
        (1231L, 12L, 0.5),
        (1112L, 21L, 1.0)
      )).toDF("item", "user", "rating")
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        new ALS().setMaxIter(1).fit(df)
      }
    }

    withClue("Valid Double Ids") {
      val df = sc.parallelize(Seq(
        (123.0, 12.0, 0.5),
        (111.0, 21.0, 1.0)
      )).toDF("item", "user", "rating")
      new ALS().setMaxIter(1).fit(df)
    }

    withClue("Valid Decimal Ids") {
      val df = sc.parallelize(Seq(
        (1231L, 12L, 0.5),
        (1112L, 21L, 1.0)
      )).toDF("item", "user", "rating")
        .select(
          col("item").cast(DecimalType(15, 2)).as("item"),
          col("user").cast(DecimalType(15, 2)).as("user"),
          col("rating")
        )
      new ALS().setMaxIter(1).fit(df)
    }

    val msg = "ALS only supports non-Null values"
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      withClue("Invalid Long: out of range") {
        val df = sc.parallelize(Seq(
          (1231000000000L, 12L, 0.5),
          (1112L, 21L, 1.0)
        )).toDF("item", "user", "rating")
        val e = intercept[Exception] { new ALS().setMaxIter(1).fit(df) }
        assert(e.getMessage.contains(msg))
      }

      withClue("Invalid Double: out of range") {
        val df = sc.parallelize(Seq(
          (1231000000000.0, 12.0, 0.5),
          (111.0, 21.0, 1.0)
        )).toDF("item", "user", "rating")
        val e = intercept[Exception] { new ALS().setMaxIter(1).fit(df) }
        assert(e.getMessage.contains(msg))
      }
    }

    withClue("Invalid Double: fractional part") {
      val df = sc.parallelize(Seq(
        (123.1, 12.0, 0.5),
        (111.0, 21.0, 1.0)
      )).toDF("item", "user", "rating")
      val e = intercept[Exception] { new ALS().setMaxIter(1).fit(df) }
      assert(e.getMessage.contains(msg))
    }

    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      withClue("Invalid Decimal: out of range") {
        val df = sc.parallelize(Seq(
          (1231000000000.0, 12L, 0.5),
          (1112.0, 21L, 1.0)
        )).toDF("item", "user", "rating")
          .select(
            col("item").cast(DecimalType(15, 2)).as("item"),
            col("user").cast(DecimalType(15, 2)).as("user"),
            col("rating")
          )
        val e = intercept[Exception] { new ALS().setMaxIter(1).fit(df) }
        assert(e.getMessage.contains(msg))
      }
    }

    withClue("Invalid Decimal: fractional part") {
      val df = sc.parallelize(Seq(
        (123.1, 12L, 0.5),
        (1112.0, 21L, 1.0)
      )).toDF("item", "user", "rating")
        .select(
          col("item").cast(DecimalType(15, 2)).as("item"),
          col("user").cast(DecimalType(15, 2)).as("user"),
          col("rating")
        )
      val e = intercept[Exception] { new ALS().setMaxIter(1).fit(df) }
      assert(e.getMessage.contains(msg))
    }

    withClue("Invalid Type") {
      val df = sc.parallelize(Seq(
        ("123.0", 12.0, 0.5),
        ("111", 21.0, 1.0)
      )).toDF("item", "user", "rating")
      val e = intercept[Exception] { new ALS().setMaxIter(1).fit(df) }
      assert(e.getMessage.contains("Column item must be of type numeric"))
    }
  }

  /**
   * Generates an explicit feedback dataset for testing ALS.
   * @param numUsers number of users
   * @param numItems number of items
   * @param rank rank
   * @param noiseStd the standard deviation of additive Gaussian noise on training data
   * @param seed random seed
   * @return (training, test)
   */
  def genExplicitTestData(
      numUsers: Int,
      numItems: Int,
      rank: Int,
      noiseStd: Double = 0.0,
      seed: Long = 11L): (RDD[Rating[Int]], RDD[Rating[Int]]) = {
    val trainingFraction = 0.6
    val testFraction = 0.3
    val totalFraction = trainingFraction + testFraction
    val random = new Random(seed)
    val userFactors = genFactors(numUsers, rank, random)
    val itemFactors = genFactors(numItems, rank, random)
    val training = ArrayBuffer.empty[Rating[Int]]
    val test = ArrayBuffer.empty[Rating[Int]]
    for ((userId, userFactor) <- userFactors; (itemId, itemFactor) <- itemFactors) {
      val x = random.nextDouble()
      if (x < totalFraction) {
        val rating = BLAS.nativeBLAS.sdot(rank, userFactor, 1, itemFactor, 1)
        if (x < trainingFraction) {
          val noise = noiseStd * random.nextGaussian()
          training += Rating(userId, itemId, rating + noise.toFloat)
        } else {
          test += Rating(userId, itemId, rating)
        }
      }
    }
    logInfo(log"Generated an explicit feedback dataset with ${MDC(TRAINING_SIZE, training.size)} " +
      log"ratings for training and ${MDC(TEST_SIZE, test.size)} for test.")
    (sc.parallelize(training.toSeq, 2), sc.parallelize(test.toSeq, 2))
  }

  /**
   * Generates an implicit feedback dataset for testing ALS.
   * @param numUsers number of users
   * @param numItems number of items
   * @param rank rank
   * @param noiseStd the standard deviation of additive Gaussian noise on training data
   * @param seed random seed
   * @return (training, test)
   */
  def genImplicitTestData(
      numUsers: Int,
      numItems: Int,
      rank: Int,
      noiseStd: Double = 0.0,
      seed: Long = 11L): (RDD[Rating[Int]], RDD[Rating[Int]]) = {
    ALSSuite.genImplicitTestData(sc, numUsers, numItems, rank, noiseStd, seed)
  }

  /**
   * Generates random user/item factors, with i.i.d. values drawn from U(a, b).
   * @param size number of users/items
   * @param rank number of features
   * @param random random number generator
   * @param a min value of the support (default: -1)
   * @param b max value of the support (default: 1)
   * @return a sequence of (ID, factors) pairs
   */
  private def genFactors(
      size: Int,
      rank: Int,
      random: Random,
      a: Float = -1.0f,
      b: Float = 1.0f): Seq[(Int, Array[Float])] = {
    ALSSuite.genFactors(size, rank, random, a, b)
  }

  /**
  * Train ALS using the given training set and parameters
  * @param training training dataset
  * @param rank rank of the matrix factorization
  * @param maxIter max number of iterations
  * @param regParam regularization constant
  * @param implicitPrefs whether to use implicit preference
  * @param numUserBlocks number of user blocks
  * @param numItemBlocks number of item blocks
  * @return a trained ALSModel
  */
  def trainALS(
    training: RDD[Rating[Int]],
    rank: Int,
    maxIter: Int,
    regParam: Double,
    implicitPrefs: Boolean = false,
    numUserBlocks: Int = 2,
    numItemBlocks: Int = 3): ALSModel = {
    val spark = this.spark
    import spark.implicits._
    val als = new ALS()
      .setRank(rank)
      .setRegParam(regParam)
      .setImplicitPrefs(implicitPrefs)
      .setNumUserBlocks(numUserBlocks)
      .setNumItemBlocks(numItemBlocks)
      .setSeed(0)
    als.fit(training.toDF())
  }

  /**
   * Test ALS using the given training/test splits and parameters.
   * @param training training dataset
   * @param test test dataset
   * @param rank rank of the matrix factorization
   * @param maxIter max number of iterations
   * @param regParam regularization constant
   * @param implicitPrefs whether to use implicit preference
   * @param numUserBlocks number of user blocks
   * @param numItemBlocks number of item blocks
   * @param targetRMSE target test RMSE
   */
  def testALS(
      training: RDD[Rating[Int]],
      test: RDD[Rating[Int]],
      rank: Int,
      maxIter: Int,
      regParam: Double,
      implicitPrefs: Boolean = false,
      numUserBlocks: Int = 2,
      numItemBlocks: Int = 3,
      targetRMSE: Double = 0.05): Unit = {
    val spark = this.spark
    import spark.implicits._
    val als = new ALS()
      .setRank(rank)
      .setRegParam(regParam)
      .setImplicitPrefs(implicitPrefs)
      .setNumUserBlocks(numUserBlocks)
      .setNumItemBlocks(numItemBlocks)
      .setSeed(0)
    val alpha = als.getAlpha
    val model = als.fit(training.toDF())
    testTransformerByGlobalCheckFunc[Rating[Int]](test.toDF(), model, "rating", "prediction") {
        case rows: Seq[Row] =>
          val predictions = rows.map(row => (row.getFloat(0).toDouble, row.getFloat(1).toDouble))

          val rmse =
            if (implicitPrefs) {
              // TODO: Use a better (rank-based?) evaluation metric for implicit feedback.
              // We limit the ratings and the predictions to interval [0, 1] and compute the
              // weighted RMSE with the confidence scores as weights.
              val (totalWeight, weightedSumSq) = predictions.map { case (rating, prediction) =>
                val confidence = 1.0 + alpha * math.abs(rating)
                val rating01 = math.max(math.min(rating, 1.0), 0.0)
                val prediction01 = math.max(math.min(prediction, 1.0), 0.0)
                val err = prediction01 - rating01
                (confidence, confidence * err * err)
              }.reduce[(Double, Double)] { case ((c0, e0), (c1, e1)) =>
                (c0 + c1, e0 + e1)
              }
              math.sqrt(weightedSumSq / totalWeight)
            } else {
              val errorSquares = predictions.map { case (rating, prediction) =>
                val err = rating - prediction
                err * err
              }
              val mse = errorSquares.sum / errorSquares.length
              math.sqrt(mse)
            }
          logInfo(log"Test RMSE is ${MDC(RMSE, rmse)}.")
          assert(rmse < targetRMSE)
    }

    MLTestingUtils.checkCopyAndUids(als, model)
  }

  test("exact rank-1 matrix") {
    val (training, test) = genExplicitTestData(numUsers = 20, numItems = 40, rank = 1)
    testALS(training, test, maxIter = 1, rank = 1, regParam = 1e-5, targetRMSE = 0.001)
    testALS(training, test, maxIter = 1, rank = 2, regParam = 1e-5, targetRMSE = 0.001)
  }

  test("approximate rank-1 matrix") {
    val (training, test) =
      genExplicitTestData(numUsers = 20, numItems = 40, rank = 1, noiseStd = 0.01)
    testALS(training, test, maxIter = 2, rank = 1, regParam = 0.01, targetRMSE = 0.02)
    testALS(training, test, maxIter = 2, rank = 2, regParam = 0.01, targetRMSE = 0.02)
  }

  test("approximate rank-2 matrix") {
    val (training, test) =
      genExplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
    testALS(training, test, maxIter = 4, rank = 2, regParam = 0.01, targetRMSE = 0.03)
    testALS(training, test, maxIter = 4, rank = 3, regParam = 0.01, targetRMSE = 0.03)
  }

  test("different block settings") {
    val (training, test) =
      genExplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
    for ((numUserBlocks, numItemBlocks) <- Seq((1, 1), (1, 2), (2, 1), (2, 2))) {
      testALS(training, test, maxIter = 4, rank = 3, regParam = 0.01, targetRMSE = 0.03,
        numUserBlocks = numUserBlocks, numItemBlocks = numItemBlocks)
    }
  }

  test("more blocks than ratings") {
    val (training, test) =
      genExplicitTestData(numUsers = 4, numItems = 4, rank = 1)
    testALS(training, test, maxIter = 2, rank = 1, regParam = 1e-4, targetRMSE = 0.002,
     numItemBlocks = 5, numUserBlocks = 5)
  }

  test("implicit feedback") {
    val (training, test) =
      genImplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
    testALS(training, test, maxIter = 4, rank = 2, regParam = 0.01, implicitPrefs = true,
      targetRMSE = 0.3)
  }

  test("implicit feedback regression") {
    val trainingWithNeg = sc.parallelize(Seq(Rating(0, 0, 1), Rating(1, 1, 1), Rating(0, 1, -3)))
    val trainingWithZero = sc.parallelize(Seq(Rating(0, 0, 1), Rating(1, 1, 1), Rating(0, 1, 0)))
    val modelWithNeg =
      trainALS(trainingWithNeg, rank = 1, maxIter = 5, regParam = 0.01, implicitPrefs = true)
    val modelWithZero =
      trainALS(trainingWithZero, rank = 1, maxIter = 5, regParam = 0.01, implicitPrefs = true)
    val userFactorsNeg = modelWithNeg.userFactors
    val itemFactorsNeg = modelWithNeg.itemFactors
    val userFactorsZero = modelWithZero.userFactors
    val itemFactorsZero = modelWithZero.itemFactors
    assert(userFactorsNeg.intersect(userFactorsZero).count() == 0)
    assert(itemFactorsNeg.intersect(itemFactorsZero).count() == 0)
  }
  test("using generic ID types") {
    val (ratings, _) = genImplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)

    val longRatings = ratings.map(r => Rating(r.user.toLong, r.item.toLong, r.rating))
    val (longUserFactors, _) = ALS.train(longRatings, rank = 2, maxIter = 4, seed = 0)
    assert(longUserFactors.first()._1.getClass === classOf[Long])

    val strRatings = ratings.map(r => Rating(r.user.toString, r.item.toString, r.rating))
    val (strUserFactors, _) = ALS.train(strRatings, rank = 2, maxIter = 4, seed = 0)
    assert(strUserFactors.first()._1.getClass === classOf[String])
  }

  test("nonnegative constraint") {
    val (ratings, _) = genImplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
    val (userFactors, itemFactors) =
      ALS.train(ratings, rank = 2, maxIter = 4, nonnegative = true, seed = 0)
    def isNonnegative(factors: RDD[(Int, Array[Float])]): Boolean = {
      factors.values.map { _.forall(_ >= 0.0) }.reduce(_ && _)
    }
    assert(isNonnegative(userFactors))
    assert(isNonnegative(itemFactors))
    // TODO: Validate the solution.
  }

  test("als partitioner is a projection") {
    for (p <- Seq(1, 10, 100, 1000)) {
      val part = new ALSPartitioner(p)
      var k = 0
      while (k < p) {
        assert(k === part.getPartition(k))
        assert(k === part.getPartition(k.toLong))
        k += 1
      }
    }
  }

  test("partitioner in returned factors") {
    val (ratings, _) = genImplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
    val (userFactors, itemFactors) = ALS.train(
      ratings, rank = 2, maxIter = 4, numUserBlocks = 3, numItemBlocks = 4, seed = 0)
    for ((tpe, factors) <- Seq(("User", userFactors), ("Item", itemFactors))) {
      assert(userFactors.partitioner.isDefined, s"$tpe factors should have partitioner.")
      val part = userFactors.partitioner.get
      userFactors.mapPartitionsWithIndex { (idx, items) =>
        items.foreach { case (id, _) =>
          if (part.getPartition(id) != idx) {
            throw new SparkException(s"$tpe with ID $id should not be in partition $idx.")
          }
        }
        Iterator.empty
      }.count()
    }
  }

  test("als with large number of iterations") {
    val (ratings, _) = genExplicitTestData(numUsers = 4, numItems = 4, rank = 1)
    ALS.train(ratings, rank = 1, maxIter = 50, numUserBlocks = 2, numItemBlocks = 2, seed = 0)
    ALS.train(ratings, rank = 1, maxIter = 50, numUserBlocks = 2, numItemBlocks = 2,
      implicitPrefs = true, seed = 0)
  }

  test("read/write") {
    val spark = this.spark
    import spark.implicits._
    import ALSSuite._
    val (ratings, _) = genExplicitTestData(numUsers = 4, numItems = 4, rank = 1)

    def getFactors(df: DataFrame): Set[(Int, Array[Float])] = {
      df.select("id", "features").collect().map { case r =>
        (r.getInt(0), r.getAs[Array[Float]](1))
      }.toSet
    }

    def checkModelData(model: ALSModel, model2: ALSModel): Unit = {
      assert(model.rank === model2.rank)
      assert(getFactors(model.userFactors) === getFactors(model2.userFactors))
      assert(getFactors(model.itemFactors) === getFactors(model2.itemFactors))
    }

    val als = new ALS()
    testEstimatorAndModelReadWrite(als, ratings.toDF(), allEstimatorParamSettings,
      allModelParamSettings, checkModelData)
  }

  private def checkNumericTypesALS(
      estimator: ALS,
      spark: SparkSession,
      column: String,
      baseType: NumericType)
      (check: (ALSModel, ALSModel) => Unit)
      (check2: (ALSModel, ALSModel, DataFrame, Encoder[_]) => Unit): Unit = {
    val dfs = genRatingsDFWithNumericCols(spark, column)
    val maybeDf = dfs.find { case (numericTypeWithEncoder, _) =>
      numericTypeWithEncoder.numericType == baseType
    }
    assert(maybeDf.isDefined)
    val df = maybeDf.get._2

    val expected = estimator.fit(df)
    val actuals = dfs.map(t => (t, estimator.fit(t._2)))
    actuals.foreach { case (_, actual) => check(expected, actual) }
    actuals.foreach { case (t, actual) => check2(expected, actual, t._2, t._1.encoder) }

    val baseDF = dfs.find(_._1.numericType == baseType).get._2
    val others = baseDF.columns.toSeq.diff(Seq(column)).map(col)
    val cols = Seq(col(column).cast(StringType)) ++ others
    val strDF = baseDF.select(cols: _*)
    val thrown = intercept[IllegalArgumentException] {
      estimator.fit(strDF)
    }
    assert(thrown.getMessage.contains(
      s"$column must be of type numeric but was actually of type string"))
  }

  private class NumericTypeWithEncoder[A](val numericType: NumericType)
      (implicit val encoder: Encoder[(A, Int, Double)])

  private def genRatingsDFWithNumericCols(
      spark: SparkSession,
      column: String) = {

    import testImplicits._

    val df = spark.createDataFrame(Seq(
      (0, 10, 1.0),
      (1, 20, 2.0),
      (2, 30, 3.0),
      (3, 40, 4.0),
      (4, 50, 5.0)
    )).toDF("user", "item", "rating")

    val others = df.columns.toSeq.diff(Seq(column)).map(col)
    val types =
      Seq(new NumericTypeWithEncoder[Short](ShortType),
        new NumericTypeWithEncoder[Long](LongType),
        new NumericTypeWithEncoder[Int](IntegerType),
        new NumericTypeWithEncoder[Float](FloatType),
        new NumericTypeWithEncoder[Byte](ByteType),
        new NumericTypeWithEncoder[Double](DoubleType),
        new NumericTypeWithEncoder[Decimal](DecimalType(10, 0))(ExpressionEncoder())
      )
    types.map { t =>
      val cols = Seq(col(column).cast(t.numericType)) ++ others
      t -> df.select(cols: _*)
    }
  }

  test("input type validation") {
    val spark = this.spark
    import spark.implicits._

    // check that ALS can handle all numeric types for rating column
    // and user/item columns (when the user/item ids are within Int range)
    val als = new ALS().setMaxIter(1).setRank(1)
    Seq(("user", IntegerType), ("item", IntegerType), ("rating", FloatType)).foreach {
      case (colName, sqlType) =>
        checkNumericTypesALS(als, spark, colName, sqlType) {
          (ex, act) =>
            ex.userFactors.first().getSeq[Float](1) === act.userFactors.first().getSeq[Float](1)
        } { (ex, act, df, enc) =>
          // With AQE on/off, the order of result may be different. Here sortby the result.
          val expected = ex.transform(df).selectExpr("prediction")
            .sort("prediction").first().getFloat(0)
          testTransformerByGlobalCheckFunc(df, act, "prediction") {
            case rows: Seq[Row] =>
              expected ~== rows.sortBy(_.getFloat(0)).head.getFloat(0) absTol 1e-6
          }(enc)
        }
    }
    // check user/item ids falling outside of Int range
    val big = Int.MaxValue.toLong + 1
    val small = Int.MinValue.toDouble - 1
    val df = Seq(
      (0, 0L, 0d, 1, 1L, 1d, 3.0),
      (0, big, small, 0, big, small, 2.0),
      (1, 1L, 1d, 0, 0L, 0d, 5.0)
    ).toDF("user", "user_big", "user_small", "item", "item_big", "item_small", "rating")
    val msg = "ALS only supports non-Null values"
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      withClue("fit should fail when ids exceed integer range. ") {
        assert(intercept[Exception] {
          als.fit(df.select(df("user_big").as("user"), df("item"), df("rating")))
        }.getMessage.contains(msg))
        assert(intercept[Exception] {
          als.fit(df.select(df("user_small").as("user"), df("item"), df("rating")))
        }.getMessage.contains(msg))
        assert(intercept[Exception] {
          als.fit(df.select(df("item_big").as("item"), df("user"), df("rating")))
        }.getMessage.contains(msg))
        assert(intercept[Exception] {
          als.fit(df.select(df("item_small").as("item"), df("user"), df("rating")))
        }.getMessage.contains(msg))
      }
      withClue("transform should fail when ids exceed integer range. ") {
        val model = als.fit(df)
        def testTransformIdExceedsIntRange[A : Encoder](dataFrame: DataFrame): Unit = {
          val e1 = intercept[Exception] {
            model.transform(dataFrame).collect()
          }
          TestUtils.assertExceptionMsg(e1, msg)
          val e2 = intercept[StreamingQueryException] {
            testTransformer[A](dataFrame, model, "prediction") { _ => }
          }
          TestUtils.assertExceptionMsg(e2, msg)
        }
        testTransformIdExceedsIntRange[(Long, Int)](df.select(df("user_big").as("user"),
          df("item")))
        testTransformIdExceedsIntRange[(Double, Int)](df.select(df("user_small").as("user"),
          df("item")))
        testTransformIdExceedsIntRange[(Long, Int)](df.select(df("item_big").as("item"),
          df("user")))
        testTransformIdExceedsIntRange[(Double, Int)](df.select(df("item_small").as("item"),
          df("user")))
      }
    }
  }

  test("SPARK-18268: ALS with empty RDD should fail with better message") {
    val ratings = sc.parallelize(Array.empty[Rating[Int]].toImmutableArraySeq)
    intercept[IllegalArgumentException] {
      ALS.train(ratings)
    }
  }

  test("ALS cold start user/item prediction strategy") {
    val spark = this.spark
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val (ratings, _) = genExplicitTestData(numUsers = 4, numItems = 4, rank = 1)
    val data = ratings.toDF()
    val knownUser = data.select(max("user")).as[Int].first()
    val unknownUser = knownUser + 10
    val knownItem = data.select(max("item")).as[Int].first()
    val unknownItem = knownItem + 20
    val test = Seq(
      (unknownUser, unknownItem, true),
      (knownUser, unknownItem, true),
      (unknownUser, knownItem, true),
      (knownUser, knownItem, false)
    ).toDF("user", "item", "expectedIsNaN")

    val als = new ALS().setMaxIter(1).setRank(1)
    // default is 'nan'
    val defaultModel = als.fit(data)
    testTransformer[(Int, Int, Boolean)](test, defaultModel, "expectedIsNaN", "prediction") {
      case Row(expectedIsNaN: Boolean, prediction: Float) =>
        assert(prediction.isNaN === expectedIsNaN)
    }

    // check 'drop' strategy should filter out rows with unknown users/items
    val defaultPrediction = defaultModel.transform(test).select("prediction")
      .as[Float].filter(!_.isNaN).first()
    testTransformerByGlobalCheckFunc[(Int, Int, Boolean)](test,
      defaultModel.setColdStartStrategy("drop"), "prediction") {
      case rows: Seq[Row] =>
        val dropPredictions = rows.map(_.getFloat(0))
        assert(dropPredictions.length == 1)
        assert(!dropPredictions.head.isNaN)
        assert(dropPredictions.head ~== defaultPrediction relTol 1e-14)
    }
  }

  test("case insensitive cold start param value") {
    val spark = this.spark
    import spark.implicits._
    val (ratings, _) = genExplicitTestData(numUsers = 2, numItems = 2, rank = 1)
    val data = ratings.toDF()
    val model = new ALS().fit(data)
    Seq("nan", "NaN", "Nan", "drop", "DROP", "Drop").foreach { s =>
      testTransformer[Rating[Int]](data, model.setColdStartStrategy(s), "prediction") { _ => }
    }
  }

  private def getALSModel = {
    val spark = this.spark
    import spark.implicits._

    val userFactors = Seq(
      (0, Array(6.0f, 4.0f)),
      (1, Array(3.0f, 4.0f)),
      (2, Array(3.0f, 6.0f))
    ).toDF("id", "features")
    val itemFactors = Seq(
      (3, Array(5.0f, 6.0f)),
      (4, Array(6.0f, 2.0f)),
      (5, Array(3.0f, 6.0f)),
      (6, Array(4.0f, 1.0f))
    ).toDF("id", "features")
    val als = new ALS().setRank(2)
    new ALSModel(als.uid, als.getRank, userFactors, itemFactors)
      .setUserCol("user")
      .setItemCol("item")
  }

  test("recommendForAllUsers with k <, = and > num_items") {
    val model = getALSModel
    val numUsers = model.userFactors.count()
    val numItems = model.itemFactors.count()
    val expected = Map(
      0 -> Seq((3, 54f), (4, 44f), (5, 42f), (6, 28f)),
      1 -> Seq((3, 39f), (5, 33f), (4, 26f), (6, 16f)),
      2 -> Seq((3, 51f), (5, 45f), (4, 30f), (6, 18f))
    )

    Seq(2, 4, 6).foreach { k =>
      val n = math.min(k, numItems).toInt
      val expectedUpToN = expected.transform((_, v) => v.slice(0, n))
      val topItems = model.recommendForAllUsers(k)
      assert(topItems.count() == numUsers)
      assert(topItems.columns.contains("user"))
      checkRecommendations(topItems, expectedUpToN.toMap, "item")
    }
  }

  test("recommendForAllItems with k <, = and > num_users") {
    val model = getALSModel
    val numUsers = model.userFactors.count()
    val numItems = model.itemFactors.count()
    val expected = Map(
      3 -> Seq((0, 54f), (2, 51f), (1, 39f)),
      4 -> Seq((0, 44f), (2, 30f), (1, 26f)),
      5 -> Seq((2, 45f), (0, 42f), (1, 33f)),
      6 -> Seq((0, 28f), (2, 18f), (1, 16f))
    )

    Seq(2, 3, 4).foreach { k =>
      val n = math.min(k, numUsers).toInt
      val expectedUpToN = expected.transform((_, v) => v.slice(0, n))
      val topUsers = getALSModel.recommendForAllItems(k)
      assert(topUsers.count() == numItems)
      assert(topUsers.columns.contains("item"))
      checkRecommendations(topUsers, expectedUpToN.toMap, "user")
    }
  }

  test("recommendForUserSubset with k <, = and > num_items") {
    val spark = this.spark
    import spark.implicits._
    val model = getALSModel
    val numItems = model.itemFactors.count()
    val expected = Map(
      0 -> Seq((3, 54f), (4, 44f), (5, 42f), (6, 28f)),
      2 -> Seq((3, 51f), (5, 45f), (4, 30f), (6, 18f))
    )
    val userSubset = expected.keys.toSeq.toDF("user")
    val numUsersSubset = userSubset.count()

    Seq(2, 4, 6).foreach { k =>
      val n = math.min(k, numItems).toInt
      val expectedUpToN = expected.transform((_, v) => v.slice(0, n))
      val topItems = model.recommendForUserSubset(userSubset, k)
      assert(topItems.count() == numUsersSubset)
      assert(topItems.columns.contains("user"))
      checkRecommendations(topItems, expectedUpToN.toMap, "item")
    }
  }

  test("recommendForItemSubset with k <, = and > num_users") {
    val spark = this.spark
    import spark.implicits._
    val model = getALSModel
    val numUsers = model.userFactors.count()
    val expected = Map(
      3 -> Seq((0, 54f), (2, 51f), (1, 39f)),
      6 -> Seq((0, 28f), (2, 18f), (1, 16f))
    )
    val itemSubset = expected.keys.toSeq.toDF("item")
    val numItemsSubset = itemSubset.count()

    Seq(2, 3, 4).foreach { k =>
      val n = math.min(k, numUsers).toInt
      val expectedUpToN = expected.transform((_, v) => v.slice(0, n))
      val topUsers = model.recommendForItemSubset(itemSubset, k)
      assert(topUsers.count() == numItemsSubset)
      assert(topUsers.columns.contains("item"))
      checkRecommendations(topUsers, expectedUpToN.toMap, "user")
    }
  }

  test("subset recommendations eliminate duplicate ids, returns same results as unique ids") {
    val spark = this.spark
    import spark.implicits._
    val model = getALSModel
    val k = 2

    val users = Seq(0, 1).toDF("user")
    val dupUsers = Seq(0, 1, 0, 1).toDF("user")
    val singleUserRecs = model.recommendForUserSubset(users, k)
    val dupUserRecs = model.recommendForUserSubset(dupUsers, k)
      .as[(Int, Seq[(Int, Float)])].collect().toMap
    assert(singleUserRecs.count() == dupUserRecs.size)
    checkRecommendations(singleUserRecs, dupUserRecs, "item")

    val items = Seq(3, 4, 5).toDF("item")
    val dupItems = Seq(3, 4, 5, 4, 5).toDF("item")
    val singleItemRecs = model.recommendForItemSubset(items, k)
    val dupItemRecs = model.recommendForItemSubset(dupItems, k)
      .as[(Int, Seq[(Int, Float)])].collect().toMap
    assert(singleItemRecs.count() == dupItemRecs.size)
    checkRecommendations(singleItemRecs, dupItemRecs, "user")
  }

  test("subset recommendations on full input dataset equivalent to recommendForAll") {
    val spark = this.spark
    import spark.implicits._
    val model = getALSModel
    val k = 2

    val userSubset = model.userFactors.withColumnRenamed("id", "user").drop("features")
    val userSubsetRecs = model.recommendForUserSubset(userSubset, k)
    val allUserRecs = model.recommendForAllUsers(k).as[(Int, Seq[(Int, Float)])].collect().toMap
    checkRecommendations(userSubsetRecs, allUserRecs, "item")

    val itemSubset = model.itemFactors.withColumnRenamed("id", "item").drop("features")
    val itemSubsetRecs = model.recommendForItemSubset(itemSubset, k)
    val allItemRecs = model.recommendForAllItems(k).as[(Int, Seq[(Int, Float)])].collect().toMap
    checkRecommendations(itemSubsetRecs, allItemRecs, "user")
  }

  test("ALS should not introduce unnecessary shuffle") {
    def getShuffledDependencies(rdd: RDD[_]): Seq[ShuffleDependency[_, _, _]] = {
      rdd.dependencies.flatMap {
        case s: ShuffleDependency[_, _, _] =>
          Seq(s) ++ getShuffledDependencies(s.rdd)
        case o =>
          Seq.empty ++ getShuffledDependencies(o.rdd)
      }
    }

    val spark = this.spark
    import spark.implicits._
    val (ratings, _) = genExplicitTestData(numUsers = 2, numItems = 2, rank = 1)
    val data = ratings.toDF()
    val model = new ALS()
      .setMaxIter(2)
      .setImplicitPrefs(true)
      .setCheckpointInterval(-1)
      .fit(data)

    val userFactors = model.userFactors
    val itemFactors = model.itemFactors
    val shuffledUserFactors = getShuffledDependencies(userFactors.rdd).filter { dep =>
      dep.rdd.name != null && dep.rdd.name.contains("userFactors")
    }
    val shuffledItemFactors = getShuffledDependencies(itemFactors.rdd).filter { dep =>
      dep.rdd.name != null && dep.rdd.name.contains("itemFactors")
    }
    assert(shuffledUserFactors.size == 0)
    assert(shuffledItemFactors.size == 0)
  }

  private def checkRecommendations(
      topK: DataFrame,
      expected: Map[Int, Seq[(Int, Float)]],
      dstColName: String): Unit = {
    val spark = this.spark
    import spark.implicits._

    assert(topK.columns.contains("recommendations"))
    topK.as[(Int, Seq[(Int, Float)])].collect().foreach { case (id: Int, recs: Seq[(Int, Float)]) =>
      assert(recs === expected(id))
    }
    topK.collect().foreach { row =>
      val recs = row.getAs[mutable.ArraySeq[Row]]("recommendations")
      assert(recs(0).fieldIndex(dstColName) == 0)
      assert(recs(0).fieldIndex("rating") == 1)
    }
  }
}

class ALSCleanerSuite extends SparkFunSuite with LocalRootDirsTest {

  test("ALS shuffle cleanup in algorithm") {
    val conf = new SparkConf()
    val localDir = Utils.createTempDir()
    val checkpointDir = Utils.createTempDir()
    def getAllFiles: Set[File] = {
      val files = FileUtils.listFiles(
        localDir,
        TrueFileFilter.INSTANCE,
        TrueFileFilter.INSTANCE).asScala.toSet
      files
    }
    try {
      conf.set("spark.local.dir", localDir.getAbsolutePath)
      val sc = new SparkContext("local[2]", "ALSCleanerSuite", conf)
      val pattern = "shuffle_(\\d+)_.+\\.data".r
      try {
        sc.setCheckpointDir(checkpointDir.getAbsolutePath)
        // There should be 0 shuffle files at the start
        val initialIds = getAllFiles.flatMap { f =>
          pattern.findAllIn(f.getName()).matchData.map { _.group(1) } }
        assert(initialIds.size === 0)
        // Generate test data
        val (training, _) = ALSSuite.genImplicitTestData(sc, 20, 5, 1, 0.2, 0)
        // Implicitly test the cleaning of parents during ALS training
        val spark = SparkSession.builder()
          .sparkContext(sc)
          .getOrCreate()
        import spark.implicits._
        val als = new ALS()
          .setRank(1)
          .setRegParam(1e-5)
          .setSeed(0)
          .setCheckpointInterval(1)
          .setMaxIter(7)
        val model = als.fit(training.toDF())
        val resultingFiles = getAllFiles
        // We expect the last shuffles files, block ratings, user factors, and item factors to be
        // around but no more.
        val rddIds = resultingFiles.flatMap { f =>
          pattern.findAllIn(f.getName()).matchData.map { _.group(1) } }
        assert(rddIds.size === 4)
      } finally {
        sc.stop()
      }
    } finally {
      Utils.deleteRecursively(localDir)
      Utils.deleteRecursively(checkpointDir)
    }
  }
}

class ALSStorageSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("invalid storage params") {
    intercept[IllegalArgumentException] {
      new ALS().setIntermediateStorageLevel("foo")
    }
    intercept[IllegalArgumentException] {
      new ALS().setIntermediateStorageLevel("NONE")
    }
    intercept[IllegalArgumentException] {
      new ALS().setFinalStorageLevel("foo")
    }
  }

  test("default and non-default storage params set correct RDD StorageLevels") {
    val spark = this.spark
    import spark.implicits._
    val data = Seq(
      (0, 0, 1.0),
      (0, 1, 2.0),
      (1, 2, 3.0),
      (1, 0, 2.0)
    ).toDF("user", "item", "rating")
    val als = new ALS().setMaxIter(1).setRank(1)
    // add listener to check intermediate RDD default storage levels
    val defaultListener = new IntermediateRDDStorageListener
    sc.addSparkListener(defaultListener)
    val model = als.fit(data)
    // check final factor RDD default storage levels
    val defaultFactorRDDs = sc.getPersistentRDDs.collect {
      case (id, rdd) if rdd.name == "userFactors" || rdd.name == "itemFactors" =>
        rdd.name -> ((id, rdd.getStorageLevel))
    }.toMap
    defaultFactorRDDs.foreach { case (_, (id, level)) =>
      assert(level == StorageLevel.MEMORY_AND_DISK)
    }
    defaultListener.storageLevels.foreach(level => assert(level == StorageLevel.MEMORY_AND_DISK))

    // add listener to check intermediate RDD non-default storage levels
    val nonDefaultListener = new IntermediateRDDStorageListener
    sc.addSparkListener(nonDefaultListener)
    val nonDefaultModel = als
      .setFinalStorageLevel(StorageLevelMapper.MEMORY_ONLY.name())
      .setIntermediateStorageLevel(StorageLevelMapper.DISK_ONLY.name())
      .fit(data)
    // check final factor RDD non-default storage levels
    val levels = sc.getPersistentRDDs.collect {
      case (id, rdd) if rdd.name == "userFactors" && rdd.id != defaultFactorRDDs("userFactors")._1
        || rdd.name == "itemFactors" && rdd.id != defaultFactorRDDs("itemFactors")._1 =>
        rdd.getStorageLevel
    }
    levels.foreach(level => assert(level == StorageLevel.MEMORY_ONLY))
    nonDefaultListener.storageLevels.foreach(level => assert(level == StorageLevel.DISK_ONLY))
  }

  test("saved model size estimation") {
    import testImplicits._

    val als = new ALS().setMaxIter(1).setRank(8)
    val estimatedDFSize = (3 + 2) * (8 + 1) * 4
    val df = sc.parallelize(Seq(
      (123, 1, 0.5),
      (123, 2, 0.7),
      (123, 3, 0.6),
      (111, 2, 1.0),
      (111, 1, 0.1)
    )).toDF("item", "user", "rating")
    assert(als.estimateModelSize(df) === estimatedDFSize)

    val model = als.fit(df)
    assert(model.estimatedSize == estimatedDFSize)
  }
}

private class IntermediateRDDStorageListener extends SparkListener {

  val storageLevels: mutable.ArrayBuffer[StorageLevel] = mutable.ArrayBuffer()

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageLevels = stageCompleted.stageInfo.rddInfos.collect {
      case info if info.name.contains("Blocks") || info.name.contains("Factors-") =>
        info.storageLevel
    }
    storageLevels ++= stageLevels
  }

}

object ALSSuite extends Logging {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allModelParamSettings: Map[String, Any] = Map(
    "predictionCol" -> "myPredictionCol"
  )

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allEstimatorParamSettings: Map[String, Any] = allModelParamSettings ++ Map(
    "maxIter" -> 1,
    "rank" -> 1,
    "regParam" -> 0.01,
    "numUserBlocks" -> 2,
    "numItemBlocks" -> 2,
    "implicitPrefs" -> true,
    "alpha" -> 0.9,
    "nonnegative" -> true,
    "checkpointInterval" -> 20,
    "intermediateStorageLevel" -> StorageLevelMapper.MEMORY_ONLY.name(),
    "finalStorageLevel" -> StorageLevelMapper.MEMORY_AND_DISK_SER.name()
  )

  // Helper functions to generate test data we share between ALS test suites

  /**
   * Generates random user/item factors, with i.i.d. values drawn from U(a, b).
   * @param size number of users/items
   * @param rank number of features
   * @param random random number generator
   * @param a min value of the support (default: -1)
   * @param b max value of the support (default: 1)
   * @return a sequence of (ID, factors) pairs
   */
  private def genFactors(
      size: Int,
      rank: Int,
      random: Random,
      a: Float = -1.0f,
      b: Float = 1.0f): Seq[(Int, Array[Float])] = {
    require(size > 0 && size < Int.MaxValue / 3)
    require(b > a)
    val ids = mutable.Set.empty[Int]
    while (ids.size < size) {
      ids += random.nextInt()
    }
    val width = b - a
    ids.toSeq.sorted.map(id => (id, Array.fill(rank)(a + random.nextFloat() * width)))
  }

  /**
   * Generates an implicit feedback dataset for testing ALS.
   *
   * @param sc SparkContext
   * @param numUsers number of users
   * @param numItems number of items
   * @param rank rank
   * @param noiseStd the standard deviation of additive Gaussian noise on training data
   * @param seed random seed
   * @return (training, test)
   */
  def genImplicitTestData(
      sc: SparkContext,
      numUsers: Int,
      numItems: Int,
      rank: Int,
      noiseStd: Double = 0.0,
      seed: Long = 11L): (RDD[Rating[Int]], RDD[Rating[Int]]) = {
    // The assumption of the implicit feedback model is that unobserved ratings are more likely to
    // be negatives.
    val positiveFraction = 0.8
    val negativeFraction = 1.0 - positiveFraction
    val trainingFraction = 0.6
    val testFraction = 0.3
    val totalFraction = trainingFraction + testFraction
    val random = new Random(seed)
    val userFactors = genFactors(numUsers, rank, random)
    val itemFactors = genFactors(numItems, rank, random)
    val training = ArrayBuffer.empty[Rating[Int]]
    val test = ArrayBuffer.empty[Rating[Int]]
    for ((userId, userFactor) <- userFactors; (itemId, itemFactor) <- itemFactors) {
      val rating = BLAS.nativeBLAS.sdot(rank, userFactor, 1, itemFactor, 1)
      val threshold = if (rating > 0) positiveFraction else negativeFraction
      val observed = random.nextDouble() < threshold
      if (observed) {
        val x = random.nextDouble()
        if (x < totalFraction) {
          if (x < trainingFraction) {
            val noise = noiseStd * random.nextGaussian()
            training += Rating(userId, itemId, rating + noise.toFloat)
          } else {
            test += Rating(userId, itemId, rating)
          }
        }
      }
    }
    logInfo(log"Generated an implicit feedback dataset with ${MDC(TRAINING_SIZE, training.size)}" +
      log" ratings for training and ${MDC(TEST_SIZE, test.size)} for test.")
    (sc.parallelize(training.toSeq, 2), sc.parallelize(test.toSeq, 2))
  }
}
