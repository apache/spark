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

/**
 * An example app for running item similarity computation on MovieLens format
 * sparse data (http://grouplens.org/datasets/movielens/) through column based
 * similarity calculation and compare with row based similarity calculation and
 * ALS + row based similarity calculation flow. For running row and column based
 * similarity on raw features, we are using implicit matrix factorization.
 *
 *
 * A synthetic dataset in MovieLens format can be found at `data/mllib/sample_movielens_data.txt`.
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
package org.apache.spark.examples.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import org.apache.spark.mllib.evaluation.RankingMetrics
import scala.collection.mutable

object MovieLensSimilarity {

  case class Params(
      input: String = null,
      numIterations: Int = 20,
      rank: Int = 50,
      alpha: Double = 0.0,
      numUserBlocks: Int = -1,
      numProductBlocks: Int = -1,
      delim: String = "::",
      topk: Int = 50,
      threshold: Double = 1e-2) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("MovieLensSimilarity") {
      head("MovieLensSimilarity: an example app for similarity flows on MovieLens data.")
      opt[Int]("rank")
        .text(s"rank, default: ${defaultParams.rank}}")
        .action((x, c) => c.copy(rank = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[Int]("numUserBlocks")
        .text(s"number of user blocks, default: ${defaultParams.numUserBlocks} (auto)")
        .action((x, c) => c.copy(numUserBlocks = x))
      opt[Int]("numProductBlocks")
        .text(s"number of product blocks, default: ${defaultParams.numProductBlocks} (auto)")
        .action((x, c) => c.copy(numProductBlocks = x))
      opt[Double]("alpha")
        .text(s"alpha for implicit feedback")
        .action((x, c) => c.copy(alpha = x))
      opt[Int]("topk")
        .text("topk for ALS validation")
        .action((x, c) => c.copy(topk = x))
      opt[Double]("threshold")
        .text("threshold for dimsum sampling and kernel sparsity")
        .action((x, c) => c.copy(threshold = x))
      opt[String]("delim")
        .text("use delimiter, default ::")
        .action((x, c) => c.copy(delim = x))
      arg[String]("<input>")
        .required()
        .text("input paths to a MovieLens dataset of ratings")
        .action((x, c) => c.copy(input = x))
      note(
        """
          |For example, the following command runs this app on a synthetic dataset:
          |
          | bin/run-example mllib.MovieLensSimilarity \
          |  --rank 25 --numIterations 20 --alpha 0.01 --topk 25\
          |  data/mllib/sample_movielens_data.txt
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }

  def run(params: Params) {
    val conf =
      new SparkConf()
        .setAppName(s"MovieLensSimilarity with $params")
        .registerKryoClasses(Array(classOf[mutable.BitSet], classOf[Rating]))

    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    val delim = params.delim
    val ratings = sc.textFile(params.input).map { line =>
      val fields = line.split(delim)
      /*
         * MovieLens ratings are on a scale of 1-5:
         * 5: Must see
         * 4: Will enjoy
         * 3: It's okay
         * 2: Fairly bad
         * 1: Awful
         * So we should not recommend a movie if the predicted rating is less than 3.
         * To map ratings to confidence scores, we use
         * 5 -> 2.5, 4 -> 1.5, 3 -> 0.5, 2 -> -0.5, 1 -> -1.5. This mappings means unobserved
         * entries are generally between It's okay and Fairly bad.
         * The semantics of 0 in this expanded world of non-positive weights
         * are "the same as never having interacted at all".
         */
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble - 2.5)
    }.cache()

    val numRatings = ratings.count()
    val numUsers = ratings.map(_.user).distinct().count()
    val numMovies = ratings.map(_.product).distinct().count()

    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

    val productFeatures = ratings.map { entry =>
      MatrixEntry(entry.product, entry.user, entry.rating)
    }
    val productMatrix = new CoordinateMatrix(productFeatures).toIndexedRowMatrix()

    // brute force row similarities
    println("Running row similarities with threshold 1e-4")
    val rowSimilarities = productMatrix.rowSimilarities()

    // Row similarities using user defined threshold
    println(s"Running row similarities with threshold ${params.threshold}")
    val rowSimilaritiesApprox = productMatrix.rowSimilarities(threshold = params.threshold)

    // Compute similar columns on transpose matrix
    val userFeatures = ratings.map { entry =>
      MatrixEntry(entry.user, entry.product, entry.rating)
    }.repartition(sc.defaultParallelism).cache()

    val featureMatrix = new CoordinateMatrix(userFeatures).toRowMatrix()
    // Compute similar columns with dimsum sampling
    println(s"Running column similarity with threshold ${params.threshold}")
    val colSimilarities = featureMatrix.columnSimilarities(params.threshold)

    val exactEntries = rowSimilarities.entries.map { case MatrixEntry(i, j, u) => ((i, j), u) }
    val rowEntriesApprox = rowSimilaritiesApprox.entries.map { case MatrixEntry(i, j, u) =>
      ((i, j), u)
    }
    val colEntriesApprox = colSimilarities.entries.map { case MatrixEntry(i, j, v) => ((i, j), v) }

    val rowMAE = exactEntries.join(rowEntriesApprox).values.map {
      case (u, v) => math.abs(u - v)
    }

    val colMAE = exactEntries.join(colEntriesApprox).values.map {
      case (u, v) => math.abs(u - v)
    }

    println(s"Common entries row: ${rowMAE.count()} col: ${colMAE.count()}")
    println(s"Average absolute error in estimate row: ${rowMAE.mean()} col: ${colMAE.mean()}")

    val model = new ALS()
      .setRank(params.rank)
      .setIterations(params.numIterations)
      .setLambda(0.0)
      .setAlpha(params.alpha)
      .setImplicitPrefs(true)
      .setUserBlocks(params.numUserBlocks)
      .setProductBlocks(params.numProductBlocks)
      .run(ratings)

    // Compute similar columns through low rank approximation using ALS
    println(s"Running ALS based row similarities")

    val lowRankedSimilarities = model.similarProducts(params.topk)

    val labels = rowSimilarities.entries.map { case (entry) =>
      (entry.i, (entry.j, entry.value))
    }.topByKey(params.topk)(Ordering.by(_._2)).map { case (item, similarItems) =>
      (item, similarItems.map(_._1))
    }

    val predicted = lowRankedSimilarities.entries.map { case (entry) =>
      (entry.i, (entry.j, entry.value))
    }.topByKey(params.topk)(Ordering.by(_._2)).map { case (item, similarItems) =>
      (item, similarItems.map(_._1))
    }

    val predictionAndLabels =
      predicted.join(labels).map { case (item, (predicted, labels)) =>
        (predicted, labels)
      }

    val rankingMetrics = new RankingMetrics[Long](predictionAndLabels)
    println(s"MAP ${rankingMetrics.meanAveragePrecision}")

    sc.stop()
  }
}
