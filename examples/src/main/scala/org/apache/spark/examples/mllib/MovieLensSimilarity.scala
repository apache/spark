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
 * An example app for running item similarity computation on MovieLens data
 * (http://grouplens.org/datasets/movielens/) through column based similarity
 * calculation flow compared with ALS + row based similarity calculation flow
 *
 * Run with
 * {{{
 * bin/run-example org.apache.spark.examples.mllib.MovieLensALS
 * }}}
 *
 * A synthetic dataset in MovieLens format can be found at `data/mllib/sample_movielens_data.txt`.
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
package org.apache.spark.examples.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import org.apache.spark.mllib.recommendation.Rating

import scala.collection.mutable

object MovieLensSimilarity {

  case class Params(
      input: String = null,
      kryo: Boolean = false,
      numIterations: Int = 20,
      lambda: Double = 1.0,
      rank: Int = 10,
      numUserBlocks: Int = -1,
      numProductBlocks: Int = -1,
      implicitPrefs: Boolean = false,
      threshold: Double = 1e-4) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("MovieLensALS") {
      head("MovieLensALS: an example app for ALS on MovieLens data.")
      opt[Int]("rank")
        .text(s"rank, default: ${defaultParams.rank}}")
        .action((x, c) => c.copy(rank = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("lambda")
        .text(s"lambda (smoothing constant), default: ${defaultParams.lambda}")
        .action((x, c) => c.copy(lambda = x))
      opt[Unit]("kryo")
        .text("use Kryo serialization")
        .action((_, c) => c.copy(kryo = true))
      opt[Int]("numUserBlocks")
        .text(s"number of user blocks, default: ${defaultParams.numUserBlocks} (auto)")
        .action((x, c) => c.copy(numUserBlocks = x))
      opt[Int]("numProductBlocks")
        .text(s"number of product blocks, default: ${defaultParams.numProductBlocks} (auto)")
        .action((x, c) => c.copy(numProductBlocks = x))
      opt[Unit]("implicitPrefs")
        .text("use implicit preference")
        .action((_, c) => c.copy(implicitPrefs = true))
      opt[Double]("threshold")
        .text("similarity threshold for dimsum sampling and kernel shuffle optimization")
        .action((x, c) => c.copy(threshold = x))
      arg[String]("<input>")
        .required()
        .text("input paths to a MovieLens dataset of ratings")
        .action((x, c) => c.copy(input = x))
      note(
        """
          |For example, the following command runs this app on a synthetic dataset:
          |
          | bin/spark-submit --class org.apache.spark.examples.mllib.MovieLensALS \
          |  examples/target/scala-*/spark-examples-*.jar \
          |  --rank 5 --numIterations 20 --lambda 1.0 --kryo \
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
    val conf = new SparkConf().setAppName(s"MovieLensSimilarity with $params")
    if (params.kryo) {
      conf.registerKryoClasses(Array(classOf[mutable.BitSet], classOf[Rating]))
        .set("spark.kryoserializer.buffer.mb", "8")
    }
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val implicitPrefs = params.implicitPrefs

    val ratings = sc.textFile(params.input).map { line =>
      val fields = line.split("::")
      if (implicitPrefs) {
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
      } else {
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
      }
    }.cache()

    val numRatings = ratings.count()
    val numUsers = ratings.map(_.user).distinct().count()
    val numMovies = ratings.map(_.product).distinct().count()

    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

    val productFeatures = ratings.map { entry =>
      MatrixEntry(entry.product, entry.user, entry.rating)
    }
    val productMatrix = new CoordinateMatrix(productFeatures).toIndexedRowMatrix()
    val rowSimilarities = productMatrix.rowSimilarities(threshold=params.threshold)

    // Compute similar rows through dimsum sampling
    val userFeatures = ratings.map { entry =>
      MatrixEntry(entry.user, entry.product, entry.rating)
    }
    val featureMatrix = new CoordinateMatrix(userFeatures).toRowMatrix()
    val colSimilarities = featureMatrix.columnSimilarities(params.threshold)

    val rowEntries = rowSimilarities.entries.map { case MatrixEntry(i, j, u) => ((i, j), u) }
    val colEntries = colSimilarities.entries.map { case MatrixEntry(i, j, v) => ((i, j), v) }
    val MAE = colEntries.leftOuterJoin(rowEntries).values.map {
      case (u, Some(v)) =>
        math.abs(u - v)
      case (u, None) =>
        math.abs(u)
    }.mean()

    println(s"Average absolute error in estimate is: $MAE")

    sc.stop()
  }
}
