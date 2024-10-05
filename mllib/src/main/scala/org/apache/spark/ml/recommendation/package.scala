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

package org.apache.spark.ml

import com.google.common.collect.{Ordering => GuavaOrdering}

import org.apache.spark.ml.linalg.BLAS
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


package object recommendation {
  private[recommendation] def collect_top_k(e: Column, num: Int, reverse: Boolean): Column =
    Column.internalFn("collect_top_k", e, lit(num), lit(reverse))

  /**
   * Returns a subset of a factor DataFrame limited to only those unique ids contained
   * in the input dataset.
   * @param dataset input Dataset containing id column to user to filter factors.
   * @param factors factor DataFrame to filter.
   * @param column column name containing the ids in the input dataset.
   * @return DataFrame containing factors only for those ids present in both the input dataset and
   *         the factor DataFrame.
   */
  private[recommendation] def getSourceFactorSubset(
                                                     dataset: Dataset[_],
                                                     factors: DataFrame,
                                                     column: String): DataFrame = {
    factors
      .join(dataset.select(column), factors("id") === dataset(column), joinType = "left_semi")
      .select(factors("id"), factors("features"))
  }

  /**
   * Makes recommendations for all users (or items).
   *
   * Note: the previous approach used for computing top-k recommendations
   * used a cross-join followed by predicting a score for each row of the joined dataset.
   * However, this results in exploding the size of intermediate data. While Spark SQL makes it
   * relatively efficient, the approach implemented here is significantly more efficient.
   *
   * This approach groups factors into blocks and computes the top-k elements per block,
   * using GEMV (it use less memory compared with GEMM, and is much faster than DOT) and
   * an efficient selection based on [[GuavaOrdering]] (instead of [[BoundedPriorityQueue]]).
   * It then computes the global top-k by aggregating the per block top-k elements with
   * a [[TopByKeyAggregator]]. This significantly reduces the size of intermediate and shuffle data.
   * This is the DataFrame equivalent to the approach used in
   * [[org.apache.spark.mllib.recommendation.MatrixFactorizationModel]].
   *
   * @param srcFactors src factors for which to generate recommendations
   * @param dstFactors dst factors used to make recommendations
   * @param srcOutputColumn name of the column for the source ID in the output DataFrame
   * @param dstOutputColumn name of the column for the destination ID in the output DataFrame
   * @param num max number of recommendations for each record
   * @return a DataFrame of (srcOutputColumn: Int, recommendations), where recommendations are
   *         stored as an array of (dstOutputColumn: Int, rating: Float) Rows.
   */
  private[recommendation] def recommendForAll(
                                               rank: Int,
                                               srcFactors: DataFrame,
                                               dstFactors: DataFrame,
                                               srcOutputColumn: String,
                                               dstOutputColumn: String,
                                               num: Int,
                                               blockSize: Int): DataFrame = {
    import srcFactors.sparkSession.implicits._
    import scala.jdk.CollectionConverters._

    val ratingColumn = "rating"
    val recommendColumn = "recommendations"
    val srcFactorsBlocked = blockify(srcFactors.as[(Int, Array[Float])], blockSize)
    val dstFactorsBlocked = blockify(dstFactors.as[(Int, Array[Float])], blockSize)
    val ratings = srcFactorsBlocked.crossJoin(dstFactorsBlocked)
      .as[(Array[Int], Array[Float], Array[Int], Array[Float])]
      .mapPartitions { iter =>
        var scores: Array[Float] = null
        var idxOrd: GuavaOrdering[Int] = null
        iter.flatMap { case (srcIds, srcMat, dstIds, dstMat) =>
          require(srcMat.length == srcIds.length * rank)
          require(dstMat.length == dstIds.length * rank)
          val m = srcIds.length
          val n = dstIds.length
          if (scores == null || scores.length < n) {
            scores = Array.ofDim[Float](n)
            idxOrd = new GuavaOrdering[Int] {
              override def compare(left: Int, right: Int): Int = {
                Ordering[Float].compare(scores(left), scores(right))
              }
            }
          }

          Iterator.range(0, m).flatMap { i =>
            // scores = i-th vec in srcMat * dstMat
            BLAS.javaBLAS.sgemv("T", rank, n, 1.0F, dstMat, 0, rank,
              srcMat, i * rank, 1, 0.0F, scores, 0, 1)

            val srcId = srcIds(i)
            idxOrd.greatestOf(Iterator.range(0, n).asJava, num).asScala
              .iterator.map { j => (srcId, dstIds(j), scores(j)) }
          }
        }
      }.toDF(srcOutputColumn, dstOutputColumn, ratingColumn)

    val arrayType = ArrayType(
      new StructType()
        .add(dstOutputColumn, IntegerType)
        .add(ratingColumn, FloatType)
    )

    ratings.groupBy(srcOutputColumn)
      .agg(collect_top_k(struct(ratingColumn, dstOutputColumn), num, false))
      .as[(Int, Seq[(Float, Int)])]
      .map(t => (t._1, t._2.map(p => (p._2, p._1))))
      .toDF(srcOutputColumn, recommendColumn)
      .withColumn(recommendColumn, col(recommendColumn).cast(arrayType))
  }

  /**
   * Blockifies factors to improve the efficiency of cross join
   */
  private[recommendation] def blockify(
                                        factors: Dataset[(Int, Array[Float])],
                                        blockSize: Int): Dataset[(Array[Int], Array[Float])] = {
    import factors.sparkSession.implicits._
    factors.mapPartitions { iter =>
      iter.grouped(blockSize)
        .map(block => (block.map(_._1).toArray, block.flatMap(_._2).toArray))
    }
  }
}
