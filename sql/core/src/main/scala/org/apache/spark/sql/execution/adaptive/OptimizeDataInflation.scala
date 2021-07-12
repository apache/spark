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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.exchange.{KeySketcher, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Estimate the joined size of each partition, via Independent Bernoulli Sampling:
 * The join size estimation algorithm is to form independent Bernoulli samples S1 and S2
 * (with sampling probabilities p1 and p2) of tables T1 and T2 that are being inner joined,
 * compute the join size J' of the two samples, and then scale it appropriately J = J' / p1 / p2.
 * This is a simplest and unbiased estimation algorithm.
 * See section 3.1 in <a href="https://www.vldb.org/pvldb/vol8/p1530-vengerov.pdf">
 * Join Size Estimation Subject to Filter Conditions</a> for details.
 */
object OptimizeDataInflation extends Logging {

  private[sql] def detectDataInflation(
      conf: SQLConf,
      left: ShuffleQueryStageExec,
      right: ShuffleQueryStageExec,
      joinType: JoinType): Map[Int, Double] = {
    if (conf.getConf(SQLConf.SKEW_JOIN_ENABLED) &&
      conf.getConf(SQLConf.SKEW_JOIN_INFLATION_ENABLED) &&
      Seq(Inner, LeftOuter, RightOuter).contains(joinType)) {

      val leftSketchOpt = left.shuffle.asInstanceOf[ShuffleExchangeExec]
        .shuffleExecAccumulators.find(_.isInstanceOf[KeySketcher])
        .map(_.asInstanceOf[KeySketcher])
      val leftMapOpt = leftSketchOpt.map(_.sketchMap)
        .filter(map => map != null && map.nonEmpty)

      val rightSketchOpt = right.shuffle.asInstanceOf[ShuffleExchangeExec]
        .shuffleExecAccumulators.find(_.isInstanceOf[KeySketcher])
        .map(_.asInstanceOf[KeySketcher])
      val rightMapOpt = rightSketchOpt.map(_.sketchMap)
        .filter(map => map != null && map.nonEmpty)

      if (leftMapOpt.isEmpty || rightMapOpt.isEmpty) return Map.empty
      val (leftMap, rightMap) = (leftMapOpt.get, rightMapOpt.get)

      val numPartitions = leftSketchOpt.get.numPartitions
      require(numPartitions == rightSketchOpt.get.numPartitions)

      // copied from org.apache.spark.sql.catalyst.expressions.Pmod
      def pmod(a: Int, n: Int): Int = {
        val r = a % n
        if (r < 0) {(r + n) % n} else r
      }

      def median(array: Array[Double]) : Double = {
        val len = array.length
        val sorted = array.sorted
        if (len % 2 == 0) {
          math.max(1, (sorted(len / 2) + sorted(len / 2 - 1)) / 2)
        } else {
          math.max(1, sorted(len / 2))
        }
      }

      val joinedSize = Array.ofDim[Double](numPartitions)
      joinType match {
        case Inner =>
          val Seq(smallMap, bigMap) = Seq(leftMap, rightMap).sortBy(_.size)
          smallMap.foreach { case (hash64, weight) =>
            if (bigMap.contains(hash64)) {
              val partitionId = pmod((hash64 >> 32).toInt, numPartitions)
              joinedSize(partitionId) += weight.toDouble * bigMap(hash64)
            }
          }

        case LeftOuter =>
          leftMap.foreach { case (hash64, weight) =>
            val partitionId = pmod((hash64 >> 32).toInt, numPartitions)
            joinedSize(partitionId) += weight
            if (rightMap.contains(hash64)) {
              joinedSize(partitionId) += weight.toDouble * rightMap(hash64)
            }
          }

        case RightOuter =>
          rightMap.foreach { case (hash64, weight) =>
            val partitionId = pmod((hash64 >> 32).toInt, numPartitions)
            joinedSize(partitionId) += weight
            if (leftMap.contains(hash64)) {
              joinedSize(partitionId) += weight.toDouble * leftMap(hash64)
            }
          }

        case _ => throw new UnsupportedOperationException(s"Unsupported joinType = $joinType")
      }
      logDebug("OptimizeDataInflation: estimated joined sizes(#row): " +
        s"${joinedSize.map(_.round).mkString("[", ", ", "]")}")

      val medianEstimatedSize = median(joinedSize)
      val threshold = conf.getConf(SQLConf.SKEW_JOIN_INFLATION_FACTOR) * medianEstimatedSize
      joinedSize.iterator.zipWithIndex
        .filter(_._1 >= threshold)
        .map { case (estimatedSize, pid) =>
          (pid, math.min(1000, estimatedSize / medianEstimatedSize))
        }.toMap
    } else {
      Map.empty
    }
  }

}
