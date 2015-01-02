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

package org.apache.spark.storage

import scala.collection.mutable

import org.apache.spark.Logging

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._
import org.apache.spark.executor.{BlockAccess, BlockAccessType, DataReadMethod}

/**
 * :: DeveloperApi ::
 * A SparkListener that maintains RDD and block hit and miss rates.
 */
class MissRateListener extends SparkListener {
  // Hit/miss rates for reads
  private[storage] val rddToHitMissCount = mutable.Map[Int, mutable.Map[BlockId, (Int, Int)]]()
  private[storage] val nonRddToHitMissCount = mutable.Map[BlockId, (Int, Int)]()

  def missRateFor(rddId: Int): Option[Double] = {
    rddToHitMissCount.get(rddId).map { hitMissRates =>
      hitMissRates.values.map { case (hitCount, missCount) => 
        (hitCount, missCount - 1) // One compulsory miss.
      }.reduce { (pair1: (Int, Int), pair2: (Int, Int)) => 
        (pair1._1 + pair2._1, pair1._2 + pair2._2)
      }
    }.filter(_ != 0 -> 0).map {
      case (hitCount, missCount) => missCount.asInstanceOf[Double] / (hitCount + missCount)
    }
  }

  private def recordBlockAccess(blockId: BlockId, blockAccess: BlockAccess) {
    if (blockAccess.accessType == BlockAccessType.Read) {
      val hitMissMap = blockId match {
        case RDDBlockId(rddId, split) => 
          rddToHitMissCount.getOrElseUpdate(rddId, mutable.Map.empty[BlockId, (Int, Int)])
        case _ => nonRddToHitMissCount
      }
      val oldHitMissRate = hitMissMap.getOrElse(blockId, (0, 0))
      hitMissMap(blockId) = blockAccess.inputMetrics.map(_.readMethod) match {
        case None => (oldHitMissRate._1, oldHitMissRate._2 + 1)
        case Some(_) => (oldHitMissRate._1 + 1, oldHitMissRate._2)
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = synchronized {
    val info = taskEnd.taskInfo
    val metrics = taskEnd.taskMetrics
    if (metrics != null) {
      val accessedBlocks = metrics.accessedBlocks.getOrElse(Seq[(BlockId, BlockAccess)]())
      accessedBlocks.foreach { case (id, access) => recordBlockAccess(id, access) }
    }
  }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) = synchronized {
    rddToHitMissCount.remove(unpersistRDD.rddId)
  }
}
