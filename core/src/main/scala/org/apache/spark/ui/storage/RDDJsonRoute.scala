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
package org.apache.spark.ui.storage

import javax.servlet.http.HttpServletRequest

import org.apache.spark.status.api._
import org.apache.spark.status.{JsonRequestHandler, StatusJsonRoute}
import org.apache.spark.storage.{StorageStatus, RDDInfo, StorageUtils}

class RDDJsonRoute(parent: JsonRequestHandler) extends StatusJsonRoute[RDDStorageInfo] {

  override def renderJson(route: HttpServletRequest): RDDStorageInfo = {
    parent.withSparkUI(route){case(ui, route) =>
      val rddIdOpt = JsonRequestHandler.extractRDDId(route.getPathInfo)
      rddIdOpt match {
        case Some(rddId) =>
          RDDJsonRoute.getRDDStorageInfo(rddId, ui.storageListener, true).getOrElse{
            throw new IllegalArgumentException("no rdd found w/ id " + rddId)
          }
        case None =>
          throw new IllegalArgumentException("no valid rdd id in path")
      }

    }
  }
}

object RDDJsonRoute {

  def getRDDStorageInfo(rddId: Int, listener: StorageListener, includeDetails: Boolean): Option[RDDStorageInfo] = {
    val storageStatusList = listener.storageStatusList
    listener.rddInfoList.find(_.id == rddId).map{rddInfo =>
      getRDDStorageInfo(rddId, rddInfo, storageStatusList, includeDetails)
    }
  }

  def getRDDStorageInfo(
    rddId: Int,
    rddInfo: RDDInfo,
    storageStatusList: Seq[StorageStatus],
    includeDetails: Boolean
  ): RDDStorageInfo = {
    val workers = storageStatusList.map((rddId, _))
    val blockLocations = StorageUtils.getRddBlockLocations(rddId, storageStatusList)
    val blocks = storageStatusList
      .flatMap(_.rddBlocksById(rddId))
      .sortWith(_._1.name < _._1.name)
      .map { case (blockId, status) =>
      (blockId, status, blockLocations.get(blockId).getOrElse(Seq[String]("Unknown")))
    }


    val dataDistribution = if (includeDetails) {
      Some(storageStatusList.map{status =>
        RDDDataDistribution(
          address = status.blockManagerId.hostPort,
          memoryUsed = status.memUsedByRdd(rddId),
          memoryRemaining = status.memRemaining,
          diskUsed = status.diskUsedByRdd(rddId)
        )})
    } else {
      None
    }
    val partitions = if (includeDetails) {
      Some(blocks.map{ case(id, block, locations) =>
        RDDPartitionInfo(
          blockName = id.name,
          storageLevel = block.storageLevel.description,
          memoryUsed = block.memSize,
          diskUsed = block.diskSize,
          executors = locations
        )
      })
    } else {
      None
    }

    RDDStorageInfo(
      id = rddId,
      name = rddInfo.name,
      numPartitions = rddInfo.numPartitions,
      numCachedPartitions = rddInfo.numCachedPartitions,
      storageLevel = rddInfo.storageLevel.description,
      memoryUsed = rddInfo.memSize,
      diskUsed = rddInfo.diskSize,
      dataDistribution = dataDistribution,
      partitions = partitions
    )
  }
}
