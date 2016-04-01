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

class BlockExInfo(val blockId: RDDBlockId) extends Comparable[BlockExInfo] {

  var size: Long = _
  var creatStartTime: Long = _
  var creatFinTime: Long = _
  var creatCost: Long = _

  var serStartTime: Long = _
  var serFinTime: Long = _
  var serCost: Long = 0
  var serAndDeCost: Long = _

  var fakeSerCost: Long = 0

  var isExist: Int = 0
  // 0: not exist; 1: in-memory; 2: ser in disk
  var norCost: Double = _ // normalized cost

  var sonSet: Set[BlockId] = Set()

  // write the creatFinTime and cal the creatFinTime
  def writeFinAndCalCreatCost(finTime: Long) {
    creatFinTime = finTime
    creatCost = creatFinTime - creatStartTime
    norCost = creatCost.toDouble / (size / 1024 / 1024)
    isExist = 1
  }

  def writeAndCalSerCost(serStart: Long, serFin: Long): Unit = {
    serStartTime = serStart
    serFinTime = serFin
    serCost = serFinTime - serStartTime
    isExist = 2
  }

  def decidePolicy: Int = {
    if (creatCost < serAndDeCost) {
      norCost = creatCost.toDouble / size
      3 // creat Cost is low so just remove from memory
    } else {
      norCost = serAndDeCost.toDouble / size
      4 // ser and deser cost is low, so just ser to disk
    }
  }

  override def compareTo(o: BlockExInfo): Int = {
    this.norCost.compare(o.norCost)
  }
}