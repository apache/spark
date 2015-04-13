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

package org.apache.spark.ps.strategy

/**
 * ModelPartitionStrategy
 * Created by genmao.ygm on 15-3-19.
 */
private[ps] class ModelPartitionStrategy(
    numRowPartitions: Int = 1,
    numColPartitions: Int = 1,
    numRows: Int = 1,
    numCols: Int = 1) {

  def getLocInServers(row: Int, col: Int): (Int, Int, Int) = {
    val rIdx = getIdxAtRange(row, numRows, numRowPartitions)
    val cIdx = getIdxAtRange(col, numCols, numColPartitions)
    val serverId = (rIdx - 1) * numRowPartitions + cIdx
    (serverId, row, col)
  }

  def getRowLocInServers(row: Int, col: Int): (Int, Int) = {
    val rIdx = getIdxAtRange(row, numRows, numRowPartitions)
    val cIdx = getIdxAtRange(col, numCols, numColPartitions)
    val serverId = (rIdx - 1) * numRowPartitions + cIdx
    (serverId, row)
  }

  private def getIdxAtRange(n: Int, len: Int, range: Int): Int = n * range / len + 1

}
