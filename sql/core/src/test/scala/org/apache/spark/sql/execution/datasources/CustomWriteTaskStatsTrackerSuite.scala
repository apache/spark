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

package org.apache.spark.sql.execution.datasources

import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow

class CustomWriteTaskStatsTrackerSuite extends SparkFunSuite {

  def checkFinalStats(tracker: CustomWriteTaskStatsTracker, result: Map[String, Int]): Unit = {
    assert(tracker.getFinalStats(0L, 0L)
      .asInstanceOf[CustomWriteTaskStats].numRowsPerFile == result)
  }

  test("sequential file writing") {
    val tracker = new CustomWriteTaskStatsTracker
    tracker.newFile("a")
    tracker.newRow("a", null)
    tracker.newRow("a", null)
    tracker.newFile("b")
    checkFinalStats(tracker, Map("a" -> 2, "b" -> 0))
  }

  test("random file writing") {
    val tracker = new CustomWriteTaskStatsTracker
    tracker.newFile("a")
    tracker.newRow("a", null)
    tracker.newFile("b")
    tracker.newRow("a", null)
    tracker.newRow("b", null)
    checkFinalStats(tracker, Map("a" -> 2, "b" -> 1))
  }
}

class CustomWriteTaskStatsTracker extends WriteTaskStatsTracker {

  val numRowsPerFile = mutable.Map.empty[String, Int]

  override def newPartition(partitionValues: InternalRow): Unit = {}

  override def newFile(filePath: String): Unit = {
    numRowsPerFile.put(filePath, 0)
  }

  override def closeFile(filePath: String): Unit = {}

  override def newRow(filePath: String, row: InternalRow): Unit = {
    numRowsPerFile(filePath) += 1
  }

  override def getFinalStats(taskCommitTime: Long, taskWriteDataTime: Long): WriteTaskStats = {
    CustomWriteTaskStats(numRowsPerFile.toMap)
  }
}

case class CustomWriteTaskStats(numRowsPerFile: Map[String, Int]) extends WriteTaskStats
