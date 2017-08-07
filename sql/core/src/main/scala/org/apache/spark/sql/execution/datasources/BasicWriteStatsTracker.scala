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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.SerializableConfiguration


/**
 * TODO
 */
case class BasicWriteTaskStats(
    numPartitions: Int,
    numFiles: Int,
    numBytes: Long,
    numRows: Long)
  extends WriteTaskStats


/**
 * TODO
 * @param hadoopConf
 */
class BasicWriteTaskStatsTracker(hadoopConf: Configuration)
  extends WriteTaskStatsTracker {

  var numPartitions: Int = 0
  var numFiles: Int = 0
  var numBytes: Long = 0L
  var numRows: Long = 0L

  var curFile: String = null


  private def getFileSize(filePath: String): Long = {
    if (filePath != null) {
      val path = new Path(filePath)
      val fs = path.getFileSystem(hadoopConf)
      fs.getFileStatus(path).getLen()
    } else {
      0L
    }
  }


  override def newPartition(partitionValues: InternalRow): Unit = {
    numPartitions += 1
  }

  override def newBucket(bucketId: Int): Unit = {
    /* currently unhandled */
  }

  override def newFile(filePath: String): Unit = {
    if (numFiles > 0) {
      numBytes += getFileSize(curFile)
    }
    curFile = filePath
    numFiles += 1
  }

  override def newRow(row: InternalRow): Unit = {
    numRows += 1
  }

  override def getFinalStats(): WriteTaskStats = {
    if (numFiles > 0) {
      numBytes += getFileSize(curFile)
    }
    BasicWriteTaskStats(numPartitions, numFiles, numBytes, numRows)
  }
}


/**
 * TODO
 */
class BasicWriteJobStatsTracker(
    serializableHadoopConf: SerializableConfiguration,
    @transient val metrics: Map[String, SQLMetric])
  extends WriteJobStatsTracker {

  override def newTaskInstance(): WriteTaskStatsTracker = {
    new BasicWriteTaskStatsTracker(serializableHadoopConf.value)
  }

  override def processStats(stats: Seq[WriteTaskStats]): Unit = {
    val sparkContext = SparkContext.getActive.get
    var numPartitions = 0
    var numFiles = 0
    var totalNumBytes: Long = 0L
    var totalNumOutput: Long = 0L

    val basicStats = stats.map(_.asInstanceOf[BasicWriteTaskStats])

    basicStats.foreach { summary =>
      numPartitions += summary.numPartitions
      numFiles += summary.numFiles
      totalNumBytes += summary.numBytes
      totalNumOutput += summary.numRows
    }

    metrics("numFiles").add(numFiles)
    metrics("numOutputBytes").add(totalNumBytes)
    metrics("numOutputRows").add(totalNumOutput)
    metrics("numParts").add(numPartitions)

    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toList)
  }
}
