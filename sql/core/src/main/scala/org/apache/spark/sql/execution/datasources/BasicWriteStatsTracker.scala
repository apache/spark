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
 * Simple metrics collected during an instance of [[FileFormatWriter.ExecuteWriteTask]].
 * These were first introduced in https://github.com/apache/spark/pull/18159 (SPARK-20703).
 */
case class BasicWriteTaskStats(
    numPartitions: Int,
    numFiles: Int,
    numBytes: Long,
    numRows: Long)
  extends WriteTaskStats


/**
 * Simple [[WriteTaskStatsTracker]] implementation that produces [[BasicWriteTaskStats]].
 * @param hadoopConf
 */
class BasicWriteTaskStatsTracker(hadoopConf: Configuration)
  extends WriteTaskStatsTracker {

  private[this] var numPartitions: Int = 0
  private[this] var numFiles: Int = 0
  private[this] var numBytes: Long = 0L
  private[this] var numRows: Long = 0L

  private[this] var curFile: String = null


  private def getFileSize(filePath: String): Long = {
    val path = new Path(filePath)
    val fs = path.getFileSystem(hadoopConf)
    fs.getFileStatus(path).getLen()
  }


  override def newPartition(partitionValues: InternalRow): Unit = {
    numPartitions += 1
  }

  override def newBucket(bucketId: Int): Unit = {
    // currently unhandled
  }

  override def newFile(filePath: String): Unit = {
    if (numFiles > 0) {
      // we assume here that we've finished writing to disk the previous file by now
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
 * Simple [[WriteJobStatsTracker]] implementation that's serializable, capable of
 * instantiating [[BasicWriteTaskStatsTracker]] on executors and processing the
 * [[BasicWriteTaskStats]] they produce by aggregating the metrics and posting them
 * as DriverMetricUpdates.
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
    var numPartitions: Long = 0L
    var numFiles: Long = 0L
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
