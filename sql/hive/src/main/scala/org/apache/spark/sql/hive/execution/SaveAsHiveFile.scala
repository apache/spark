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

package org.apache.spark.sql.hive.execution

import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.datasources.FileFormatWriter
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}

// Base trait from which all hive insert statement physical execution extends.
private[hive] trait SaveAsHiveFile extends DataWritingCommand {

  protected def saveAsHiveFile(sparkSession: SparkSession,
                               plan: SparkPlan,
                               hadoopConf: Configuration,
                               fileSinkConf: FileSinkDesc,
                               outputLocation: String,
                               partitionAttributes: Seq[Attribute] = Nil,
                               bucketSpec: Option[BucketSpec] = None,
                               options: Map[String, String] = Map.empty): Unit = {

    val sessionState = sparkSession.sessionState

    val isCompressed = hadoopConf.get("hive.exec.compress.output", "false").toBoolean
    if (isCompressed) {
      // Please note that isCompressed, "mapreduce.output.fileoutputformat.compress",
      // "mapreduce.output.fileoutputformat.compress.codec", and
      // "mapreduce.output.fileoutputformat.compress.type"
      // have no impact on ORC because it uses table properties to store compression information.
      hadoopConf.set("mapreduce.output.fileoutputformat.compress", "true")
      fileSinkConf.setCompressed(true)
      fileSinkConf.setCompressCodec(hadoopConf
        .get("mapreduce.output.fileoutputformat.compress.codec"))
      fileSinkConf.setCompressType(hadoopConf
        .get("mapreduce.output.fileoutputformat.compress.type"))
    }

    val committer = FileCommitProtocol.instantiate(
      sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = java.util.UUID.randomUUID().toString,
      outputPath = outputLocation)

    FileFormatWriter.write(
      sparkSession = sparkSession,
      plan = plan,
      fileFormat = new HiveFileFormat(fileSinkConf),
      committer = committer,
      outputSpec = FileFormatWriter.OutputSpec(outputLocation, Map.empty),
      hadoopConf = hadoopConf,
      partitionColumns = partitionAttributes,
      bucketSpec = bucketSpec,
      statsTrackers = Seq(basicWriteJobStatsTracker(hadoopConf)),
      options = options)
  }
}

