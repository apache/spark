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
package org.apache.spark.sql.v2.avro

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.AvroOptions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.SerializableConfiguration

case class AvroScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter],
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty) extends FileScan {
  override def isSplitable(path: Path): Boolean = true

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = SerializableConfiguration.broadcast(sparkSession.sparkContext, hadoopConf)
    val parsedOptions = new AvroOptions(caseSensitiveMap, hadoopConf)
    // The partition values are already truncated in `FileScan.partitions`.
    // We should use `readPartitionSchema` as the partition schema here.
    AvroPartitionReaderFactory(
      conf,
      broadcastedConf,
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      parsedOptions,
      pushedFilters.toImmutableArraySeq)
  }

  override def equals(obj: Any): Boolean = obj match {
    case a: AvroScan => super.equals(a) && dataSchema == a.dataSchema && options == a.options &&
      equivalentFilters(pushedFilters, a.pushedFilters)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override def getMetaData(): Map[String, String] = {
    super.getMetaData() ++ Map("PushedFilters" -> seqToString(pushedFilters.toImmutableArraySeq))
  }
}
