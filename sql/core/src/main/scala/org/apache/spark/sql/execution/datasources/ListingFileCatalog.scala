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

import org.apache.hadoop.fs._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType


/**
 * A [[FileCatalog]] that generates the list of files to process by recursively listing all the
 * files present in `paths`.
 *
 * @param rootPaths the list of root table paths to scan
 * @param parameters as set of options to control discovery
 * @param partitionSchema an optional partition schema that will be use to provide types for the
 *                        discovered partitions
 */
class ListingFileCatalog(
    sparkSession: SparkSession,
    override val rootPaths: Seq[Path],
    parameters: Map[String, String],
    partitionSchema: Option[StructType])
  extends PartitioningAwareFileCatalog(sparkSession, parameters, partitionSchema) with Logging {

  @volatile private var cachedLeafFiles: mutable.LinkedHashMap[Path, FileStatus] = _
  @volatile private var cachedLeafDirToChildrenFiles: Map[Path, Array[FileStatus]] = _
  @volatile private var cachedPartitionSpec: PartitionSpec = _

  refresh()

  override def partitionSpec(): PartitionSpec = {
    if (cachedPartitionSpec == null) {
      cachedPartitionSpec = inferPartitioning()
    }
    logTrace(s"Partition spec: $cachedPartitionSpec")
    cachedPartitionSpec
  }

  override protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus] = {
    cachedLeafFiles
  }

  override protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = {
    cachedLeafDirToChildrenFiles
  }

  override def refresh(): Unit = {
    val files = listLeafFiles(rootPaths)
    cachedLeafFiles =
      new mutable.LinkedHashMap[Path, FileStatus]() ++= files.map(f => f.getPath -> f)
    cachedLeafDirToChildrenFiles = files.toArray.groupBy(_.getPath.getParent)
    cachedPartitionSpec = null
  }

  override def equals(other: Any): Boolean = other match {
    case hdfs: ListingFileCatalog => rootPaths.toSet == hdfs.rootPaths.toSet
    case _ => false
  }

  override def hashCode(): Int = rootPaths.toSet.hashCode()
}
