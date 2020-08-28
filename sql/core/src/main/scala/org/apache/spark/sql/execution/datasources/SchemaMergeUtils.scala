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
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

object SchemaMergeUtils extends Logging {
  /**
   * Figures out a merged Parquet/ORC schema with a distributed Spark job.
   */
  def mergeSchemasInParallel(
      sparkSession: SparkSession,
      parameters: Map[String, String],
      files: Seq[FileStatus],
      schemaReader: (Seq[FileStatus], Configuration, Boolean) => Seq[StructType])
      : Option[StructType] = {
    val serializedConf = new SerializableConfiguration(
      sparkSession.sessionState.newHadoopConfWithOptions(parameters))

    // !! HACK ALERT !!
    // Here is a hack for Parquet, but it can be used by Orc as well.
    //
    // Parquet requires `FileStatus`es to read footers.
    // Here we try to send cached `FileStatus`es to executor side to avoid fetching them again.
    // However, `FileStatus` is not `Serializable`
    // but only `Writable`.  What makes it worse, for some reason, `FileStatus` doesn't play well
    // with `SerializableWritable[T]` and always causes a weird `IllegalStateException`.  These
    // facts virtually prevents us to serialize `FileStatus`es.
    //
    // Since Parquet only relies on path and length information of those `FileStatus`es to read
    // footers, here we just extract them (which can be easily serialized), send them to executor
    // side, and resemble fake `FileStatus`es there.
    val partialFileStatusInfo = files.map(f => (f.getPath.toString, f.getLen))

    // Set the number of partitions to prevent following schema reads from generating many tasks
    // in case of a small number of orc files.
    val numParallelism = Math.min(Math.max(partialFileStatusInfo.size, 1),
      sparkSession.sparkContext.defaultParallelism)

    val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles

    // Issues a Spark job to read Parquet/ORC schema in parallel.
    val partiallyMergedSchemas =
      sparkSession
        .sparkContext
        .parallelize(partialFileStatusInfo, numParallelism)
        .mapPartitions { iterator =>
          // Resembles fake `FileStatus`es with serialized path and length information.
          val fakeFileStatuses = iterator.map { case (path, length) =>
            new FileStatus(length, false, 0, 0, 0, 0, null, null, null, new Path(path))
          }.toSeq

          val schemas = schemaReader(fakeFileStatuses, serializedConf.value, ignoreCorruptFiles)

          if (schemas.isEmpty) {
            Iterator.empty
          } else {
            var mergedSchema = schemas.head
            schemas.tail.foreach { schema =>
              try {
                mergedSchema = mergedSchema.merge(schema)
              } catch { case cause: SparkException =>
                throw new SparkException(
                  s"Failed merging schema:\n${schema.treeString}", cause)
              }
            }
            Iterator.single(mergedSchema)
          }
        }.collect()

    if (partiallyMergedSchemas.isEmpty) {
      None
    } else {
      var finalSchema = partiallyMergedSchemas.head
      partiallyMergedSchemas.tail.foreach { schema =>
        try {
          finalSchema = finalSchema.merge(schema)
        } catch { case cause: SparkException =>
          throw new SparkException(
            s"Failed merging schema:\n${schema.treeString}", cause)
        }
      }
      Some(finalSchema)
    }
  }
}
