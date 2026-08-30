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

package org.apache.spark.sql.execution.datasources.orc

import org.apache.spark.DebugFilesystem
import org.apache.spark.memory.MemoryMode
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.orc.OrcPartitionReaderFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

class OrcPartitionReaderFactorySuite extends OrcTest with SharedSparkSession {

  import testImplicits._

  test("SPARK-57529: Fix possible ORC reader leak in OrcPartitionReaderFactory") {
    withTempPath { dir =>
      val dataSchema = StructType(Array(StructField("value", StringType)))
      spark.range(10)
        .select($"id".cast(StringType).as("value"))
        .write.orc(dir.getCanonicalPath)

      val orcFile = dir.listFiles(_.getName.endsWith(".orc")).headOption
        .getOrElse(fail("No ORC file written"))

      withSQLConf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val sqlConf = spark.sessionState.conf
        val hadoopConf = spark.sessionState.newHadoopConf()
        // Route file I/O through DebugFilesystem so we can assert no streams are leaked.
        hadoopConf.set("fs.file.impl", classOf[DebugFilesystem].getName)
        hadoopConf.set("fs.file.impl.disable.cache", "true")
        val broadcastedConf =
          spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

        val factory = OrcPartitionReaderFactory(
          sqlConf = sqlConf,
          broadcastedConf = broadcastedConf,
          dataSchema = dataSchema,
          readDataSchema = dataSchema,
          partitionSchema = StructType(Seq.empty),
          // Integer literal on a STRING column triggers IllegalArgumentException in
          // OrcFilters.createFilter -> buildLeafSearchArgument
          filters = Array(EqualTo("value", 1)),
          aggregation = None,
          options = new OrcOptions(Map.empty[String, String], sqlConf),
          memoryMode = MemoryMode.ON_HEAP)

        val partFile = PartitionedFile(
          partitionValues = InternalRow.empty,
          filePath = SparkPath.fromPathString(orcFile.getAbsolutePath),
          start = 0,
          length = orcFile.length())

        DebugFilesystem.clearOpenStreams()
        intercept[IllegalArgumentException] {
          factory.buildReader(partFile)
        }
        DebugFilesystem.assertNoOpenStreams()
      }
    }
  }
}
