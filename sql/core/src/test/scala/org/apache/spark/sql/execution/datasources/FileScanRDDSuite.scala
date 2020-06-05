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

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class FileScanRDDSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("SPARK-31913: Fix StackOverflowError in FileScanRDD") {
    def copyFiles(numFiles: Int, dir: File): Unit = {
      val files = dir.list().filter(_.startsWith("part-"))
      assert(files.length > 0)
      val fileSuffix = files.head.substring(11)
      (1 to numFiles).foreach { id =>
        val dst = new File(dir, s"part-$id-$fileSuffix")
        Files.copy(new File(dir, files.head).toPath, dst.toPath)
      }
    }

    withSQLConf(
      "fs.file.impl" -> classOf[MockDistributedFileSystem].getName,
      SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD.key -> "2",
      SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1024MB",
      SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "102400") {
      withTempPath { tempDir =>
        val schema = StructType(Array(StructField("a", StringType)))
        val data = Seq().asInstanceOf[Seq[String]].toDF("a")
        data.write.format("parquet").save(tempDir.getCanonicalPath)
        copyFiles(10000, tempDir)
        val df = spark.read.schema(schema).format("parquet").load(tempDir.toURI.toString)
        assert(df.count() == 0L)
      }
    }
  }
}
