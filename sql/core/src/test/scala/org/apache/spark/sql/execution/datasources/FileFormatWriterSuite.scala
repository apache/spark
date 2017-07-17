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

import java.io.{File, FilenameFilter}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext

class FileFormatWriterSuite extends QueryTest with SharedSQLContext {

  test("empty file should be skipped while write to file") {
    withTempDir { dir =>
      dir.delete()
      spark.range(10000).repartition(10).write.parquet(dir.toString)
      val df = spark.read.parquet(dir.toString)
      val allFiles = dir.listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          !name.startsWith(".") && !name.startsWith("_")
        }
      })
      assert(allFiles.length == 10)

      withTempDir { dst_dir =>
        dst_dir.delete()
        df.where("id = 50").write.parquet(dst_dir.toString)
        val allFiles = dst_dir.listFiles(new FilenameFilter {
          override def accept(dir: File, name: String): Boolean = {
            !name.startsWith(".") && !name.startsWith("_")
          }
        })
        // First partition file and the data file
        assert(allFiles.length == 2)
      }
    }
  }
}
