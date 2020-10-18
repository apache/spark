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

import org.apache.spark.sql.{Encoders, FakeFileSystemRequiringDSOption, QueryTest, Row}

// The trait contains tests for all file-based data sources. The tests that are not applicable to
// all file-based data sources should be placed to `FileBasedDataSourceSuite`.
trait CommonFileDataSourceSuite { self: QueryTest =>

  protected def dataSourceFormat: String

  test(s"Propagate Hadoop configs from $dataSourceFormat options to underlying file system") {
    withSQLConf(
      "fs.file.impl" -> classOf[FakeFileSystemRequiringDSOption].getName,
      "fs.file.impl.disable.cache" -> "true") {
      Seq(false, true).foreach { mergeSchema =>
        withTempPath { dir =>
          val path = dir.getAbsolutePath
          val conf = Map("ds_option" -> "value", "mergeSchema" -> mergeSchema.toString)
          spark.createDataset(Seq("abc"))(Encoders.STRING)
            .write
            .options(conf)
            .format(dataSourceFormat)
            .save(path)
          Seq(path, "file:" + path.stripPrefix("file:")).foreach { p =>
            val readback = spark
              .read
              .options(conf)
              .format(dataSourceFormat)
              .load(p)
            // Checks that read doesn't throw the exception from `FakeFileSystemRequiringDSOption`
            readback.write.mode("overwrite").format("noop").save()
          }
        }
      }
    }
  }
}
