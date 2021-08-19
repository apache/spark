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

import java.util.Locale

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.{AnalysisException, Dataset, Encoders, FakeFileSystemRequiringDSOption, SparkSession}
import org.apache.spark.sql.catalyst.plans.SQLHelper

/**
 * The trait contains tests for all file-based data sources.
 * The tests that are not applicable to all file-based data sources should be placed to
 * [[org.apache.spark.sql.FileBasedDataSourceSuite]].
 */
trait CommonFileDataSourceSuite extends SQLHelper { self: AnyFunSuite =>

  protected def spark: SparkSession
  protected def dataSourceFormat: String
  protected def inputDataset: Dataset[_] = spark.createDataset(Seq("abc"))(Encoders.STRING)

  test(s"SPARK-36349: disallow saving of ANSI intervals to $dataSourceFormat") {
    Seq("INTERVAL '1' DAY", "INTERVAL '1' YEAR").foreach { i =>
      withTempPath { dir =>
        val errMsg = intercept[AnalysisException] {
          spark.sql(s"SELECT $i").write.format(dataSourceFormat).save(dir.getAbsolutePath)
        }.getMessage
        assert(errMsg.contains("Cannot save interval data type into external storage"))
      }
    }

    // Check all built-in file-based datasources except of libsvm which requires particular schema.
    if (!Set("libsvm").contains(dataSourceFormat.toLowerCase(Locale.ROOT))) {
      Seq("INTERVAL DAY TO SECOND", "INTERVAL YEAR TO MONTH").foreach { it =>
        val errMsg = intercept[AnalysisException] {
          spark.sql(s"CREATE TABLE t (i $it) USING $dataSourceFormat")
        }.getMessage
        assert(errMsg.contains("data source does not support"))
      }
    }
  }

  test(s"Propagate Hadoop configs from $dataSourceFormat options to underlying file system") {
    withSQLConf(
      "fs.file.impl" -> classOf[FakeFileSystemRequiringDSOption].getName,
      "fs.file.impl.disable.cache" -> "true") {
      Seq(false, true).foreach { mergeSchema =>
        withTempPath { dir =>
          val path = dir.getAbsolutePath
          val conf = Map("ds_option" -> "value", "mergeSchema" -> mergeSchema.toString)
          inputDataset
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
