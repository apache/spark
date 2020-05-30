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

package org.apache.spark.sql

import java.io.File

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite to handle metadata cache related.
 */
abstract class MetadataCacheSuite extends QueryTest with SharedSparkSession {

  /** Removes one data file in the given directory. */
  protected def deleteOneFileInDirectory(dir: File): Unit = {
    assert(dir.isDirectory)
    val oneFile = dir.listFiles().find { file =>
      !file.getName.startsWith("_") && !file.getName.startsWith(".")
    }
    assert(oneFile.isDefined)
    oneFile.foreach(_.delete())
  }

  test("SPARK-16336,SPARK-27961 Suggest fixing FileNotFoundException") {
    withTempPath { (location: File) =>
      // Create an ORC directory
      spark.range(start = 0, end = 100, step = 1, numPartitions = 3)
        .write.orc(location.getAbsolutePath)

      // Read the directory in
      val df = spark.read.orc(location.getAbsolutePath)
      assert(df.count() == 100)

      // Delete a file
      deleteOneFileInDirectory(location)

      // Read it again and now we should see a FileNotFoundException
      val e = intercept[SparkException] {
        df.count()
      }
      assert(e.getMessage.contains("FileNotFoundException"))
      assert(e.getMessage.contains("recreating the Dataset/DataFrame involved"))
    }
  }
}

class MetadataCacheV1Suite extends MetadataCacheSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "orc")

  test("SPARK-16337 temporary view refresh") {
    withTempView("view_refresh") { withTempPath { (location: File) =>
      // Create an ORC directory
      spark.range(start = 0, end = 100, step = 1, numPartitions = 3)
        .write.orc(location.getAbsolutePath)

      // Read the directory in
      spark.read.orc(location.getAbsolutePath).createOrReplaceTempView("view_refresh")
      assert(sql("select count(*) from view_refresh").first().getLong(0) == 100)

      // Delete a file
      deleteOneFileInDirectory(location)

      // Read it again and now we should see a FileNotFoundException
      val e = intercept[SparkException] {
        sql("select count(*) from view_refresh").first()
      }
      assert(e.getMessage.contains("FileNotFoundException"))
      assert(e.getMessage.contains("REFRESH"))

      // Refresh and we should be able to read it again.
      spark.catalog.refreshTable("view_refresh")
      val newCount = sql("select count(*) from view_refresh").first().getLong(0)
      assert(newCount > 0 && newCount < 100)
    }}
  }

  test("case sensitivity support in temporary view refresh") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempView("view_refresh") {
        withTempPath { (location: File) =>
          // Create a Parquet directory
          spark.range(start = 0, end = 100, step = 1, numPartitions = 3)
            .write.orc(location.getAbsolutePath)

          // Read the directory in
          spark.read.orc(location.getAbsolutePath).createOrReplaceTempView("view_refresh")

          // Delete a file
          deleteOneFileInDirectory(location)
          intercept[SparkException](sql("select count(*) from view_refresh").first())

          // Refresh and we should be able to read it again.
          spark.catalog.refreshTable("vIeW_reFrEsH")
          val newCount = sql("select count(*) from view_refresh").first().getLong(0)
          assert(newCount > 0 && newCount < 100)
        }
      }
    }
  }
}

class MetadataCacheV2Suite extends MetadataCacheSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")
}
