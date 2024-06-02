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

package org.apache.spark.sql.hive

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

/**
 * Test suite to handle metadata cache related.
 */
class HiveMetadataCacheSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  test("SPARK-16337 temporary view refresh") {
    checkRefreshView(isTemp = true)
  }

  test("view refresh") {
    checkRefreshView(isTemp = false)
  }

  private def checkRefreshView(isTemp: Boolean): Unit = {
    withView("view_refresh") {
      withTable("view_table") {
        // Create a Parquet directory
        spark.range(start = 0, end = 100, step = 1, numPartitions = 3)
          .write.saveAsTable("view_table")

        val temp = if (isTemp) "TEMPORARY" else ""
        spark.sql(s"CREATE $temp VIEW view_refresh AS SELECT * FROM view_table WHERE id > -1")
        assert(sql("select count(*) from view_refresh").first().getLong(0) == 100)

        // Delete a file using the Hadoop file system interface since the path returned by
        // inputFiles is not recognizable by Java IO.
        val p = new Path(spark.table("view_table").inputFiles.head)
        assert(p.getFileSystem(hiveContext.sessionState.newHadoopConf()).delete(p, false))

        // Read it again and now we should see a FileNotFoundException
        checkErrorMatchPVals(
          exception = intercept[SparkException] {
            sql("select count(*) from view_refresh").first()
          },
          errorClass = "FAILED_READ_FILE.FILE_NOT_EXIST",
          parameters = Map("path" -> ".*")
        )

        // Refresh and we should be able to read it again.
        spark.catalog.refreshTable("view_refresh")
        val newCount = sql("select count(*) from view_refresh").first().getLong(0)
        assert(newCount > 0 && newCount < 100)
      }
    }
  }

  def testCaching(pruningEnabled: Boolean): Unit = {
    test(s"partitioned table is cached when partition pruning is $pruningEnabled") {
      withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> pruningEnabled.toString) {
        withTable("test") {
          withTempDir { dir =>
            spark.range(5).selectExpr("id", "id as f1", "id as f2").write
              .partitionBy("f1", "f2")
              .mode("overwrite")
              .parquet(dir.getAbsolutePath)

            spark.sql(s"""
              |create external table test (id long)
              |partitioned by (f1 int, f2 int)
              |stored as parquet
              |location "${dir.toURI}"""".stripMargin)
            spark.sql("msck repair table test")

            val df = spark.sql("select * from test")
            assert(sql("select * from test").count() == 5)

            def deleteRandomFile(): Unit = {
              val p = new Path(spark.table("test").inputFiles.head)
              assert(p.getFileSystem(hiveContext.sessionState.newHadoopConf()).delete(p, true))
            }

            // Delete a file, then assert that we tried to read it. This means the table was cached.
            deleteRandomFile()
            checkErrorMatchPVals(
              exception = intercept[SparkException] {
                sql("select * from test").count()
              },
              errorClass = "FAILED_READ_FILE.FILE_NOT_EXIST",
              parameters = Map("path" -> ".*")
            )

            // Test refreshing the cache.
            spark.catalog.refreshTable("test")
            assert(sql("select * from test").count() == 4)
            assert(spark.table("test").inputFiles.length == 4)

            // Test refresh by path separately since it goes through different code paths than
            // refreshTable does.
            deleteRandomFile()
            spark.catalog.cacheTable("test")
            spark.catalog.refreshByPath("/some-invalid-path")  // no-op
            checkErrorMatchPVals(
              exception = intercept[SparkException] {
                sql("select * from test").count()
              },
              errorClass = "FAILED_READ_FILE.FILE_NOT_EXIST",
              parameters = Map("path" -> ".*")
            )
            spark.catalog.refreshByPath(dir.getAbsolutePath)
            assert(sql("select * from test").count() == 3)
          }
        }
      }
    }
  }

  for (pruningEnabled <- Seq(true, false)) {
    testCaching(pruningEnabled)
  }
}
