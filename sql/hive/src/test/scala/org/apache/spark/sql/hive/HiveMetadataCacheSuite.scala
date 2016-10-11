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
import org.apache.spark.sql.test.SQLTestUtils

/**
 * Test suite to handle metadata cache related.
 */
class HiveMetadataCacheSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  test("SPARK-16337 temporary view refresh") {
    withTempView("view_refresh") {
      withTable("view_table") {
        // Create a Parquet directory
        spark.range(start = 0, end = 100, step = 1, numPartitions = 3)
          .write.saveAsTable("view_table")

        // Read the table in
        spark.table("view_table").filter("id > -1").createOrReplaceTempView("view_refresh")
        assert(sql("select count(*) from view_refresh").first().getLong(0) == 100)

        // Delete a file using the Hadoop file system interface since the path returned by
        // inputFiles is not recognizable by Java IO.
        val p = new Path(spark.table("view_table").inputFiles.head)
        assert(p.getFileSystem(hiveContext.sessionState.newHadoopConf()).delete(p, false))

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
      }
    }
  }
}
