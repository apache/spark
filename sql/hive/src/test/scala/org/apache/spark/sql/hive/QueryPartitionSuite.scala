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

import java.sql.Timestamp

import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

class QueryPartitionSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import spark.implicits._

  private def queryWhenPathNotExist(): Unit = {
    withTempView("testData") {
      withTable("table_with_partition", "createAndInsertTest") {
        withTempDir { tmpDir =>
          val testData = sparkContext.parallelize(
            (1 to 10).map(i => TestData(i, i.toString))).toDF()
          testData.createOrReplaceTempView("testData")

          // create the table for test
          sql(s"CREATE TABLE table_with_partition(key int,value string) USING hive " +
              s"PARTITIONED by (ds string) location '${tmpDir.toURI}' ")
          sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='1') " +
              "SELECT key,value FROM testData")
          sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='2') " +
              "SELECT key,value FROM testData")
          sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='3') " +
              "SELECT key,value FROM testData")
          sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='4') " +
              "SELECT key,value FROM testData")

          // test for the exist path
          checkAnswer(sql("select key,value from table_with_partition"),
            testData.union(testData).union(testData).union(testData))

          // delete the path of one partition
          tmpDir.listFiles
              .find { f => f.isDirectory && f.getName().startsWith("ds=") }
              .foreach { f => Utils.deleteRecursively(f) }

          // test for after delete the path
          checkAnswer(sql("select key,value from table_with_partition"),
            testData.union(testData).union(testData))
        }
      }
    }
  }

  test("Replace spark.sql.hive.verifyPartitionPath by spark.sql.files.ignoreMissingFiles") {
    withSQLConf(SQLConf.IGNORE_MISSING_FILES.key -> "true") {
      queryWhenPathNotExist()
    }
  }

  test("SPARK-21739: Cast expression should initialize timezoneId") {
    withTable("table_with_timestamp_partition") {
      sql("CREATE TABLE table_with_timestamp_partition(value int) USING hive " +
        "PARTITIONED BY (ts TIMESTAMP)")
      sql("INSERT OVERWRITE TABLE table_with_timestamp_partition " +
        "PARTITION (ts = '2010-01-01 00:00:00.000') VALUES (1)")

      // test for Cast expression in TableReader
      checkAnswer(sql("SELECT * FROM table_with_timestamp_partition"),
        Seq(Row(1, Timestamp.valueOf("2010-01-01 00:00:00.000"))))

      // test for Cast expression in HiveTableScanExec
      checkAnswer(sql("SELECT value FROM table_with_timestamp_partition " +
        "WHERE ts = '2010-01-01 00:00:00.000'"), Row(1))
    }
  }
}
