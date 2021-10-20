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

package org.apache.spark.sql.sources

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class StagingInsertSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  val stagingDir = Utils.createTempDir()

  override def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.EXEC_STAGING_DIR, stagingDir.getAbsolutePath)

  override def beforeAll(): Unit = {
    super.beforeAll()
    stagingDir.delete()
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(stagingDir)
    } finally {
      super.afterAll()
    }
  }

  test("SPARK-36579: dynamic partition overwrite can use user defined staging dir") {
    withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key ->
      SQLConf.PartitionOverwriteMode.DYNAMIC.toString) {
      withTempDir { d =>
        withTable("t") {
          sql(
            s"""
               |CREATE TABLE t(c1 int, p1 int) USING PARQUET PARTITIONED BY(p1)
               |LOCATION '${d.getAbsolutePath}'
             """.stripMargin)

          val df = Seq((1, 2), (3, 4)).toDF("c1", "p1")
          df.write
            .partitionBy("p1")
            .mode("overwrite")
            .saveAsTable("t")
          checkAnswer(sql("SELECT * FROM t"), df)
          checkAnswer(sql("SELECT * FROM t WHERE p1 = 2"), Row(1, 2) :: Nil)
          checkAnswer(sql("SELECT * FROM t WHERE p1 = 4"), Row(3, 4) :: Nil)
        }
      }
    }
  }
}
