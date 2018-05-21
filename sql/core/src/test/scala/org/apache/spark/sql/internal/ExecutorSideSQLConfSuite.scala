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

package org.apache.spark.sql.internal

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SQLTestUtils

class ExecutorSideSQLConfSuite extends SparkFunSuite with SQLTestUtils {
  import testImplicits._

  protected var spark: SparkSession = null

  // Create a new [[SparkSession]] running in local-cluster mode.
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local-cluster[2,1,1024]")
      .appName("testing")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
    spark = null
  }

  test("ReadOnlySQLConf is correctly created at the executor side") {
    SQLConf.get.setConfString("spark.sql.x", "a")
    try {
      val checks = spark.range(10).mapPartitions { it =>
        val conf = SQLConf.get
        Iterator(conf.isInstanceOf[ReadOnlySQLConf] && conf.getConfString("spark.sql.x") == "a")
      }.collect()
      assert(checks.forall(_ == true))
    } finally {
      SQLConf.get.unsetConf("spark.sql.x")
    }
  }

  test("case-sensitive config should work for json schema inference") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempPath { path =>
        val pathString = path.getCanonicalPath
        spark.range(10).select('id.as("ID")).write.json(pathString)
        spark.range(10).write.mode("append").json(pathString)
        assert(spark.read.json(pathString).columns.toSet == Set("id", "ID"))
      }
    }
  }
}
