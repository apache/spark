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

import org.apache.spark.sql.execution.command.CharVarcharDDLTestBase
import org.apache.spark.sql.hive.test.TestHiveSingleton

class HiveCharVarcharTestSuite extends CharVarcharTestSuite with TestHiveSingleton {

  // The default Hive serde doesn't support nested null values.
  override def format: String = "hive OPTIONS(fileFormat='parquet')"

  private var originalPartitionMode = ""

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    originalPartitionMode = spark.conf.get("hive.exec.dynamic.partition.mode", "")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  }

  override protected def afterAll(): Unit = {
    if (originalPartitionMode == "") {
      spark.conf.unset("hive.exec.dynamic.partition.mode")
    } else {
      spark.conf.set("hive.exec.dynamic.partition.mode", originalPartitionMode)
    }
    super.afterAll()
  }

  test("SPARK-33892: SHOW CREATE TABLE AS SERDE w/ char/varchar") {
    withTable("t") {
      sql(s"CREATE TABLE t(v VARCHAR(3), c CHAR(5)) USING $format")
      val rest = sql("SHOW CREATE TABLE t AS SERDE").head().getString(0)
      assert(rest.contains("VARCHAR(3)"))
      assert(rest.contains("CHAR(5)"))
    }
  }

  test("SPARK-35700: Read char/varchar orc table with created and written by external systems") {
    withTable("t") {
      hiveClient.runSqlHive("CREATE TABLE t(c CHAR(5), v VARCHAR(7)) STORED AS ORC")
      hiveClient.runSqlHive("INSERT INTO t VALUES('Spark', 'kyuubi')")
      checkAnswer(sql("SELECT c, v from t"), Row("Spark", "kyuubi"))
      checkAnswer(sql("SELECT v from t where c = 'Spark' and v = 'kyuubi'"), Row("kyuubi"))
    }
  }
}

class HiveCharVarcharDDLTestSuite extends CharVarcharDDLTestBase with TestHiveSingleton {

  // The default Hive serde doesn't support nested null values.
  override def format: String = "hive OPTIONS(fileFormat='parquet')"

  private var originalPartitionMode = ""

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    originalPartitionMode = spark.conf.get("hive.exec.dynamic.partition.mode", "")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  }

  override protected def afterAll(): Unit = {
    if (originalPartitionMode == "") {
      spark.conf.unset("hive.exec.dynamic.partition.mode")
    } else {
      spark.conf.set("hive.exec.dynamic.partition.mode", originalPartitionMode)
    }
    super.afterAll()
  }
}
