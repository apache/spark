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
import org.apache.spark.sql.types.CharType

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

  // TODO(SPARK-34203): Move this too super class when the ticket gets fixed
  test("char type values should be padded or trimmed: static partitioned columns") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c CHAR(5)) USING $format PARTITIONED BY (c)")
      (0 to 5).map(n => "a" + " " * n).foreach { v =>
        sql(s"INSERT INTO t PARTITION (c ='$v') VALUES ('1')")
        checkAnswer(spark.table("t"), Row("1", "a" + " " * 4))
        checkColType(spark.table("t").schema(1), CharType(5))
        sql(s"ALTER TABLE t DROP PARTITION(c='$v')")
        checkAnswer(spark.table("t"), Nil)
      }
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
