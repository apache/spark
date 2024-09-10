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

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.SQLInsertTestSuite
import org.apache.spark.sql.hive.test.TestHiveSingleton

class HiveSQLInsertTestSuite extends SQLInsertTestSuite with TestHiveSingleton {

  private val originalPartitionMode = spark.conf.getOption("hive.exec.dynamic.partition.mode")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  }

  override protected def afterAll(): Unit = {
    originalPartitionMode
      .map(v => spark.conf.set("hive.exec.dynamic.partition.mode", v))
      .getOrElse(spark.conf.unset("hive.exec.dynamic.partition.mode"))
    super.afterAll()
  }

  override def format: String = "hive OPTIONS(fileFormat='parquet')"

  override def checkV1AndV2Error(
      exception: SparkThrowable,
      v1ErrorClass: String,
      v2ErrorClass: String,
      v1Parameters: Map[String, String],
      v2Parameters: Map[String, String]): Unit = {
    checkError(exception = exception, sqlState = None, condition = v1ErrorClass,
      parameters = v1Parameters)
  }
}
