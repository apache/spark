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

import java.net.URL

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for [[org.apache.spark.sql.internal.SharedState]].
 */
class SharedStateSuite extends SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.hadoop.fs.defaultFS", "file:///")
  }

  test("SPARK-31692: Url handler factory should have the hadoop configs from Spark conf") {
    // Accessing shared state to init the object since it is `lazy val`
    spark.sharedState
    val field = classOf[URL].getDeclaredField("factory")
    field.setAccessible(true)
    val value = field.get(null)
    assert(value.isInstanceOf[FsUrlStreamHandlerFactory])
    val streamFactory = value.asInstanceOf[FsUrlStreamHandlerFactory]

    val confField = classOf[FsUrlStreamHandlerFactory].getDeclaredField("conf")
    confField.setAccessible(true)
    val conf = confField.get(streamFactory)

    assert(conf.isInstanceOf[Configuration])
    assert(conf.asInstanceOf[Configuration].get("fs.defaultFS") == "file:///")
  }

  test("Default database does not exist") {
    SQLConf.get.setConfString("spark.sql.catalog.spark_catalog.defaultDatabase",
      "default_database_not_exists")

    checkError(
      exception = intercept[SparkException] {
        spark.sharedState.externalCatalog
      },
      condition = "DEFAULT_DATABASE_NOT_EXISTS",
      parameters = Map("defaultDatabase" -> "default_database_not_exists")
    )

    SQLConf.get.setConfString("spark.sql.catalog.spark_catalog.defaultDatabase",
      SessionCatalog.DEFAULT_DATABASE)
  }
}
