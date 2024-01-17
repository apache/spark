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

package org.apache.spark.sql.execution.joins

import org.apache.spark.SparkEnv
import org.apache.spark.internal.config.CLEAN_BROADCAST_AFTER_EXECUTION_ENABLED
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.test.SQLTestData.{TestData, TestData2}
import org.apache.spark.sql.test.SQLTestUtils

/**
 * Test clean broadcast data after sql execution ends
 * (if we set spark.broadcast.cleanAfterExecution.enabled=true).
 */
class CleanBroadcastAfterExecutionSuite extends QueryTest with SQLTestUtils {

  import testImplicits._

  var spark: SparkSession = null

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local")
      .config(CLEAN_BROADCAST_AFTER_EXECUTION_ENABLED.key, "true")
      .appName("testing")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      spark.stop()
      spark = null
    } finally {
      super.afterAll()
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    System.gc()
  }

  test("clean broadcast data after execution") {
    val testDataDF = spark.sparkContext.parallelize(
      (1 to 3).map(i => TestData(i, i.toString))).toDF()
    testDataDF.createOrReplaceTempView("testData")
    val testData2DF = spark.sparkContext.parallelize(
      TestData2(1, 1) :: TestData2(2, 2) :: TestData2(3, 1) :: Nil, 2).toDF()
    testData2DF.createOrReplaceTempView("testData2")
    checkAnswer(sql("SELECT  /*+ BROADCASTJOIN  */ t1.* " +
      " from testData t1 left join testData2 t2 ON t1.key = t2.a"), testDataDF)
    assert(SparkEnv.get.broadcastManager.getExecutionBroadcastIds().isEmpty)
  }
}
