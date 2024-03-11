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
package org.apache.spark.sql.execution.datasources

import java.util.Collections

import org.scalatest.BeforeAndAfter
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryTableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class InMemoryTableMetricSuite
  extends QueryTest with SharedSparkSession with BeforeAndAfter {
  import testImplicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
  }

  private def testMetricOnDSv2(func: String => Unit, checker: Map[Long, String] => Unit): Unit = {
    withTable("testcat.table_name") {
      val statusStore = spark.sharedState.statusStore
      val oldCount = statusStore.executionsList().size

      val testCatalog = spark.sessionState.catalogManager.catalog("testcat").asTableCatalog

      testCatalog.createTable(
        Identifier.of(Array(), "table_name"),
        Array(Column.create("i", IntegerType)),
        Array.empty[Transform], Collections.emptyMap[String, String])

      func("testcat.table_name")

      // Wait until the new execution is started and being tracked.
      eventually(timeout(10.seconds), interval(10.milliseconds)) {
        assert(statusStore.executionsCount() >= oldCount)
      }

      // Wait for listener to finish computing the metrics for the execution.
      eventually(timeout(10.seconds), interval(10.milliseconds)) {
        assert(statusStore.executionsList().nonEmpty &&
          statusStore.executionsList().last.metricValues != null)
      }

      val execId = statusStore.executionsList().last.executionId
      val metrics = statusStore.executionMetrics(execId)
      checker(metrics)
    }
  }

  test("Report metrics from Datasource v2 write: append") {
    testMetricOnDSv2(table => {
      val df = sql("select 1 as i")
      val v2Writer = df.writeTo(table)
      v2Writer.append()
    }, metrics => {
      val customMetric = metrics.find(_._2 == "in-memory rows: 1")
      assert(customMetric.isDefined)
    })
  }

  test("Report metrics from Datasource v2 write: overwrite") {
    testMetricOnDSv2(table => {
      val df = Seq(1, 2, 3).toDF("i")
      val v2Writer = df.writeTo(table)
      v2Writer.overwrite(lit(true))
    }, metrics => {
      val customMetric = metrics.find(_._2 == "in-memory rows: 3")
      assert(customMetric.isDefined)
    })
  }
}
