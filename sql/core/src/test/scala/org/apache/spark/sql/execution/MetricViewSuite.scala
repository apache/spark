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

package org.apache.spark.sql.execution

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.metricview.serde.{AssetSource, Column, DimensionExpression, MeasureExpression, MetricView, MetricViewFactory, SQLSource}
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

class SimpleMetricViewSuite extends MetricViewSuite with SharedSparkSession

/**
 * A suite for testing metric view related functionality.
 */
abstract class MetricViewSuite extends QueryTest with SQLTestUtils {
  import testImplicits._

  protected val testMetricViewName = "test_metric_view"
  protected val testTableName = "test_table"
  protected val testTableData = Seq(
    ("region_1", "product_1", 80, 5.0),
    ("region_1", "product_2", 70, 10.0),
    ("REGION_1", "product_3", 60, 15.0),
    ("REGION_1", "product_4", 50, 20.0),
    ("region_2", "product_1", 40, 25.0),
    ("region_2", "product_2", 30, 30.0),
    ("REGION_2", "product_3", 20, 35.0),
    ("REGION_2", "product_4", 10, 40.0)
  )
  protected val testMetricViewColumns = Seq(
    Column("region", DimensionExpression("region"), 0),
    Column("product", DimensionExpression("product"), 1),
    Column("region_upper", DimensionExpression("upper(region)"), 2),
    Column("count_sum", MeasureExpression("sum(count)"), 3),
    Column("price_avg", MeasureExpression("avg(price)"), 4)
  )

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    testTableData
      .toDF("region", "product", "count", "price")
      .write
      .saveAsTable(testTableName)
  }

  protected def createMetricView(
      metricViewName: String,
      metricViewDefinition: MetricView): Unit = {
    val yaml = MetricViewFactory.toYAML(metricViewDefinition)
    sql(s"""
        |CREATE VIEW $metricViewName
        |WITH METRICS
        |LANGUAGE YAML
        |AS
        |$$$$
        |$yaml
        |$$$$
        |""".stripMargin)
  }

  protected def withMetricView(
      viewName: String,
      metricViewDefinition: MetricView)(body: => Unit): Unit = {
    createMetricView(viewName, metricViewDefinition)
    withView(viewName) {
      body
    }
  }

  test("test source type") {
    val sources = Seq(
      AssetSource(testTableName),
      SQLSource("SELECT * FROM test_table")
    )
    sources.foreach { source =>
      val metricView = MetricView("0.1", source, None, testMetricViewColumns)
      withMetricView(testMetricViewName, metricView) {
        checkAnswer(
          sql("SELECT measure(count_sum), measure(price_avg) FROM test_metric_view"),
          sql("SELECT sum(count), avg(price) FROM test_table")
        )
        checkAnswer(
          sql("SELECT measure(count_sum), measure(price_avg) " +
            "FROM test_metric_view WHERE region_upper = 'REGION_1'"),
          sql("SELECT sum(count), avg(price) FROM test_table WHERE upper(region) = 'REGION_1'")
        )
      }
    }
  }

  test("test where clause") {
    val metricView = MetricView(
      "0.1", AssetSource(testTableName),
      Some("product = 'product_1'"), testMetricViewColumns)
    withMetricView(testMetricViewName, metricView) {
      checkAnswer(
        sql("SELECT measure(count_sum), measure(price_avg) FROM test_metric_view"),
        sql("SELECT sum(count), avg(price) FROM test_table WHERE product = 'product_1'")
      )
      checkAnswer(
        sql("SELECT measure(count_sum), measure(price_avg) " +
          "FROM test_metric_view WHERE region_upper = 'REGION_1'"),
        sql("SELECT sum(count), avg(price) FROM test_table WHERE " +
          "product = 'product_1' AND upper(region) = 'REGION_1'")
      )
    }
  }

  test("test dimensions and measures") {
    val metricView = MetricView(
      "0.1", AssetSource(testTableName), None, testMetricViewColumns)
    withMetricView(testMetricViewName, metricView) {
      // dimension and measure
      checkAnswer(
        sql("SELECT region, product, measure(count_sum), measure(price_avg) " +
          "FROM test_metric_view GROUP BY region, product"),
        sql("SELECT region, product, sum(count), avg(price) " +
          "FROM test_table GROUP BY region, product")
      )
      // dimension only
      checkAnswer(
        sql("SELECT region_upper FROM test_metric_view GROUP BY 1"),
        sql("SELECT upper(region) FROM test_table GROUP BY 1")
      )
      // measure only
      checkAnswer(
        sql("SELECT measure(count_sum) FROM test_metric_view"),
        sql("SELECT sum(count) FROM test_table")
      )
    }
  }

  test("column from source cannot be used when query metric view") {
    val metricView = MetricView("0.1", AssetSource(testTableName), None, testMetricViewColumns)
    withMetricView(testMetricViewName, metricView) {
      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT sum(count) FROM test_metric_view").collect()
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map(
          "objectName" -> "`count`",
          "proposal" -> "`count_sum`, `product`, `region`, `price_avg`, `region_upper`"
        ),
        queryContext = Array(ExpectedContext(
          fragment = "count",
          start = 11,
          stop = 15
        ))
      )
    }
  }
}
