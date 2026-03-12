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

  test("test ORDER BY and LIMIT clauses") {
    val metricView = MetricView(
      "0.1", AssetSource(testTableName), None, testMetricViewColumns)
    withMetricView(testMetricViewName, metricView) {
      checkAnswer(
        sql("SELECT region, measure(count_sum) " +
          "FROM test_metric_view GROUP BY region ORDER BY 2 DESC"),
        sql("SELECT region, sum(count) " +
          "FROM test_table GROUP BY region ORDER BY 2 DESC")
      )
      checkAnswer(
        sql("SELECT product, measure(price_avg) " +
          "FROM test_metric_view GROUP BY product ORDER BY 2 ASC LIMIT 2"),
        sql("SELECT product, avg(price) " +
          "FROM test_table GROUP BY product ORDER BY 2 ASC LIMIT 2")
      )
    }
  }

  test("test complex WHERE conditions with dimensions") {
    val metricView = MetricView(
      "0.1", AssetSource(testTableName), None, testMetricViewColumns)
    withMetricView(testMetricViewName, metricView) {
      checkAnswer(
        sql("SELECT region, product, measure(count_sum) " +
          "FROM test_metric_view WHERE region_upper = 'REGION_1' " +
          "AND product IN ('product_1', 'product_2') " +
          "GROUP BY region, product"),
        sql("SELECT region, product, sum(count) " +
          "FROM test_table WHERE upper(region) = 'REGION_1' " +
          "AND product IN ('product_1', 'product_2') " +
          "GROUP BY region, product")
      )
      checkAnswer(
        sql("SELECT measure(count_sum), measure(price_avg) " +
          "FROM test_metric_view WHERE region_upper LIKE 'REGION_%' AND product <> 'product_4'"),
        sql("SELECT sum(count), avg(price) " +
          "FROM test_table WHERE upper(region) LIKE 'REGION_%' AND product <> 'product_4'")
      )
    }
  }

  test("test metric view with where clause and additional query filters") {
    val metricView = MetricView(
      "0.1", AssetSource(testTableName),
      Some("product IN ('product_1', 'product_2')"), testMetricViewColumns)
    withMetricView(testMetricViewName, metricView) {
      checkAnswer(
        sql("SELECT region, measure(count_sum) " +
          "FROM test_metric_view WHERE region_upper = 'REGION_1' GROUP BY region"),
        sql("SELECT region, sum(count) " +
          "FROM test_table WHERE product IN ('product_1', 'product_2') " +
          "AND upper(region) = 'REGION_1' GROUP BY region")
      )
    }
  }

  test("test multiple measures with different aggregations") {
    val columns = Seq(
      Column("region", DimensionExpression("region"), 0),
      Column("count_sum", MeasureExpression("sum(count)"), 1),
      Column("count_avg", MeasureExpression("avg(count)"), 2),
      Column("count_max", MeasureExpression("max(count)"), 3),
      Column("count_min", MeasureExpression("min(count)"), 4),
      Column("price_sum", MeasureExpression("sum(price)"), 5)
    )
    val metricView = MetricView("0.1", AssetSource(testTableName), None, columns)
    withMetricView(testMetricViewName, metricView) {
      checkAnswer(
        sql("SELECT measure(count_sum), measure(count_avg), measure(count_max), " +
          "measure(count_min), measure(price_sum) FROM test_metric_view"),
        sql("SELECT sum(count), avg(count), max(count), min(count), sum(price) FROM test_table")
      )
      checkAnswer(
        sql("SELECT region, measure(count_sum), measure(count_max), measure(price_sum) " +
          "FROM test_metric_view GROUP BY region"),
        sql("SELECT region, sum(count), max(count), sum(price) FROM test_table GROUP BY region")
      )
    }
  }

  test("test dimension expressions with case statements") {
    val columns = Seq(
      Column("region", DimensionExpression("region"), 0),
      Column("region_category", DimensionExpression(
        "CASE WHEN region = 'region_1' THEN 'Group A' ELSE 'Group B' END"), 1),
      Column("count_sum", MeasureExpression("sum(count)"), 2)
    )
    val metricView = MetricView("0.1", AssetSource(testTableName), None, columns)
    withMetricView(testMetricViewName, metricView) {
      checkAnswer(
        sql("SELECT region_category, measure(count_sum) " +
          "FROM test_metric_view GROUP BY region_category"),
        sql("SELECT CASE WHEN region = 'region_1' THEN 'Group A' ELSE 'Group B' END, " +
          "sum(count) FROM test_table " +
          "GROUP BY CASE WHEN region = 'region_1' THEN 'Group A' ELSE 'Group B' END")
      )
    }
  }

  test("test measure expressions with arithmetic operations") {
    val columns = Seq(
      Column("region", DimensionExpression("region"), 0),
      Column("total_revenue", MeasureExpression("sum(count * price)"), 1),
      Column("avg_revenue", MeasureExpression("avg(count * price)"), 2)
    )
    val metricView = MetricView("0.1", AssetSource(testTableName), None, columns)
    withMetricView(testMetricViewName, metricView) {
      checkAnswer(
        sql("SELECT measure(total_revenue), measure(avg_revenue) FROM test_metric_view"),
        sql("SELECT sum(count * price), avg(count * price) FROM test_table")
      )
      checkAnswer(
        sql("SELECT region, measure(total_revenue) " +
          "FROM test_metric_view GROUP BY region ORDER BY 2 DESC"),
        sql("SELECT region, sum(count * price) " +
          "FROM test_table GROUP BY region ORDER BY 2 DESC")
      )
    }
  }

  test("test dimensions with aggregate functions in GROUP BY") {
    val metricView = MetricView(
      "0.1", AssetSource(testTableName), None, testMetricViewColumns)
    withMetricView(testMetricViewName, metricView) {
      checkAnswer(
        sql("SELECT region_upper, product, measure(count_sum) " +
          "FROM test_metric_view GROUP BY region_upper, product ORDER BY region_upper, product"),
        sql("SELECT upper(region), product, sum(count) " +
          "FROM test_table GROUP BY upper(region), product ORDER BY upper(region), product")
      )
    }
  }

  test("test WHERE clause with OR conditions") {
    val metricView = MetricView(
      "0.1", AssetSource(testTableName), None, testMetricViewColumns)
    withMetricView(testMetricViewName, metricView) {
      checkAnswer(
        sql("SELECT measure(count_sum), measure(price_avg) " +
          "FROM test_metric_view WHERE region = 'region_1' OR product = 'product_1'"),
        sql("SELECT sum(count), avg(price) " +
          "FROM test_table WHERE region = 'region_1' OR product = 'product_1'")
      )
    }
  }

  test("test dimension-only query with multiple dimensions") {
    val metricView = MetricView(
      "0.1", AssetSource(testTableName), None, testMetricViewColumns)
    withMetricView(testMetricViewName, metricView) {
      checkAnswer(
        sql("SELECT region_upper, product " +
          "FROM test_metric_view GROUP BY region_upper, product"),
        sql("SELECT upper(region), product FROM test_table GROUP BY upper(region), product")
      )
    }
  }

  test("test query with SELECT * should fail") {
    val metricView = MetricView(
      "0.1", AssetSource(testTableName), None, testMetricViewColumns)
    withMetricView(testMetricViewName, metricView) {
      intercept[Exception] {
        sql("SELECT * FROM test_metric_view").collect()
      }
    }
  }

  test("test SQLSource with complex query") {
    val sqlSource = SQLSource(
      "SELECT region, product, count, price FROM test_table WHERE count > 20")
    val metricView = MetricView("0.1", sqlSource, None, testMetricViewColumns)
    withMetricView(testMetricViewName, metricView) {
      checkAnswer(
        sql("SELECT measure(count_sum), measure(price_avg) FROM test_metric_view"),
        sql("SELECT sum(count), avg(price) FROM test_table WHERE count > 20")
      )
      checkAnswer(
        sql("SELECT region, measure(count_sum) FROM test_metric_view GROUP BY region"),
        sql("SELECT region, sum(count) FROM test_table WHERE count > 20 GROUP BY region")
      )
    }
  }

  test("test measure function without GROUP BY") {
    val metricView = MetricView(
      "0.1", AssetSource(testTableName), None, testMetricViewColumns)
    withMetricView(testMetricViewName, metricView) {
      checkAnswer(
        sql("SELECT measure(count_sum) FROM test_metric_view"),
        sql("SELECT sum(count) FROM test_table")
      )
      checkAnswer(
        sql("SELECT measure(count_sum), measure(price_avg) FROM test_metric_view"),
        sql("SELECT sum(count), avg(price) FROM test_table")
      )
    }
  }

  test("test combining multiple dimension expressions in WHERE") {
    val metricView = MetricView(
      "0.1", AssetSource(testTableName), None, testMetricViewColumns)
    withMetricView(testMetricViewName, metricView) {
      checkAnswer(
        sql("SELECT product, measure(count_sum) " +
          "FROM test_metric_view WHERE region = 'region_1' AND region_upper = 'REGION_1' " +
          "GROUP BY product"),
        sql("SELECT product, sum(count) " +
          "FROM test_table WHERE region = 'region_1' AND upper(region) = 'REGION_1' " +
          "GROUP BY product")
      )
    }
  }

  test("test measure with COUNT DISTINCT") {
    val columns = Seq(
      Column("region", DimensionExpression("region"), 0),
      Column("product_count", MeasureExpression("count(distinct product)"), 1),
      Column("count_sum", MeasureExpression("sum(count)"), 2)
    )
    val metricView = MetricView("0.1", AssetSource(testTableName), None, columns)
    withMetricView(testMetricViewName, metricView) {
      checkAnswer(
        sql("SELECT region, measure(product_count), measure(count_sum) " +
          "FROM test_metric_view GROUP BY region"),
        sql("SELECT region, count(distinct product), sum(count) " +
          "FROM test_table GROUP BY region")
      )
    }
  }

  test("test union of same aggregated metric view dataframe") {
    val metricView = MetricView(
      "0.1", AssetSource(testTableName), None, testMetricViewColumns)
    withMetricView(testMetricViewName, metricView) {
      // Create a DataFrame with aggregation and groupBy from metric view
      val df = sql(
        s"""SELECT region, measure(count_sum) as total_count, measure(price_avg) as avg_price
           |FROM $testMetricViewName
           |GROUP BY region
           |""".stripMargin)

      // Union the same DataFrame with itself - tests DeduplicateRelations
      val unionDf = df.union(df)

      // Expected result: each region should appear twice with identical values
      val expectedDf = sql(
        """
          |SELECT region, sum(count) as total_count, avg(price) as avg_price
          |FROM test_table
          |GROUP BY region
          |UNION ALL
          |SELECT region, sum(count) as total_count, avg(price) as avg_price
          |FROM test_table
          |GROUP BY region
          |""".stripMargin)

      checkAnswer(unionDf, expectedDf)

      // Verify the result has duplicate rows
      assert(unionDf.count() == df.count() * 2,
        "Union should double the row count")

      // Verify that distinct values are the same as the original
      checkAnswer(unionDf.distinct(), df)
    }
  }
}
