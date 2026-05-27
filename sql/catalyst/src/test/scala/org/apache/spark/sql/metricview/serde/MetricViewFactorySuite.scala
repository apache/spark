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

package org.apache.spark.sql.metricview.serde

import org.apache.spark.SparkFunSuite

/**
 * Test suite for [[MetricViewFactory]] YAML serialization and deserialization.
 */
class MetricViewFactorySuite extends SparkFunSuite {

  test("fromYAML - parse basic metric view with asset source") {
    val yaml =
      """version: "0.1"
        |source: my_table
        |dimensions:
        |  - name: customer_id
        |    expr: customer_id
        |measures:
        |  - name: total_revenue
        |    expr: SUM(revenue)
        |""".stripMargin

    val metricView = MetricViewFactory.fromYAML(yaml)

    assert(metricView.version === "0.1")
    assert(metricView.from.isInstanceOf[AssetSource])
    assert(metricView.from.asInstanceOf[AssetSource].name === "my_table")
    assert(metricView.where.isEmpty)
    assert(metricView.select.length === 2)

    val customerIdCol = metricView.select(0)
    assert(customerIdCol.name === "customer_id")
    assert(customerIdCol.expression.isInstanceOf[DimensionExpression])
    assert(customerIdCol.expression.expr === "customer_id")

    val revenueCol = metricView.select(1)
    assert(revenueCol.name === "total_revenue")
    assert(revenueCol.expression.isInstanceOf[MeasureExpression])
    assert(revenueCol.expression.expr === "SUM(revenue)")
  }

  test("fromYAML - parse metric view with SQL source") {
    val yaml =
      """version: "0.1"
        |source: SELECT * FROM my_table WHERE year = 2024
        |dimensions:
        |  - name: product_id
        |    expr: product_id
        |measures:
        |  - name: quantity_sold
        |    expr: SUM(quantity)
        |""".stripMargin

    val metricView = MetricViewFactory.fromYAML(yaml)

    assert(metricView.from.isInstanceOf[SQLSource])
    assert(metricView.from.asInstanceOf[SQLSource].sql ===
      "SELECT * FROM my_table WHERE year = 2024")
    assert(metricView.select.length === 2)
  }

  test("fromYAML - parse metric view with filter clause") {
    val yaml =
      """version: "0.1"
        |source: sales_data
        |filter: year >= 2020 AND status = 'completed'
        |dimensions:
        |  - name: region
        |    expr: region
        |measures:
        |  - name: sales
        |    expr: SUM(amount)
        |""".stripMargin

    val metricView = MetricViewFactory.fromYAML(yaml)

    assert(metricView.where.isDefined)
    assert(metricView.where.get === "year >= 2020 AND status = 'completed'")
  }

  test("fromYAML - parse metric view with multiple dimensions and measures") {
    val yaml =
      """version: "0.1"
        |source: transactions
        |dimensions:
        |  - name: customer_id
        |    expr: customer_id
        |  - name: product_id
        |    expr: product_id
        |  - name: region
        |    expr: region
        |measures:
        |  - name: total_revenue
        |    expr: SUM(revenue)
        |  - name: avg_revenue
        |    expr: AVG(revenue)
        |  - name: transaction_count
        |    expr: COUNT(*)
        |""".stripMargin

    val metricView = MetricViewFactory.fromYAML(yaml)

    assert(metricView.select.length === 6)

    // Check dimensions
    assert(metricView.select(0).expression.isInstanceOf[DimensionExpression])
    assert(metricView.select(1).expression.isInstanceOf[DimensionExpression])
    assert(metricView.select(2).expression.isInstanceOf[DimensionExpression])

    // Check measures
    assert(metricView.select(3).expression.isInstanceOf[MeasureExpression])
    assert(metricView.select(4).expression.isInstanceOf[MeasureExpression])
    assert(metricView.select(5).expression.isInstanceOf[MeasureExpression])
  }

  test("fromYAML - invalid YAML version") {
    val yaml =
      """version: "99.9"
        |source: my_table
        |dimensions:
        |  - name: id
        |    expr: id
        |""".stripMargin

    val exception = intercept[MetricViewValidationException] {
      MetricViewFactory.fromYAML(yaml)
    }
    assert(exception.getMessage.contains("Invalid YAML version: 99.9"))
  }

  test("fromYAML - malformed YAML") {
    val yaml = """this is not valid yaml: [unclosed bracket"""

    val exception = intercept[MetricViewYAMLParsingException] {
      MetricViewFactory.fromYAML(yaml)
    }
    assert(exception.getMessage.contains("Failed to parse YAML"))
  }

  test("toYAML - serialize basic metric view") {
    val metricView = MetricView(
      version = "0.1",
      from = AssetSource("my_table"),
      where = None,
      select = Seq(
        Column("customer_id", DimensionExpression("customer_id"), 0),
        Column("total_revenue", MeasureExpression("SUM(revenue)"), 1)
      )
    )

    val yaml = MetricViewFactory.toYAML(metricView)

    assert(yaml.contains("version: 0.1") || yaml.contains("version: \"0.1\""))
    assert(yaml.contains("source: my_table"))
    assert(yaml.contains("customer_id"))
    assert(yaml.contains("total_revenue"))

    // Verify it can be parsed back
    val reparsed = MetricViewFactory.fromYAML(yaml)
    assert(reparsed.version === "0.1")
    assert(reparsed.from.asInstanceOf[AssetSource].name === "my_table")
  }

  test("toYAML - serialize metric view with SQL source") {
    val metricView = MetricView(
      version = "0.1",
      from = SQLSource("SELECT * FROM table WHERE id > 100"),
      where = None,
      select = Seq(
        Column("id", DimensionExpression("id"), 0)
      )
    )

    val yaml = MetricViewFactory.toYAML(metricView)

    assert(yaml.contains("source: SELECT * FROM table WHERE id > 100"))
  }

  test("toYAML - serialize metric view with filter clause") {
    val metricView = MetricView(
      version = "0.1",
      from = AssetSource("sales"),
      where = Some("year >= 2020"),
      select = Seq(
        Column("region", DimensionExpression("region"), 0),
        Column("sales", MeasureExpression("SUM(amount)"), 1)
      )
    )

    val yaml = MetricViewFactory.toYAML(metricView)

    assert(yaml.contains("year >= 2020"))

    // Verify it can be parsed back
    val reparsed = MetricViewFactory.fromYAML(yaml)
    assert(reparsed.where.isDefined)
    assert(reparsed.where.get === "year >= 2020")
  }

  test("roundtrip - fromYAML and toYAML preserve data") {
    val originalYaml =
      """version: "0.1"
        |source: sales_table
        |filter: status = 'active'
        |dimensions:
        |  - name: customer_id
        |    expr: customer_id
        |  - name: product_name
        |    expr: product_name
        |measures:
        |  - name: total_revenue
        |    expr: SUM(revenue)
        |  - name: order_count
        |    expr: COUNT(order_id)
        |""".stripMargin

    // Parse the YAML
    val metricView = MetricViewFactory.fromYAML(originalYaml)

    // Serialize it back
    val serializedYaml = MetricViewFactory.toYAML(metricView)

    // Parse again to verify
    val reparsedMetricView = MetricViewFactory.fromYAML(serializedYaml)

    // Verify all fields match
    assert(reparsedMetricView.version === metricView.version)
    assert(reparsedMetricView.from === metricView.from)
    assert(reparsedMetricView.where === metricView.where)
    assert(reparsedMetricView.select.length === metricView.select.length)

    reparsedMetricView.select.zip(metricView.select).foreach { case (col1, col2) =>
      assert(col1.name === col2.name)
      assert(col1.expression.expr === col2.expression.expr)
      assert(col1.expression.getClass === col2.expression.getClass)
    }
  }

  test("roundtrip - SQL source preservation") {
    val originalYaml =
      """version: "0.1"
        |source: SELECT * FROM my_table WHERE year = 2024
        |dimensions:
        |  - name: id
        |    expr: id
        |measures:
        |  - name: total
        |    expr: SUM(value)
        |""".stripMargin

    val metricView = MetricViewFactory.fromYAML(originalYaml)
    val serializedYaml = MetricViewFactory.toYAML(metricView)
    val reparsedMetricView = MetricViewFactory.fromYAML(serializedYaml)

    assert(reparsedMetricView.from.isInstanceOf[SQLSource])
    assert(reparsedMetricView.from.asInstanceOf[SQLSource].sql ===
      "SELECT * FROM my_table WHERE year = 2024")
  }

  test("column ordinals are preserved") {
    val yaml =
      """version: "0.1"
        |source: my_table
        |dimensions:
        |  - name: col1
        |    expr: col1
        |  - name: col2
        |    expr: col2
        |measures:
        |  - name: col3
        |    expr: SUM(value)
        |""".stripMargin

    val metricView = MetricViewFactory.fromYAML(yaml)

    assert(metricView.select(0).ordinal === 0)
    assert(metricView.select(1).ordinal === 1)
    assert(metricView.select(2).ordinal === 2)
  }

  test("column metadata extraction") {
    val yaml =
      """version: "0.1"
        |source: my_table
        |dimensions:
        |  - name: customer_id
        |    expr: customer_id
        |measures:
        |  - name: revenue
        |    expr: SUM(amount)
        |""".stripMargin

    val metricView = MetricViewFactory.fromYAML(yaml)

    val dimensionMetadata = metricView.select(0).getColumnMetadata
    assert(dimensionMetadata.columnType === "dimension")
    assert(dimensionMetadata.expr === "customer_id")

    val measureMetadata = metricView.select(1).getColumnMetadata
    assert(measureMetadata.columnType === "measure")
    assert(measureMetadata.expr === "SUM(amount)")
  }

  test("empty source validation") {
    val yaml =
      """version: "0.1"
        |source: ""
        |dimensions:
        |  - name: id
        |    expr: id
        |""".stripMargin

    intercept[Exception] {
      MetricViewFactory.fromYAML(yaml)
    }
  }

  test("complex SQL expressions in measures") {
    val yaml =
      """version: "0.1"
        |source: transactions
        |dimensions:
        |  - name: date
        |    expr: DATE(timestamp)
        |measures:
        |  - name: weighted_avg
        |    expr: SUM(amount * weight) / SUM(weight)
        |  - name: distinct_customers
        |    expr: COUNT(DISTINCT customer_id)
        |""".stripMargin

    val metricView = MetricViewFactory.fromYAML(yaml)

    assert(metricView.select.length === 3)
    assert(metricView.select(0).expression.expr === "DATE(timestamp)")
    assert(metricView.select(1).expression.expr === "SUM(amount * weight) / SUM(weight)")
    assert(metricView.select(2).expression.expr === "COUNT(DISTINCT customer_id)")
  }

  test("special characters in column names and expressions") {
    val yaml =
      """version: "0.1"
        |source: my_table
        |dimensions:
        |  - name: "customer.id"
        |    expr: "`customer.id`"
        |measures:
        |  - name: "revenue_$"
        |    expr: "SUM(`revenue_$`)"
        |""".stripMargin

    val metricView = MetricViewFactory.fromYAML(yaml)

    assert(metricView.select(0).name === "customer.id")
    assert(metricView.select(1).name === "revenue_$")
  }
}
