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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.QueryContext
import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{CreateStreamingTable, TableSpec}
import org.apache.spark.sql.connector.expressions.{ClusterByTransform, FieldReference, IdentityTransform}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command.v1.CommandSuiteBase
import org.apache.spark.sql.types.IntegerType

/**
 * The class contains tests for the `CREATE STREAMING TABLE ... AS ...` command
 */
class CreateStreamingTableParserSuite extends CommandSuiteBase {
  protected lazy val parser = new SparkSqlParser()

  /** Turn an actual QueryContext into an equivalent ExpectedContext to no-op the check. */
  private def toExpectedContext(actual: QueryContext): ExpectedContext = ExpectedContext(
    objectType = actual.objectType(),
    objectName = actual.objectName(),
    startIndex = actual.startIndex(),
    stopIndex = actual.stopIndex(),
    fragment = actual.fragment()
  )

  test("CREATE STREAMING TABLE without subquery is parsed correctly") {
    comparePlans(
      parser
        .parsePlan("CREATE STREAMING TABLE st COMMENT 'populate with flow later'"),
      CreateStreamingTable(
        name = UnresolvedIdentifier(
          nameParts = Seq("st")
        ),
        columns = Seq.empty,
        partitioning = Seq.empty,
        tableSpec = TableSpec(
          properties = Map.empty,
          provider = None,
          options = Map.empty,
          location = None,
          comment = Option("populate with flow later"),
          collation = None,
          serde = None,
          external = false,
          constraints = Seq.empty
        ),
        ifNotExists = false
      ),
      checkAnalysis = false
    )
  }

  test("CREATE STREAMING TABLE - PARTITIONED BY is honored") {
    val cmd = parser
      .parsePlan("CREATE STREAMING TABLE st PARTITIONED BY (a)")
      .asInstanceOf[CreateStreamingTable]
    assert(cmd.partitioning == Seq(IdentityTransform(FieldReference(Seq("a")))))
  }

  test("CREATE STREAMING TABLE - COMMENT is honored") {
    val cmd = parser
      .parsePlan("CREATE STREAMING TABLE st COMMENT 'my streaming table'")
      .asInstanceOf[CreateStreamingTable]
    assert(cmd.tableSpec.comment == Some("my streaming table"))
  }

  test("CREATE STREAMING TABLE - TBLPROPERTIES are honored") {
    val cmd = parser
      .parsePlan("CREATE STREAMING TABLE st TBLPROPERTIES ('key' = 'value', 'num' = '1')")
      .asInstanceOf[CreateStreamingTable]
    assert(cmd.tableSpec.properties == Map("key" -> "value", "num" -> "1"))
  }

  test("CREATE STREAMING TABLE - CLUSTER BY is honored") {
    val cmd = parser
      .parsePlan("CREATE STREAMING TABLE st CLUSTER BY (a)")
      .asInstanceOf[CreateStreamingTable]
    assert(cmd.partitioning == Seq(ClusterByTransform(Seq(FieldReference(Seq("a"))))))
  }

  test("CREATE STREAMING TABLE - USING provider is honored") {
    val cmd = parser
      .parsePlan("CREATE STREAMING TABLE st USING parquet")
      .asInstanceOf[CreateStreamingTable]
    assert(cmd.tableSpec.provider == Some("parquet"))
  }

  test("CREATE STREAMING TABLE - DEFAULT COLLATION is honored") {
    val cmd = parser
      .parsePlan("CREATE STREAMING TABLE st DEFAULT COLLATION UTF8_LCASE")
      .asInstanceOf[CreateStreamingTable]
    assert(cmd.tableSpec.collation == Some("UTF8_LCASE"))
  }

  test("CREATE STREAMING TABLE - column list is honored") {
    val cmd = parser
      .parsePlan("CREATE STREAMING TABLE st (id INT, name STRING)")
      .asInstanceOf[CreateStreamingTable]
    assert(cmd.columns.map(_.name) == Seq("id", "name"))
    assert(cmd.columns.head.dataType == IntegerType)
  }

  test("CREATE STREAMING TABLE - IF NOT EXISTS is honored") {
    Seq(
      ("CREATE STREAMING TABLE IF NOT EXISTS st", true),
      ("CREATE STREAMING TABLE st", false)
    ).foreach { case (query, ifNotExists) =>
      val cmd = parser.parsePlan(query).asInstanceOf[CreateStreamingTable]
      assert(cmd.ifNotExists == ifNotExists)
    }
  }

  test("CREATE STREAMING TABLE - PARTITIONED BY, COMMENT, TBLPROPERTIES combined") {
    val cmd = parser
      .parsePlan(
        """CREATE STREAMING TABLE st
          |PARTITIONED BY (a)
          |COMMENT 'my streaming table'
          |TBLPROPERTIES ('key' = 'value')""".stripMargin)
      .asInstanceOf[CreateStreamingTable]
    assert(cmd.partitioning == Seq(IdentityTransform(FieldReference(Seq("a")))))
    assert(cmd.tableSpec.comment == Some("my streaming table"))
    assert(cmd.tableSpec.properties == Map("key" -> "value"))
  }

  // ---------------------------------------------------------------------------
  // Error cases: unsupported table features
  // ---------------------------------------------------------------------------

  test("CREATE STREAMING TABLE - column constraints are unsupported") {
    val ex = intercept[ParseException] {
      parser.parsePlan("CREATE STREAMING TABLE st (id INT PRIMARY KEY)")
    }
    checkError(
      exception = ex,
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" ->
        ("Pipeline datasets do not currently support column constraints. " +
          "Please remove any CHECK, UNIQUE, PK, and FK constraints " +
          "specified on the pipeline dataset.")),
      queryContext = ex.getQueryContext.map(toExpectedContext)
    )
  }

  test("CREATE STREAMING TABLE - bucketing is unsupported") {
    val ex = intercept[ParseException] {
      parser.parsePlan("CREATE STREAMING TABLE st CLUSTERED BY (id) INTO 4 BUCKETS")
    }
    checkError(
      exception = ex,
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" ->
        ("Bucketing is not supported for CREATE STREAMING TABLE statements. " +
          "Please remove any bucket spec specified in the statement.")),
      queryContext = ex.getQueryContext.map(toExpectedContext)
    )
  }

  test("CREATE STREAMING TABLE - options are unsupported") {
    val ex = intercept[ParseException] {
      parser.parsePlan("CREATE STREAMING TABLE st OPTIONS (key = 'value')")
    }
    checkError(
      exception = ex,
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" ->
        ("Options are not supported for CREATE STREAMING TABLE statements. " +
          "Please remove any OPTIONS lists specified in the statement.")),
      queryContext = ex.getQueryContext.map(toExpectedContext)
    )
  }

  test("CREATE STREAMING TABLE - location is unsupported") {
    val ex = intercept[ParseException] {
      parser.parsePlan("CREATE STREAMING TABLE st LOCATION '/tmp/data'")
    }
    checkError(
      exception = ex,
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" ->
        ("Specifying location is not supported for CREATE STREAMING TABLE statements. " +
          "The storage location for a pipeline dataset is managed by the pipeline itself.")),
      queryContext = ex.getQueryContext.map(toExpectedContext)
    )
  }

  test("CREATE STREAMING TABLE - STORED AS syntax is unsupported") {
    val ex = intercept[ParseException] {
      parser.parsePlan("CREATE STREAMING TABLE st STORED AS TEXTFILE")
    }
    checkError(
      exception = ex,
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" ->
        ("The STORED AS syntax is not supported for CREATE STREAMING TABLE statements. " +
          "Consider using the Data Source based USING clause instead.")),
      queryContext = ex.getQueryContext.map(toExpectedContext)
    )
  }

  test("CREATE STREAMING TABLE - SerDe information is unsupported") {
    val ex = intercept[ParseException] {
      parser.parsePlan(
        """CREATE STREAMING TABLE st
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'""".stripMargin)
    }
    checkError(
      exception = ex,
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" ->
        ("Hive SerDe format options are not supported for " +
          "CREATE STREAMING TABLE statements.")),
      queryContext = ex.getQueryContext.map(toExpectedContext)
    )
  }
}
