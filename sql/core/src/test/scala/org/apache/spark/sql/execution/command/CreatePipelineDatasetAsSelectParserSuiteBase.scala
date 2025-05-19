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

package org.apache.spark.sql.execution.command

import org.apache.spark.QueryContext
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{ColumnDefinition, CreatePipelineDatasetAsSelect}
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command.v1.CommandSuiteBase
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType, StructField, StructType}

trait CreatePipelineDatasetAsSelectParserSuiteBase extends CommandSuiteBase {
  protected lazy val parser = new SparkSqlParser()
  protected val datasetSqlSyntax: String

  test("Column definitions are correctly parsed") {
    Seq(
      (s"""
          |CREATE $datasetSqlSyntax table1 (
          |  a INT NOT NULL,
          |  b STRUCT<field: STRING, field2: INT> COMMENT "column description comment"
          |)
          |AS SELECT * FROM input
       """.stripMargin,
          Seq(
            ColumnDefinition(
              name = "a",
              dataType = IntegerType,
              nullable = false,
              comment = None,
              defaultValue = None,
              generationExpression = None,
              identityColumnSpec = None,
              metadata = new MetadataBuilder().build()
            ),
            ColumnDefinition(
              name = "b",
              dataType = StructType(
                Seq(
                  StructField("field", StringType, true),
                  StructField("field2", IntegerType, true)
                )
              ),
              nullable = true,
              comment = Some("column description comment"),
              defaultValue = None,
              generationExpression = None,
              identityColumnSpec = None,
              metadata = new MetadataBuilder().build()
            )
          )
      ),
      (s"CREATE $datasetSqlSyntax table1 AS SELECT * FROM input", Seq.empty)
    ).foreach { case (query, columnDefs) =>
      val plan = parser.parsePlan(query)
      val cmd = plan.asInstanceOf[CreatePipelineDatasetAsSelect]
      assert(cmd.columns == columnDefs)
    }
  }

  test("Partitioning is correctly parsed") {
    Seq(
      (s"CREATE $datasetSqlSyntax table1 PARTITIONED BY (a) AS SELECT * FROM input",
        Seq(IdentityTransform(FieldReference(Seq("a"))))),
      (s"CREATE $datasetSqlSyntax table1 AS SELECT * FROM input", Seq.empty)
    ).foreach { case (query, partitioning) =>
      val plan = parser.parsePlan(query)
      val cmd = plan.asInstanceOf[CreatePipelineDatasetAsSelect]
      assert(cmd.partitioning == partitioning)
    }
  }

  test("Location is unsupported") {
    val ex = intercept[ParseException] {
      parser.parsePlan(
        s"""CREATE $datasetSqlSyntax table1 LOCATION 'dbfs:/path/to/table'
           |AS SELECT * FROM input""".stripMargin)
    }
    checkError(
      exception = ex,
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> ("Specifying location is not supported for " +
        s"CREATE $datasetSqlSyntax statements. The storage location for a pipeline dataset is " +
        "managed by the pipeline itself.")),
      queryContext = ex.getQueryContext.map(toExpectedContext)
    )
  }

  test("Column constraints are unsupported") {
    val ex = intercept[ParseException] {
      parser.parsePlan(s"CREATE $datasetSqlSyntax table1 (id INT CHECK(id > 0)) AS SELECT 1")
    }
    checkError(
      exception = ex,
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> ("Pipeline datasets do not currently support column " +
        "constraints. Please remove and CHECK, UNIQUE, PK, and FK constraints specified on the " +
        "pipeline dataset.")),
      queryContext = ex.getQueryContext.map(toExpectedContext)
    )
  }

  test("Comment is correctly parsed") {
    Seq(
      (s"CREATE $datasetSqlSyntax table1 COMMENT 'this is a comment' AS SELECT * FROM input",
        Some("this is a comment")),
      (s"CREATE $datasetSqlSyntax table1 AS SELECT * FROM input", None)
    ).foreach { case (query, commentOpt) =>
      val plan = parser.parsePlan(query)
      val cmd = plan.asInstanceOf[CreatePipelineDatasetAsSelect]
      assert(cmd.tableSpec.comment == commentOpt)
    }
  }

  test("STORED AS syntax is unsupported") {
    val ex = intercept[ParseException] {
      parser.parsePlan(s"CREATE $datasetSqlSyntax table1 STORED AS TEXTFILE AS SELECT * FROM input")
    }
    checkError(
      exception = ex,
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> ("The STORED AS syntax is not supported for CREATE " +
        s"$datasetSqlSyntax statements. Consider using the Data Source based USING clause " +
        "instead.")),
      queryContext = ex.getQueryContext.map(toExpectedContext)
    )
  }

  test("SerDe information is unsupported") {
    val ex = intercept[ParseException] {
      parser.parsePlan(s"""
                          |CREATE $datasetSqlSyntax table1
                          |ROW FORMAT DELIMITED
                          |FIELDS TERMINATED BY ','
                          |AS SELECT * FROM input
       """.stripMargin)
    }
    checkError(
      exception = ex,
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> ("Hive SerDe format options are not supported for CREATE " +
        s"$datasetSqlSyntax statements.")),
      queryContext = ex.getQueryContext.map(toExpectedContext)
    )
  }

  test("USING clause is parsed correctly") {
    Seq(
      (s"CREATE $datasetSqlSyntax table1 USING parquet AS SELECT * FROM input", Some("parquet")),
      (s"CREATE $datasetSqlSyntax table1 AS SELECT * FROM input", None)
    ).foreach { case (query, providerOpt) =>
      val plan = parser.parsePlan(query)
      val cmd = plan.asInstanceOf[CreatePipelineDatasetAsSelect]
      assert(cmd.tableSpec.provider == providerOpt)
    }
  }

  test("IF NOT EXISTS parses correctly") {
    Seq(
      (s"CREATE $datasetSqlSyntax IF NOT EXISTS table1 AS SELECT * FROM input", true),
      (s"CREATE $datasetSqlSyntax table1 AS SELECT * FROM input", false)
    ).foreach { case (query, ifNotExists) =>
      val plan = parser.parsePlan(query)
      val cmd = plan.asInstanceOf[CreatePipelineDatasetAsSelect]
      assert(cmd.ifNotExists == ifNotExists)
    }
  }

  test("Table properties parse correctly") {
    Seq(
      (s"CREATE $datasetSqlSyntax table1 TBLPROPERTIES(a=FALSE, b='long comment', c=100) AS " +
        "SELECT 1", Map("a" -> "false", "b" -> "long comment", "c" -> "100")),
      (s"CREATE $datasetSqlSyntax tabel1 AS SELECT 1", Map.empty)
    ).foreach { case (query, tblProperties) =>
      val plan = parser.parsePlan(query)
      val cmd = plan.asInstanceOf[CreatePipelineDatasetAsSelect]
      assert(cmd.tableSpec.properties == tblProperties)
    }
  }

  test("Bucket spec is unsupported") {
    val ex = intercept[ParseException] {
      parser.parsePlan(
        s"""CREATE $datasetSqlSyntax table1 CLUSTERED BY (a) INTO 5 BUCKETS
           |AS SELECT * FROM input""".stripMargin)
    }
    checkError(
      exception = ex,
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> (s"Bucketing is not supported for CREATE $datasetSqlSyntax " +
        "statements. Please remove any bucket spec specified in the statement.")),
      queryContext = ex.getQueryContext.map(toExpectedContext)
    )
  }

  test("Options is unsupported") {
    val ex = intercept[ParseException] {
      parser.parsePlan(
        s"""CREATE $datasetSqlSyntax table1 OPTIONS ('key' = 'value')
           |AS SELECT * FROM input""".stripMargin)
    }
    checkError(
      exception = ex,
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> (s"Options are not supported for CREATE $datasetSqlSyntax " +
        "statements. Please remove any OPTIONS lists specified in the statement.")),
      queryContext = ex.getQueryContext.map(toExpectedContext)
    )
  }

  test("CTAS subquery is parsed into plan correctly") {
    Seq(
      "SELECT 1",
      "SELECT * FROM input",
      "SELECT a, b, c FROM input2",
      """
        |SELECT o.id, o.amount, c.region
        |FROM orders o
        |JOIN (
        |  SELECT id, region
        |  FROM customers
        |  WHERE region IS NOT NULL
        |) AS c
        |ON o.customer_id = c.id
        |""".stripMargin
    ).foreach { subquery =>
      val plan = parser.parsePlan(
        s"""
           |CREATE $datasetSqlSyntax table1 AS
           |$subquery""".stripMargin)
      val cmd = plan.asInstanceOf[CreatePipelineDatasetAsSelect]
      assert(cmd.originalText.replaceAll("\\s", "") == subquery.replaceAll("\\s", ""))
      assert(cmd.query == parser.parsePlan(subquery))
    }
  }

  test("Collation is correctly parsed") {
    Seq(
      (s"CREATE $datasetSqlSyntax table1 DEFAULT COLLATION UTF8_LCASE AS SELECT * FROM input",
        Some("UTF8_LCASE")),
      (s"CREATE $datasetSqlSyntax table1 AS SELECT * FROM input", None)
    ).foreach { case (query, collationOpt) =>
      val plan = parser.parsePlan(query)
      val cmd = plan.asInstanceOf[CreatePipelineDatasetAsSelect]
      assert(cmd.tableSpec.collation == collationOpt)
    }
  }

  /** Return an ExpectedContext that is equivalent to the passed QueryContext. Used to no-op
   * queryContext checks on error validation */
  def toExpectedContext(actualQueryContext: QueryContext): ExpectedContext = ExpectedContext(
    objectType = actualQueryContext.objectType(),
    objectName = actualQueryContext.objectName(),
    startIndex = actualQueryContext.startIndex(),
    stopIndex = actualQueryContext.stopIndex(),
    fragment = actualQueryContext.fragment()
  )
}
