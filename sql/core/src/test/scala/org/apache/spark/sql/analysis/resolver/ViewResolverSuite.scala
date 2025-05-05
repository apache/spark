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

package org.apache.spark.sql.analysis.resolver

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.analysis.resolver.{MetadataResolver, Resolver, ResolverRunner}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ViewResolverSuite extends QueryTest with SharedSparkSession {
  private val catalogName =
    "spark_catalog"
  private val col1Integer = "col1".attr.int.withNullability(false)
  private val col2String = "col2".attr.string.withNullability(false)
  private val testTable = LocalRelation.fromExternalRows(
    Seq(col1Integer, col2String),
    Seq(Row(1, "a"))
  )

  test("Temporary view") {
    withView("v1") {
      spark.sql("CREATE TEMPORARY VIEW v1 AS SELECT col1, col2 FROM VALUES (1, 'a')")

      checkViewResolution(
        "SELECT * FROM v1",
        expectedChild = testTable
          .select(col1Integer, col2String)
          .select(
            cast(col1Integer, IntegerType).as("col1"),
            cast(col2String, StringType).as("col2")
          )
      )
    }
  }

  test("Persistent view") {
    withView("v1") {
      spark.sql("CREATE VIEW v1 AS SELECT col1, col2 FROM VALUES (1, 'a')")

      checkViewResolution(
        "SELECT * FROM v1",
        expectedChild = testTable
          .select(col1Integer, col2String)
          .select(
            cast(
              col1Integer,
              IntegerType,
              ansiEnabled = conf.ansiEnabled || conf.viewSchemaCompensation
            ).as("col1"),
            cast(
              col2String,
              StringType,
              ansiEnabled = conf.ansiEnabled || conf.viewSchemaCompensation
            ).as("col2")
          )
      )
    }
  }

  test("Nested views resolution failed") {
    withTable("table1") {
      spark.sql("CREATE TABLE table1 (col1 INT, col2 STRING);")
      withView("view1") {
        spark.sql("CREATE VIEW view1 AS SELECT col1, col2 FROM table1;")
        withView("view2") {
          spark.sql("CREATE VIEW view2 AS SELECT col2, col1 FROM view1;")
          withView("view3") {
            spark.sql("CREATE VIEW view3 AS SELECT col1, col2 FROM view2;")

            spark.sql("DROP TABLE table1;")

            checkErrorTableNotFound(
              exception = intercept[AnalysisException] {
                checkViewResolution("SELECT * FROM view3")
              },
              tableName = "`table1`",
              queryContext = ExpectedContext(
                objectType = "VIEW",
                objectName = s"$catalogName.default.view1",
                startIndex = 23,
                stopIndex = 28,
                fragment = "table1"
              )
            )
          }
        }
      }
    }
  }

  test("Max nested view depth exceeded") {
    try {
      spark.sql("CREATE VIEW v0 AS SELECT * FROM VALUES (1);")
      for (i <- 0 until conf.maxNestedViewDepth) {
        spark.sql(s"CREATE VIEW v${i + 1} AS SELECT * FROM v$i;")
      }

      checkError(
        exception = intercept[AnalysisException] {
          checkViewResolution(s"SELECT * FROM v${conf.maxNestedViewDepth}")
        },
        condition = "VIEW_EXCEED_MAX_NESTED_DEPTH",
        parameters = Map(
          "viewName" -> s"`$catalogName`.`default`.`v0`",
          "maxNestedDepth" -> conf.maxNestedViewDepth.toString
        )
      )
    } finally {
      for (i <- 0 until (conf.maxNestedViewDepth + 1)) {
        spark.sql(s"DROP VIEW v${conf.maxNestedViewDepth - i};")
      }
    }
  }

  test("View with options") {
    withView("v1") {
      spark.sql("CREATE VIEW v1 AS SELECT col1, col2 FROM VALUES (1, 'a')")

      val options = new java.util.HashMap[String, String]
      options.put("foo", "bar")
      checkViewResolution(
        "SELECT * FROM v1 WITH ('foo' = 'bar')",
        expectedChild = testTable
          .select(col1Integer, col2String)
          .select(
            cast(
              col1Integer,
              IntegerType,
              ansiEnabled = conf.ansiEnabled || conf.viewSchemaCompensation
            ).as("col1"),
            cast(
              col2String,
              StringType,
              ansiEnabled = conf.ansiEnabled || conf.viewSchemaCompensation
            ).as("col2")
          ),
        expectedOptions = new CaseInsensitiveStringMap(options)
      )
    }
  }

  private def checkViewResolution(
      sqlText: String,
      expectedChild: LogicalPlan = OneRowRelation(),
      expectedOptions: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty()) = {
    val metadataResolver = new MetadataResolver(
      spark.sessionState.catalogManager,
      Resolver.createRelationResolution(spark.sessionState.catalogManager)
    )

    val unresolvedPlan = spark.sessionState.sqlParser.parsePlan(sqlText)

    metadataResolver.resolve(unresolvedPlan)

    val unresolvedRelations = unresolvedPlan.collect {
      case unresolvedRelation: UnresolvedRelation => unresolvedRelation
    }
    assert(unresolvedRelations.size == 1)

    val unresolvedView = metadataResolver
      .getRelationWithResolvedMetadata(unresolvedRelations.head)
      .get
      .asInstanceOf[SubqueryAlias]
      .child
      .asInstanceOf[View]

    val resolverRunner = new ResolverRunner(new Resolver(spark.sessionState.catalogManager))

    val resolvedView = resolverRunner
      .resolve(unresolvedPlan)
      .asInstanceOf[Project]
      .child
      .asInstanceOf[SubqueryAlias]
      .child
      .asInstanceOf[View]
    assert(resolvedView.isTempView == unresolvedView.isTempView)
    assert(
      normalizeExprIds(resolvedView.child).prettyJson == normalizeExprIds(expectedChild).prettyJson
    )
    assert(resolvedView.options == expectedOptions)
  }

  private def cast(
      child: Expression,
      dataType: DataType,
      ansiEnabled: Boolean = conf.ansiEnabled): Expression = {
    Cast(child, dataType, ansiEnabled).withTimeZone(conf.sessionLocalTimeZone)
  }
}
