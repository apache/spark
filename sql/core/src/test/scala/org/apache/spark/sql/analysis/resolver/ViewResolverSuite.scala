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

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.analysis.resolver.{MetadataResolver, Resolver}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast}
import org.apache.spark.sql.catalyst.plans.logical.{
  LocalRelation,
  LogicalPlan,
  OneRowRelation,
  Project,
  SubqueryAlias,
  View
}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType}

class ViewResolverSuite extends QueryTest with SharedSparkSession {
  private val catalogName =
    "spark_catalog"
  private val col1Integer =
    AttributeReference(name = "col1", dataType = IntegerType, nullable = false)()
  private val col2String =
    AttributeReference(name = "col2", dataType = StringType, nullable = false)()

  test("Temporary view") {
    withView("temporary_view") {
      spark.sql("CREATE TEMPORARY VIEW temporary_view AS SELECT col1, col2 FROM VALUES (1, 'a');")

      checkViewResolution(
        "SELECT * FROM temporary_view",
        expectedChild = Project(
          projectList = Seq(
            Alias(Cast(col1Integer, IntegerType).withTimeZone(conf.sessionLocalTimeZone), "col1")(),
            Alias(Cast(col2String, StringType).withTimeZone(conf.sessionLocalTimeZone), "col2")()
          ),
          child = Project(
            projectList = Seq(col1Integer, col2String),
            child = LocalRelation(
              output = Seq(col1Integer, col2String),
              data = Seq(
                InternalRow.fromSeq(Seq(1, "a").map(CatalystTypeConverters.convertToCatalyst))
              )
            )
          )
        )
      )
    }
  }

  test("Persistent view") {
    withView("persistent_view") {
      spark.sql("CREATE VIEW persistent_view AS SELECT col1, col2 FROM VALUES (1, 'a');")

      checkViewResolution(
        "SELECT * FROM persistent_view",
        expectedChild = Project(
          projectList = Seq(
            Alias(Cast(col1Integer, IntegerType).withTimeZone(conf.sessionLocalTimeZone), "col1")(),
            Alias(Cast(col2String, StringType).withTimeZone(conf.sessionLocalTimeZone), "col2")()
          ),
          child = Project(
            projectList = Seq(col1Integer, col2String),
            child = LocalRelation(
              output = Seq(col1Integer, col2String),
              data = Seq(
                InternalRow.fromSeq(Seq(1, "a").map(CatalystTypeConverters.convertToCatalyst))
              )
            )
          )
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
                fragment = "view3",
                start = 14,
                stop = 18
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
        ),
        context = ExpectedContext(
          fragment = "v100",
          start = 14,
          stop = 17
        )
      )
    } finally {
      for (i <- 0 until (conf.maxNestedViewDepth + 1)) {
        spark.sql(s"DROP VIEW v${conf.maxNestedViewDepth - i};")
      }
    }
  }

  private def checkViewResolution(
      sqlText: String,
      expectedChild: LogicalPlan = OneRowRelation()) = {
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

    val resolver = new Resolver(spark.sessionState.catalogManager)

    val resolvedView = resolver
      .lookupMetadataAndResolve(unresolvedPlan)
      .asInstanceOf[Project]
      .child
      .asInstanceOf[SubqueryAlias]
      .child
      .asInstanceOf[View]
    assert(resolvedView.isTempView == unresolvedView.isTempView)
    assert(normalizeExprIds(resolvedView.child) == normalizeExprIds(expectedChild))
  }
}
