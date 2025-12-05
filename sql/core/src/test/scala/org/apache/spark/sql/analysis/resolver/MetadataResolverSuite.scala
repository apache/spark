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

import scala.collection.mutable

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, UnresolvedRelation}
import org.apache.spark.sql.catalyst.analysis.resolver._
import org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, View}
import org.apache.spark.sql.execution.datasources.{FileResolver, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MetadataResolverSuite
    extends QueryTest
    with SharedSparkSession
    with SQLTestUtils {
  private val catalogName = "spark_catalog"

  private val keyValueTableSchema = StructType(
    Seq(
      StructField("key", IntegerType, true),
      StructField("value", StringType, true)
    )
  )
  private val fileTableSchema = StructType(
    Seq(
      StructField("id", LongType, true)
    )
  )

  test("Single CSV relation") {
    withTable("src_csv") {
      spark.sql("CREATE TABLE src_csv (key INT, value STRING) USING CSV;").collect()

      checkResolve(
        sqlText = "SELECT * FROM src_csv",
        expectedTableData = Seq(createTableData("src_csv"))
      )
    }
  }

  test("Single ORC relation") {
    withTable("src_orc") {
      spark.sql("CREATE TABLE src_orc (key INT, value STRING) USING ORC;").collect()

      checkResolve(
        sqlText = "SELECT * FROM src_orc",
        expectedTableData = Seq(createTableData("src_orc"))
      )
    }
  }

  test("Relation inside an EXISTS subquery") {
    withTable("src") {
      spark.sql("CREATE TABLE src (key INT, value STRING);").collect()

      checkResolve(
        sqlText = "SELECT * FROM VALUES (1) WHERE EXISTS (SELECT col1 FROM src)",
        expectedTableData = Seq(createTableData("src"))
      )
    }
  }

  test("Relation inside an IN subquery") {
    withTable("src") {
      spark.sql("CREATE TABLE src (key INT, value STRING);").collect()

      checkResolve(
        sqlText = "SELECT * FROM VALUES (1) WHERE col1 IN (SELECT col1 FROM src)",
        expectedTableData = Seq(createTableData("src"))
      )
    }
  }

  test("Relation inside a nested subquery expression") {
    withTable("src") {
      spark.sql("CREATE TABLE src (key INT, value STRING);").collect()

      checkResolve(
        sqlText = """
          SELECT
            col1 + (
              SELECT 35 * (
                SELECT key FROM src LIMIT 1
              ) * col1 FROM VALUES (2)
            )
          FROM
            VALUES (1)
          """,
        expectedTableData = Seq(createTableData("src"))
      )
    }
  }

  test("Relation inside a CTE definition") {
    withTable("src") {
      spark.sql("CREATE TABLE src (key INT, value STRING);").collect()

      checkResolve(
        sqlText = """
          WITH cte AS (SELECT key FROM src)
          SELECT * FROM cte
          """,
        expectedTableData = Seq(createTableData("src"))
      )
    }
  }

  test("Relation inside a CTE definition without CTE references") {
    withTable("src") {
      spark.sql("CREATE TABLE src (key INT, value STRING);").collect()

      checkResolve(
        sqlText = """
          SELECT * FROM (
            WITH cte AS (SELECT key FROM src)
            SELECT 1
          )
          """,
        expectedTableData = Seq(createTableData("src"))
      )
    }
  }

  test("Relation inside a CTE definition inside a subquery expression") {
    withTable("src") {
      spark.sql("CREATE TABLE src (key INT, value STRING);").collect()

      checkResolve(
        sqlText = """
          SELECT
            col1 + (
              SELECT 35 * (
                WITH cte AS (SELECT key FROM src)
                SELECT key FROM cte LIMIT 1
              ) * col1 FROM VALUES (2)
            )
          FROM
            VALUES (1)
          """,
        expectedTableData = Seq(createTableData("src"))
      )
    }
  }

  test("Relation from a file") {
    val df = spark.range(100).toDF()
    withTempPath(f => {
      df.write.json(f.getCanonicalPath)
      checkResolve(
        sqlText = s"select id from json.`${f.getCanonicalPath}`",
        expectedTableData = Seq(
          RelationId(
            multipartIdentifier = Seq(catalogName, "json", s"${f.getCanonicalPath}")
          ) -> TestTableData(
            name = s"file:${f.getCanonicalPath}",
            schema = fileTableSchema
          )
        )
      )
    })
  }

  test("Relation bridged from legacy Analyzer") {
    withTable("src") {
      spark.sql("CREATE TABLE src (key INT, value STRING);").collect()

      val analyzerBridgeState = new AnalyzerBridgeState
      analyzerBridgeState.relationsWithResolvedMetadata.put(
        BridgedRelationId(UnresolvedRelation(Seq("src")), Seq.empty),
        createUnresolvedCatalogRelation("src")
      )

      checkResolve(
        sqlText = "SELECT * FROM src",
        expectedTableData = Seq(createTableData("src")),
        analyzerBridgeState = Some(analyzerBridgeState)
      )
    }
  }

  test("Relation not bridged from legacy Analyzer") {
    withTable("src") {
      spark.sql("CREATE TABLE src (key INT, value STRING);").collect()

      checkResolve(
        sqlText = "SELECT * FROM src",
        expectedTableData = Seq.empty,
        analyzerBridgeState = Some(new AnalyzerBridgeState)
      )
    }
  }

  test("One view") {
    withTable("t1", "t2") {
      spark.sql("CREATE TABLE t1 (col1 INT)")
      spark.sql("CREATE TABLE t2 (col2 INT)")
      withView("v1") {
        spark.sql("CREATE VIEW v1 AS SELECT col1, col2 FROM t1, t2")

        checkResolve(
          sqlText = "SELECT * FROM v1",
          expectedTableData = Seq(
            createTableData(
              "v1",
              Some(
                StructType(
                  Seq(
                    StructField("col1", IntegerType, true),
                    StructField("col2", IntegerType, true)
                  )
                )
              )
            )
          )
        )
      }
    }
  }

  test("Nested views") {
    withTable("t1", "t2", "t3") {
      spark.sql("CREATE TABLE t1 (col1 INT)")
      spark.sql("CREATE TABLE t2 (col2 INT)")
      spark.sql("CREATE TABLE t3 (col3 INT)")
      withView("v1", "v2", "v3") {
        spark.sql("CREATE VIEW v1 AS SELECT col1 FROM t1")
        spark.sql("CREATE VIEW v2 AS SELECT col1, col2 FROM v1, t2")
        spark.sql("CREATE VIEW v3 AS SELECT col1, col2 FROM v2")

        checkResolve(
          sqlText = "SELECT * FROM v3, t3",
          expectedTableData = Seq(
            createTableData(
              "v3",
              Some(
                StructType(
                  Seq(
                    StructField("col1", IntegerType, true),
                    StructField("col2", IntegerType, true)
                  )
                )
              )
            ),
            createTableData("t3", Some(StructType(Seq(StructField("col3", IntegerType, true)))))
          )
        )
      }
    }
  }

  test("Nested views with CTEs") {
    withTable("t1", "t2", "t3") {
      spark.sql("CREATE TABLE t1 (col1 INT)")
      spark.sql("CREATE TABLE t2 (col2 INT)")
      spark.sql("CREATE TABLE t3 (col3 INT)")
      withView("v1", "v2", "v3") {
        spark.sql("CREATE VIEW v1 AS SELECT col1 FROM t1")
        spark.sql(
          "CREATE VIEW v2 AS WITH cte1 AS (SELECT col1, col2 FROM v1, t2) SELECT * FROM cte1"
        )
        spark.sql(
          "CREATE VIEW v3 AS WITH cte1 AS (SELECT col1, col2 FROM v2) SELECT * FROM cte1"
        )

        checkResolve(
          sqlText = "SELECT * FROM v3, t3",
          expectedTableData = Seq(
            createTableData(
              "v3",
              Some(
                StructType(
                  Seq(
                    StructField("col1", IntegerType, true),
                    StructField("col2", IntegerType, true)
                  )
                )
              )
            ),
            createTableData("t3", Some(StructType(Seq(StructField("col3", IntegerType, true)))))
          )
        )
      }
    }
  }

  private def checkResolve(
      sqlText: String,
      expectedTableData: Seq[(RelationId, TestTableData)],
      analyzerBridgeState: Option[AnalyzerBridgeState] = None): Unit = {
    val unresolvedPlan = spark.sessionState.sqlParser.parsePlan(sqlText)

    val resolver = new Resolver(spark.sessionState.catalogManager)

    val metadataResolver = analyzerBridgeState match {
      case Some(analyzerBridgeState) =>
        new BridgedRelationMetadataProvider(
          spark.sessionState.catalogManager,
          Resolver.createRelationResolution(spark.sessionState.catalogManager),
          analyzerBridgeState,
          new ViewResolver(resolver = resolver, catalogManager = spark.sessionState.catalogManager)
        )
      case None =>
        val relationResolution =
          Resolver.createRelationResolution(spark.sessionState.catalogManager)
        new MetadataResolver(
          spark.sessionState.catalogManager,
          relationResolution,
          Seq(new FileResolver(spark))
        )
    }

    val actualTableData = new mutable.HashMap[RelationId, TestTableData]

    AnalysisContext.withNewAnalysisContext {
      metadataResolver.resolve(unresolvedPlan)

      val relationsWithResolvedMetadata = metadataResolver match {
        case bridgedRelationMetadataProvider: BridgedRelationMetadataProvider =>
          bridgedRelationMetadataProvider.TestOnly.getRelationsWithResolvedMetadata
        case metadataResolver: MetadataResolver =>
          metadataResolver.TestOnly.getRelationsWithResolvedMetadata
      }

      relationsWithResolvedMetadata.forEach {
        case (relationId, plan) =>
          val relation = plan match {
            case SubqueryAlias(_, relation) => relation
            case relation => relation
          }
          actualTableData(relationId) =
            TestTableData(getTableName(relation), getTableSchema(relation))
      }
    }

    assert(actualTableData == mutable.HashMap(expectedTableData: _*))
  }

  private def getTableName(relation: LogicalPlan): String = {
    relation match {
      case view: View => view.desc.identifier.unquotedString
      case relation: LogicalRelation =>
        relation.relation match {
          case hadoopFsRelation: HadoopFsRelation =>
            hadoopFsRelation.location.rootPaths.mkString(",")
        }
      case relation: UnresolvedCatalogRelation => relation.tableMeta.identifier.unquotedString
    }
  }

  private def getTableSchema(relation: LogicalPlan): StructType = {
    relation match {
      case view: View => view.desc.schema
      case relation: LogicalRelation => relation.relation.schema
      case relation: UnresolvedCatalogRelation => relation.tableMeta.schema
    }
  }

  private def createTableData(name: String, schema: Option[StructType] = None) =
    RelationId(
      multipartIdentifier = Seq(catalogName, "default", name)
    ) -> TestTableData(
      name = s"$catalogName.default.$name",
      schema = schema.getOrElse(keyValueTableSchema)
    )

  private def createUnresolvedCatalogRelation(name: String) = SubqueryAlias(
    AliasIdentifier(name),
    UnresolvedCatalogRelation(
      spark.sessionState.catalog.getTableMetadata(TableIdentifier(name)),
      CaseInsensitiveStringMap.empty
    )
  )

  private case class TestTableData(name: String, schema: StructType)
}
