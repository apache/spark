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
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.analysis.resolver.{
  AnalyzerBridgeState,
  BridgedRelationMetadataProvider,
  MetadataResolver,
  RelationId,
  Resolver
}
import org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation
import org.apache.spark.sql.catalyst.expressions.{Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.{FileResolver, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MetadataResolverSuite extends QueryTest with SharedSparkSession with SQLTestUtils {
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

      checkResolveUnresolvedCatalogRelation(
        sqlText = "SELECT * FROM src_csv",
        expectedTableData = Seq(createTableData("src_csv"))
      )
    }
  }

  test("Single ORC relation") {
    withTable("src_orc") {
      spark.sql("CREATE TABLE src_orc (key INT, value STRING) USING ORC;").collect()

      checkResolveUnresolvedCatalogRelation(
        sqlText = "SELECT * FROM src_orc",
        expectedTableData = Seq(createTableData("src_orc"))
      )
    }
  }

  test("Relation inside an EXISTS subquery") {
    withTable("src") {
      spark.sql("CREATE TABLE src (key INT, value STRING) USING PARQUET;").collect()

      checkResolveUnresolvedCatalogRelation(
        sqlText = "SELECT * FROM VALUES (1) WHERE EXISTS (SELECT col1 FROM src)",
        expectedTableData = Seq(createTableData("src"))
      )
    }
  }

  test("Relation inside an IN subquery") {
    withTable("src") {
      spark.sql("CREATE TABLE src (key INT, value STRING) USING PARQUET;").collect()

      checkResolveUnresolvedCatalogRelation(
        sqlText = "SELECT * FROM VALUES (1) WHERE col1 IN (SELECT col1 FROM src)",
        expectedTableData = Seq(createTableData("src"))
      )
    }
  }

  test("Relation inside a nested subquery expression") {
    withTable("src") {
      spark.sql("CREATE TABLE src (key INT, value STRING) USING PARQUET;").collect()

      checkResolveUnresolvedCatalogRelation(
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

  test("Relation from a file") {
    val df = spark.range(100).toDF()
    withTempPath(f => {
      df.write.json(f.getCanonicalPath)
      checkResolveLogicalRelation(
        sqlText = s"select id from json.`${f.getCanonicalPath}`",
        expectedTableData = Seq(
          RelationId(
            multipartIdentifier = Seq("spark_catalog", "json", s"${f.getCanonicalPath}")
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
      spark.sql("CREATE TABLE src (key INT, value STRING) USING PARQUET;").collect()

      val analyzerBridgeState = new AnalyzerBridgeState
      analyzerBridgeState.relationsWithResolvedMetadata.put(
        UnresolvedRelation(Seq("src")),
        createUnresolvedCatalogRelation("src")
      )

      checkResolveUnresolvedCatalogRelation(
        sqlText = "SELECT * FROM src",
        expectedTableData = Seq(createTableData("src")),
        analyzerBridgeState = Some(analyzerBridgeState)
      )
    }
  }

  test("Relation not bridged from legacy Analyzer") {
    withTable("src") {
      spark.sql("CREATE TABLE src (key INT, value STRING) USING PARQUET;").collect()

      checkResolveUnresolvedCatalogRelation(
        sqlText = "SELECT * FROM src",
        expectedTableData = Seq.empty,
        analyzerBridgeState = Some(new AnalyzerBridgeState)
      )
    }
  }

  private def checkResolveUnresolvedCatalogRelation(
      sqlText: String,
      expectedTableData: Seq[(RelationId, TestTableData)],
      analyzerBridgeState: Option[AnalyzerBridgeState] = None): Unit = {
    checkResolve(
      sqlText,
      expectedTableData,
      relation =>
        relation.asInstanceOf[UnresolvedCatalogRelation].tableMeta.identifier.unquotedString,
      relation => relation.asInstanceOf[UnresolvedCatalogRelation].tableMeta.schema,
      analyzerBridgeState
    )
  }

  private def checkResolveLogicalRelation(
      sqlText: String,
      expectedTableData: Seq[(RelationId, TestTableData)],
      analyzerBridgeState: Option[AnalyzerBridgeState] = None): Unit = {
    checkResolve(
      sqlText,
      expectedTableData,
      relation =>
        relation
          .asInstanceOf[LogicalRelation]
          .relation
          .asInstanceOf[HadoopFsRelation]
          .location
          .rootPaths
          .mkString(","),
      relation => relation.asInstanceOf[LogicalRelation].relation.schema,
      analyzerBridgeState
    )
  }

  private def checkResolve(
      sqlText: String,
      expectedTableData: Seq[(RelationId, TestTableData)],
      getTableName: LogicalPlan => String,
      getTableSchema: LogicalPlan => StructType,
      analyzerBridgeState: Option[AnalyzerBridgeState]): Unit = {
    val unresolvedPlan = spark.sql(sqlText).queryExecution.logical

    val metadataResolver = analyzerBridgeState match {
      case Some(analyzerBridgeState) =>
        new BridgedRelationMetadataProvider(
          spark.sessionState.catalogManager,
          Resolver.createRelationResolution(spark.sessionState.catalogManager),
          analyzerBridgeState
        )
      case None =>
        val metadataResolver = new MetadataResolver(
          spark.sessionState.catalogManager,
          Resolver.createRelationResolution(spark.sessionState.catalogManager),
          Seq(new FileResolver(spark))
        )
        metadataResolver.resolve(unresolvedPlan)
        metadataResolver
    }

    val actualTableData = new mutable.HashMap[RelationId, TestTableData]

    def findUnresolvedRelations(unresolvedPlan: LogicalPlan): Unit = unresolvedPlan.foreach {
      case unresolvedRelation: UnresolvedRelation =>
        metadataResolver.getRelationWithResolvedMetadata(unresolvedRelation) match {
          case Some(plan) =>
            val relationId = metadataResolver.relationIdFromUnresolvedRelation(unresolvedRelation)
            val relation = plan match {
              case SubqueryAlias(_, relation) => relation
              case relation => relation
            }

            actualTableData(relationId) =
              TestTableData(getTableName(relation), getTableSchema(relation))
          case None =>
        }
      case unresolvedPlan =>
        def traverseExpressions(expression: Expression): Unit = expression match {
          case planExpression: PlanExpression[_] =>
            planExpression.plan match {
              case plan: LogicalPlan =>
                findUnresolvedRelations(plan)
              case _ =>
            }
          case expression =>
            expression.children.foreach(traverseExpressions)
        }

        unresolvedPlan.expressions.foreach(traverseExpressions)
    }

    findUnresolvedRelations(unresolvedPlan)

    assert(actualTableData == mutable.HashMap(expectedTableData: _*))
  }

  private def createTableData(name: String) =
    RelationId(
      multipartIdentifier = Seq("spark_catalog", "default", name)
    ) -> TestTableData(
      name = s"spark_catalog.default.$name",
      schema = keyValueTableSchema
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
