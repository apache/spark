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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.analysis.resolver.{MetadataResolver, Resolver}
import org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class DataSourceResolverSuite extends QueryTest with SharedSparkSession {
  private val keyValueTableSchema = StructType(
    Seq(
      StructField("key", IntegerType, true),
      StructField("value", StringType, true)
    )
  )

  test("CSV relation") {
    withTable("src_csv") {
      spark.sql("CREATE TABLE src_csv (key INT, value STRING) USING CSV;").collect()

      checkResolveOperator(
        sqlText = "SELECT * FROM src_csv",
        expectedTableName = "spark_catalog.default.src_csv",
        expectedTableSchema = keyValueTableSchema
      )
    }
  }

  test("JSON relation") {
    withTable("src_json") {
      spark.sql("CREATE TABLE src_json (key INT, value STRING) USING JSON;").collect()

      checkResolveOperator(
        sqlText = "SELECT * FROM src_json",
        expectedTableName = "spark_catalog.default.src_json",
        expectedTableSchema = keyValueTableSchema
      )
    }
  }

  test("PARQUET relation") {
    withTable("src_parquet") {
      spark.sql("CREATE TABLE src_parquet (key INT, value STRING) USING PARQUET;").collect()

      checkResolveOperator(
        sqlText = "SELECT * FROM src_parquet",
        expectedTableName = "spark_catalog.default.src_parquet",
        expectedTableSchema = keyValueTableSchema
      )
    }
  }

  test("ORC relation") {
    withTable("src_orc") {
      spark.sql("CREATE TABLE src_orc (key INT, value STRING) USING ORC;").collect()

      checkResolveOperator(
        sqlText = "SELECT * FROM src_orc",
        expectedTableName = "spark_catalog.default.src_orc",
        expectedTableSchema = keyValueTableSchema
      )
    }
  }

  private def checkResolveOperator(
      sqlText: String,
      expectedTableName: String,
      expectedTableSchema: StructType) = {
    val metadataResolver = new MetadataResolver(
      spark.sessionState.catalogManager,
      Resolver.createRelationResolution(spark.sessionState.catalogManager)
    )
    val dataSourceResolver = new DataSourceResolver(spark)

    val unresolvedPlan = spark.sql(sqlText).queryExecution.logical

    metadataResolver.resolve(unresolvedPlan)

    val unresolvedRelations = unresolvedPlan.collect {
      case unresolvedRelation: UnresolvedRelation => unresolvedRelation
    }
    assert(unresolvedRelations.size == 1)

    val partiallyResolvedRelation = metadataResolver
      .getRelationWithResolvedMetadata(unresolvedRelations.head)
      .get
      .asInstanceOf[SubqueryAlias]
      .child
    assert(partiallyResolvedRelation.isInstanceOf[UnresolvedCatalogRelation])

    val result = dataSourceResolver.resolveOperator(partiallyResolvedRelation)

    val logicalRelation = result.asInstanceOf[LogicalRelation]
    assert(
      logicalRelation.catalogTable.get.identifier.unquotedString
      == expectedTableName
    )
    assert(logicalRelation.relation.schema == expectedTableSchema)
  }
}
