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

package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.analysis.resolver.{MetadataResolver, Resolver}
import org.apache.spark.sql.catalyst.catalog.{HiveTableRelation, UnresolvedCatalogRelation}
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class DataSourceWithHiveResolverSuite extends TestHiveSingleton with SQLTestUtils {
  private val keyValueTableSchema = StructType(
    Seq(
      StructField("key", IntegerType, true),
      StructField("value", StringType, true)
    )
  )

  test("ORC table resolution") {
    withTable("src_orc") {
      spark.sql("CREATE TABLE src_orc (key INT, value STRING) STORED AS ORC")

      checkResolveOperator(
        sqlText = "SELECT * FROM src_orc",
        expectedTableName = "spark_catalog.default.src_orc",
        expectedTableSchema = keyValueTableSchema,
        convertedToLogicalRelation = true
      )
    }
  }

  test("ORC table resolution without conversion") {
    withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> "false") {
      withTable("src_orc_no_conversion") {
        spark.sql("CREATE TABLE src_orc_no_conversion (key INT, value STRING) STORED AS ORC")

        checkResolveOperator(
          sqlText = "SELECT * FROM src_orc_no_conversion",
          expectedTableName = "spark_catalog.default.src_orc_no_conversion",
          expectedTableSchema = keyValueTableSchema,
          convertedToLogicalRelation = false
        )
      }
    }
  }

  private def checkResolveOperator(
      sqlText: String,
      expectedTableName: String,
      expectedTableSchema: StructType,
      convertedToLogicalRelation: Boolean) = {
    val metadataResolver = new MetadataResolver(
      spark.sessionState.catalogManager,
      Resolver.createRelationResolution(spark.sessionState.catalogManager)
    )
    val dataSourceWithHiveResolver = new DataSourceWithHiveResolver(
      spark,
      spark.sessionState.catalog.asInstanceOf[HiveSessionCatalog]
    )

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

    dataSourceWithHiveResolver.resolveOperator(partiallyResolvedRelation) match {
      case logicalRelation: LogicalRelation =>
        assert(convertedToLogicalRelation)
        assert(logicalRelation.catalogTable.get.identifier.unquotedString == expectedTableName)
        assert(logicalRelation.relation.schema == expectedTableSchema)
      case hiveTableRelation: HiveTableRelation =>
        assert(!convertedToLogicalRelation)
        assert(hiveTableRelation.tableMeta.identifier.unquotedString == expectedTableName)
        assert(hiveTableRelation.tableMeta.schema == expectedTableSchema)
    }
  }
}
