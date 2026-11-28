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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.ClusterBySpec
import org.apache.spark.sql.connector.FakeV2ProviderWithCustomSchema
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.execution.command

/**
 * This base suite contains unified tests for the `ALTER TABLE ... CLUSTER BY` command that
 * checks V1 table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.AlterTableClusterBySuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.AlterTableClusterBySuite`
 */
trait AlterTableClusterBySuiteBase extends command.AlterTableClusterBySuiteBase
    with command.TestsV1AndV2Commands {
  override def validateClusterBy(tableName: String, clusteringColumns: Seq[String]): Unit = {
    val catalog = spark.sessionState.catalog
    val (_, db, t) = parseTableName(tableName)
    val table = catalog.getTableMetadata(TableIdentifier(t, Some(db)))
    assert(table.clusterBySpec === Some(ClusterBySpec(clusteringColumns.map(FieldReference(_)))))
  }
}

/**
 * The class contains tests for the `ALTER TABLE ... CLUSTER BY` command to check V1 In-Memory
 * table catalog.
 */
class AlterTableClusterBySuite extends AlterTableClusterBySuiteBase
    with CommandSuiteBase {
  override def commandVersion: String = super[AlterTableClusterBySuiteBase].commandVersion

  private val partitioningAndErrors = Seq(
    "PARTITIONED BY (p)" -> "SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED",
    "CLUSTERED BY (p) INTO 2 BUCKETS" -> "SPECIFY_CLUSTER_BY_WITH_BUCKETING_IS_NOT_ALLOWED")

  private val providers = Seq("parquet", classOf[FakeV2ProviderWithCustomSchema].getName)

  for (provider <- providers; (layout, error) <- partitioningAndErrors) {
    test(s"SPARK-62285: reject ALTER TABLE CLUSTER BY with $provider and $layout") {
      withNamespaceAndTable("ns", "table") { tbl =>
        sql(s"CREATE TABLE $tbl (id INT, p INT) USING $provider $layout")
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"ALTER TABLE $tbl CLUSTER BY (id)")
          },
          condition = error,
          parameters = Map.empty)
        sql(s"ALTER TABLE $tbl CLUSTER BY NONE")
        val ddl = sql(s"SHOW CREATE TABLE $tbl").head().getString(0)
        sql(s"DROP TABLE $tbl")
        sql(ddl)
      }
    }
  }

  test("SPARK-62285: reject conflicting clustering and partition transforms") {
    withNamespaceAndTable("ns", "table") { tbl =>
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"CREATE TABLE $tbl (id INT, p INT) USING PARQUET PARTITIONED BY (p, cluster_by(id))")
        },
        condition = "SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED",
        parameters = Map.empty)
    }
  }
}
