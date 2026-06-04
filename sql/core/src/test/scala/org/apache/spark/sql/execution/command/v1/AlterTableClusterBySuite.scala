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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.ClusterBySpec
import org.apache.spark.sql.connector.expressions.{ApplyTransform, FieldReference, IdentityTransform, Transform}
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
    assert(table.clusterBySpec === Some(ClusterBySpec.ofColumns(clusteringColumns.map(FieldReference(_)))))
  }

  override def validateClusterBy(
      tableName: String,
      clusteringColumns: Seq[String],
      expectedTransforms: Seq[Option[Transform]]): Unit = {
    val catalog = spark.sessionState.catalog
    val (_, db, t) = parseTableName(tableName)
    val table = catalog.getTableMetadata(TableIdentifier(t, Some(db)))
    val spec = table.clusterBySpec.get
    assert(spec.columnNames === clusteringColumns.map(FieldReference(_)))
    assert(spec.entries.length === expectedTransforms.length,
      s"Expected ${expectedTransforms.length} entries but got ${spec.entries.length}")
    spec.entries.zip(expectedTransforms).foreach {
      case (entry, None) =>
        assert(entry.isInstanceOf[IdentityTransform],
          s"Expected plain column but got: $entry")
      case (entry, Some(transform)) =>
        assert(entry.isInstanceOf[ApplyTransform],
          s"Expected ApplyTransform but got: $entry")
        assert(entry.name() === transform.name(),
          s"Transform name mismatch: ${entry.name()} != ${transform.name()}")
    }
  }
}

/**
 * The class contains tests for the `ALTER TABLE ... CLUSTER BY` command to check V1 In-Memory
 * table catalog.
 */
class AlterTableClusterBySuite extends AlterTableClusterBySuiteBase
    with CommandSuiteBase {
  override def commandVersion: String = super[AlterTableClusterBySuiteBase].commandVersion
}
