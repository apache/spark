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

package org.apache.spark.sql.connector.catalog

import java.util

import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class StagingInMemoryTableCatalog extends InMemoryTableCatalog with StagingTableCatalog {
  import InMemoryTableCatalog._
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def stageCreate(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    validateStagedTable(partitions, properties)
    new TestStagedCreateTable(
      ident,
      new InMemoryTable(s"$name.${ident.quoted}",
        CatalogV2Util.v2ColumnsToStructType(columns), partitions, properties))
  }

  override def stageReplace(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    validateStagedTable(partitions, properties)
    new TestStagedReplaceTable(
      ident,
      new InMemoryTable(s"$name.${ident.quoted}",
        CatalogV2Util.v2ColumnsToStructType(columns), partitions, properties))
  }

  override def stageCreateOrReplace(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    validateStagedTable(partitions, properties)
    new TestStagedCreateOrReplaceTable(
      ident,
      new InMemoryTable(s"$name.${ident.quoted}",
        CatalogV2Util.v2ColumnsToStructType(columns), partitions, properties))
  }

  private def validateStagedTable(
      partitions: Array[Transform],
      properties: util.Map[String, String]): Unit = {
    if (partitions.nonEmpty) {
      throw new UnsupportedOperationException(
        s"Catalog $name: Partitioned tables are not supported")
    }

    maybeSimulateFailedTableCreation(properties)
  }

  protected abstract class TestStagedTable(
      ident: Identifier,
      delegateTable: InMemoryTable)
    extends StagedTable with SupportsWrite with SupportsRead {

    override def abortStagedChanges(): Unit = {}

    override def name(): String = delegateTable.name

    override def schema(): StructType = delegateTable.schema

    override def capabilities(): util.Set[TableCapability] = delegateTable.capabilities

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
      delegateTable.newWriteBuilder(info)
    }

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      delegateTable.newScanBuilder(options)
    }
  }

  private class TestStagedCreateTable(
      ident: Identifier,
      delegateTable: InMemoryTable) extends TestStagedTable(ident, delegateTable) {

    override def commitStagedChanges(): Unit = {
      val maybePreCommittedTable = tables.putIfAbsent(ident, delegateTable)
      if (maybePreCommittedTable != null) {
        throw new TableAlreadyExistsException(ident.asMultipartIdentifier)
      }
    }
  }

  private class TestStagedReplaceTable(
      ident: Identifier,
      delegateTable: InMemoryTable) extends TestStagedTable(ident, delegateTable) {

    override def commitStagedChanges(): Unit = {
      maybeSimulateDropBeforeCommit()
      val maybePreCommittedTable = tables.replace(ident, delegateTable)
      if (maybePreCommittedTable == null) {
        throw QueryCompilationErrors.cannotReplaceMissingTableError(ident)
      }
    }

    private def maybeSimulateDropBeforeCommit(): Unit = {
      if ("true".equalsIgnoreCase(
        delegateTable.properties.get(SIMULATE_DROP_BEFORE_REPLACE_PROPERTY))) {
        tables.remove(ident)
      }
    }
  }

  private class TestStagedCreateOrReplaceTable(
      ident: Identifier,
      delegateTable: InMemoryTable) extends TestStagedTable(ident, delegateTable) {

    override def commitStagedChanges(): Unit = {
      tables.put(ident, delegateTable)
    }
  }
}
