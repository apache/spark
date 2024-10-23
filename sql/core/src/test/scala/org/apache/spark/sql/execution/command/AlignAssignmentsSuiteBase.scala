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

import java.util.Collections

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, Analyzer, FunctionRegistry, NoSuchTableException, ResolveSessionCatalog}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogNotFoundException, CatalogV2Util, Column, ColumnDefaultValue, Identifier, SupportsRowLevelOperations, TableCapability, TableCatalog, TableWritePrivilege}
import org.apache.spark.sql.connector.expressions.{LiteralValue, Transform}
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, IntegerType, StructType}

abstract class AlignAssignmentsSuiteBase extends AnalysisTest {

  private val primitiveTable = {
    val t = mock(classOf[SupportsRowLevelOperations])
    val schema = new StructType()
      .add("i", "INT", nullable = false)
      .add("l", "LONG")
      .add("txt", "STRING")
    when(t.columns()).thenReturn(CatalogV2Util.structTypeToV2Columns(schema))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private val primitiveTableSource = {
    val t = mock(classOf[SupportsRowLevelOperations])
    val schema = new StructType()
      .add("l", "LONG")
      .add("txt", "STRING")
      .add("i", "INT")
    when(t.columns()).thenReturn(CatalogV2Util.structTypeToV2Columns(schema))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private val nestedStructTable = {
    val t = mock(classOf[SupportsRowLevelOperations])
    val schema = new StructType()
      .add("i", "INT")
      .add(
        "s",
        "STRUCT<n_i: INT NOT NULL, n_s: STRUCT<dn_i: INT NOT NULL, dn_l: LONG>>",
        nullable = false)
      .add("txt", "STRING")
    when(t.columns()).thenReturn(CatalogV2Util.structTypeToV2Columns(schema))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private val nestedStructTableSource = {
    val t = mock(classOf[SupportsRowLevelOperations])
    val schema = new StructType()
      .add("i", "INT")
      .add("s", "STRUCT<n_i: INT, n_s: STRUCT<dn_i: INT, dn_l: LONG>>")
      .add("txt", "STRING")
    when(t.columns()).thenReturn(CatalogV2Util.structTypeToV2Columns(schema))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private val mapArrayTable = {
    val t = mock(classOf[SupportsRowLevelOperations])
    val schema = new StructType()
      .add("i", "INT")
      .add("a", "ARRAY<STRUCT<i1: INT, i2: INT>>")
      .add("m", "MAP<STRING, STRING>")
      .add("txt", "STRING")
    when(t.columns()).thenReturn(CatalogV2Util.structTypeToV2Columns(schema))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private val charVarcharTable = {
    val t = mock(classOf[SupportsRowLevelOperations])
    val schema = new StructType()
      .add("c", "CHAR(5)")
      .add(
        "s",
        "STRUCT<n_i: INT, n_vc: VARCHAR(5)>",
        nullable = false)
      .add(
        "a",
        "ARRAY<STRUCT<n_i: INT, n_vc: VARCHAR(5)>>",
        nullable = false)
      .add(
        "mk",
        "MAP<STRUCT<n_i: INT, n_vc: VARCHAR(5)>, STRING>",
        nullable = false)
      .add(
        "mv",
        "MAP<STRING, STRUCT<n_i: INT, n_vc: VARCHAR(5)>>",
        nullable = false)
    when(t.columns()).thenReturn(CatalogV2Util.structTypeToV2Columns(schema))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private val acceptsAnySchemaTable = {
    val t = mock(classOf[SupportsRowLevelOperations])
    val schema = new StructType()
      .add("i", "INT", nullable = false)
      .add("l", "LONG")
      .add("txt", "STRING")
    when(t.columns()).thenReturn(CatalogV2Util.structTypeToV2Columns(schema))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    when(t.capabilities()).thenReturn(Collections.singleton(TableCapability.ACCEPT_ANY_SCHEMA))
    t
  }

  private val defaultValuesTable = {
    val t = mock(classOf[SupportsRowLevelOperations])
    val iDefault = new ColumnDefaultValue("42", LiteralValue(42, IntegerType))
    when(t.columns()).thenReturn(Array(
      Column.create("b", BooleanType, true, null, null),
      Column.create("i", IntegerType, true, null, iDefault, null)))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private val v2Catalog = {
    val newCatalog = mock(classOf[TableCatalog])
    when(newCatalog.loadTable(any())).thenAnswer((invocation: InvocationOnMock) => {
      val ident = invocation.getArguments()(0).asInstanceOf[Identifier]
      ident.name match {
        case "primitive_table" => primitiveTable
        case "primitive_table_src" => primitiveTableSource
        case "nested_struct_table" => nestedStructTable
        case "nested_struct_table_src" => nestedStructTableSource
        case "map_array_table" => mapArrayTable
        case "char_varchar_table" => charVarcharTable
        case "accepts_any_schema_table" => acceptsAnySchemaTable
        case "default_values_table" => defaultValuesTable
        case name => throw new NoSuchTableException(Seq(name))
      }
    })
    when(newCatalog.loadTable(any(), any[java.util.Set[TableWritePrivilege]]()))
      .thenCallRealMethod()
    when(newCatalog.name()).thenReturn("cat")
    newCatalog
  }

  private val v1SessionCatalog =
    new SessionCatalog(new InMemoryCatalog(), FunctionRegistry.builtin, new SQLConf())

  private val v2SessionCatalog = new V2SessionCatalog(v1SessionCatalog)

  private val catalogManager = {
    val manager = mock(classOf[CatalogManager])
    when(manager.catalog(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArguments()(0).asInstanceOf[String] match {
        case "testcat" => v2Catalog
        case CatalogManager.SESSION_CATALOG_NAME => v2SessionCatalog
        case name => throw new CatalogNotFoundException(s"No such catalog: $name")
      }
    })
    when(manager.currentCatalog).thenReturn(v2Catalog)
    when(manager.currentNamespace).thenReturn(Array.empty[String])
    when(manager.v1SessionCatalog).thenReturn(v1SessionCatalog)
    when(manager.v2SessionCatalog).thenReturn(v2SessionCatalog)
    manager
  }

  protected def parseAndResolve(query: String): LogicalPlan = {
    val analyzer = new CustomAnalyzer(catalogManager) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Seq(
        new ResolveSessionCatalog(catalogManager))
    }
    val analyzed = analyzer.execute(CatalystSqlParser.parsePlan(query))
    analyzer.checkAnalysis(analyzed)
    analyzed
  }

  class CustomAnalyzer(catalogManager: CatalogManager) extends Analyzer(catalogManager) {

    private val ignoredRuleNames = Set(
      "org.apache.spark.sql.catalyst.analysis.RewriteUpdateTable",
      "org.apache.spark.sql.catalyst.analysis.RewriteMergeIntoTable")

    override def batches: Seq[Batch] = {
      val defaultBatches = super.batches
      defaultBatches.map { batch =>
        val filteredRules = batch.rules.filterNot { rule =>
          ignoredRuleNames.contains(rule.ruleName)
        }
        Batch(batch.name, batch.strategy, filteredRules: _*)
      }
    }
  }

  protected def assertNullCheckExists(plan: LogicalPlan, colPath: Seq[String]): Unit = {
    val asserts = plan.expressions.flatMap(e => e.collect {
      case assert: AssertNotNull if assert.walkedTypePath == colPath => assert
    })
    assert(asserts.nonEmpty, s"Must have NOT NULL checks for col $colPath")
  }

  protected def assertAnalysisException(query: String, messages: String*): Unit = {
    val exception = intercept[AnalysisException] {
      parseAndResolve(query)
    }
    messages.foreach(message => assert(exception.message.contains(message)))
  }
}
