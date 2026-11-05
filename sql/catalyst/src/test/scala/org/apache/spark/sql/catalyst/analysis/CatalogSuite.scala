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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.CurrentUserContext.CURRENT_USER
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.ColumnDefinition
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin, TreeNodeTag}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{CharType, MetadataBuilder, StringType, StructType, VarcharType}


class CatalogSuite extends AnalysisTest {

  test("resolve command identifiers with their origin and tags") {
    val catalogManager = getAnalyzer.catalogManager
    val resolution = new CatalogIdentifierResolution(catalogManager)
    val origin = Origin(line = Some(7), startPosition = Some(11))
    val tag = TreeNodeTag[String]("command-identifier")
    val unresolved = CurrentOrigin.withOrigin(origin) {
      UnresolvedIdentifier(Seq("view"), allowTemp = false)
    }
    unresolved.setTagValue(tag, "preserved")

    val resolved = resolution.resolveUnresolvedIdentifier(unresolved)

    assert(resolved.catalog == catalogManager.currentCatalog)
    assert(resolved.identifier.namespace().toSeq == Seq("default"))
    assert(resolved.identifier.name() == "view")
    assert(resolved.output.isEmpty)
    assert(resolved.origin == origin)
    assert(resolved.getTagValue(tag) == Some("preserved"))
  }

  test("resolve command identifiers only uses temporary views when allowed") {
    val catalogManager = getAnalyzer.catalogManager
    val resolution = new CatalogIdentifierResolution(catalogManager)
    Seq(true, false).foreach { allowTemp =>
      val resolved = resolution.resolveUnresolvedIdentifier(
        UnresolvedIdentifier(Seq("TaBlE"), allowTemp = allowTemp))
      val expectedCatalog = if (allowTemp) FakeSystemCatalog else catalogManager.currentCatalog
      assert(resolved.catalog == expectedCatalog)
      assert(resolved.identifier.name() == "TaBlE")
      assert(resolved.output.isEmpty)
    }
  }

  test("column output preserves metadata, nullability and CHAR/VARCHAR configuration") {
    val resolution = new CatalogIdentifierResolution(getAnalyzer.catalogManager)
    val metadata = new MetadataBuilder().putString("source", "column").build()
    val columns = Seq(
      ColumnDefinition("char_col", CharType(4), nullable = false, metadata = metadata),
      ColumnDefinition("nested", new StructType().add("varchar_col", VarcharType(8))))

    for (preserve <- Seq(false, true); standard <- Seq(false, true)) {
      withSQLConf(
        SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> preserve.toString,
        SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> standard.toString) {
        val output = resolution.columnsToOutput(columns)
        val expectedTypes = if (preserve || standard) {
          columns.map(_.dataType)
        } else {
          Seq(StringType, new StructType().add("varchar_col", StringType))
        }
        assert(output.map(_.name) == columns.map(_.name))
        assert(output.map(_.dataType) == expectedTypes)
        assert(output.map(_.nullable) == columns.map(_.nullable))
        assert(output.map(_.metadata) == columns.map(_.metadata))
        assert(output.map(_.exprId).distinct.size == columns.size)
      }
    }
    assert(resolution.columnsToOutput(Nil).isEmpty)
  }

  test("desc table when owner is set to null") {
    val table = CatalogTable(
      identifier = TableIdentifier("tbl", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      owner = null,
      schema = new StructType().add("col1", "int").add("col2", "string"),
      provider = Some("parquet"))
    table.toLinkedHashMap
  }

  test("SPARK-45454: Set table owner to current_user") {
    val testOwner = "test_table_owner"
    try {
      CURRENT_USER.set(testOwner)
      val table = CatalogTable(
        identifier = TableIdentifier("tbl", Some("db1")),
        tableType = CatalogTableType.MANAGED,
        storage = CatalogStorageFormat.empty,
        schema = new StructType().add("col1", "int").add("col2", "string"),
        provider = Some("parquet"))
      assert(table.owner === testOwner)
    } finally {
      CURRENT_USER.remove()
    }
  }
}
