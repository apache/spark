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

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionException, PartitionsAlreadyExistException}
import org.apache.spark.sql.connector.expressions.{LogicalExpressions, NamedReference}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class SupportsPartitionManagementSuite extends SparkFunSuite {

  private val ident: Identifier = Identifier.of(Array("ns"), "test_table")

  def ref(name: String): NamedReference = LogicalExpressions.parseReference(name)

  private val catalog: InMemoryTableCatalog = {
    val newCatalog = new InMemoryTableCatalog
    newCatalog.initialize("test", CaseInsensitiveStringMap.empty())
    newCatalog.createTable(
      ident,
      new StructType()
        .add("id", IntegerType)
        .add("data", StringType)
        .add("dt", StringType),
      Array(LogicalExpressions.identity(ref("dt"))),
      util.Collections.emptyMap[String, String])
    newCatalog
  }

  private def hasPartitions(table: SupportsPartitionManagement): Boolean = {
    !table.listPartitionIdentifiers(Array.empty, InternalRow.empty).isEmpty
  }

  test("createPartition") {
    val table = catalog.loadTable(ident)
    val partTable = new InMemoryPartitionTable(
      table.name(), table.schema(), table.partitioning(), table.properties())
    assert(!hasPartitions(partTable))

    val partIdent = InternalRow.apply("3")
    partTable.createPartition(partIdent, new util.HashMap[String, String]())
    assert(hasPartitions(partTable))
    assert(partTable.partitionExists(partIdent))

    partTable.dropPartition(partIdent)
    assert(!hasPartitions(partTable))
  }

  test("dropPartition") {
    val table = catalog.loadTable(ident)
    val partTable = new InMemoryPartitionTable(
      table.name(), table.schema(), table.partitioning(), table.properties())
    assert(!hasPartitions(partTable))

    val partIdent = InternalRow.apply("3")
    val partIdent1 = InternalRow.apply("4")
    partTable.createPartition(partIdent, new util.HashMap[String, String]())
    partTable.createPartition(partIdent1, new util.HashMap[String, String]())
    assert(partTable.listPartitionIdentifiers(Array.empty, InternalRow.empty).length == 2)

    partTable.dropPartition(partIdent)
    assert(partTable.listPartitionIdentifiers(Array.empty, InternalRow.empty).length == 1)
    partTable.dropPartition(partIdent1)
    assert(!hasPartitions(partTable))
  }

  test("purgePartition") {
    val table = catalog.loadTable(ident)
    val partTable = new InMemoryPartitionTable(
      table.name(), table.schema(), table.partitioning(), table.properties())
    val errMsg = intercept[UnsupportedOperationException] {
      partTable.purgePartition(InternalRow.apply("3"))
    }.getMessage
    assert(errMsg.contains("purge is not supported"))
  }

  test("replacePartitionMetadata") {
    val table = catalog.loadTable(ident)
    val partTable = new InMemoryPartitionTable(
      table.name(), table.schema(), table.partitioning(), table.properties())
    assert(!hasPartitions(partTable))

    val partIdent = InternalRow.apply("3")
    partTable.createPartition(partIdent, new util.HashMap[String, String]())
    assert(hasPartitions(partTable))
    assert(partTable.partitionExists(partIdent))
    assert(partTable.loadPartitionMetadata(partIdent).isEmpty)

    partTable.replacePartitionMetadata(partIdent, Map("paramKey" -> "paramValue").asJava)
    assert(hasPartitions(partTable))
    assert(partTable.partitionExists(partIdent))
    assert(!partTable.loadPartitionMetadata(partIdent).isEmpty)
    assert(partTable.loadPartitionMetadata(partIdent).get("paramKey") == "paramValue")

    partTable.dropPartition(partIdent)
    assert(!hasPartitions(partTable))
  }

  test("loadPartitionMetadata") {
    val table = catalog.loadTable(ident)
    val partTable = new InMemoryPartitionTable(
      table.name(), table.schema(), table.partitioning(), table.properties())
    assert(!hasPartitions(partTable))

    val partIdent = InternalRow.apply("3")
    partTable.createPartition(partIdent, Map("paramKey" -> "paramValue").asJava)
    assert(hasPartitions(partTable))
    assert(partTable.partitionExists(partIdent))
    assert(!partTable.loadPartitionMetadata(partIdent).isEmpty)
    assert(partTable.loadPartitionMetadata(partIdent).get("paramKey") == "paramValue")

    partTable.dropPartition(partIdent)
    assert(!hasPartitions(partTable))
  }

  test("listPartitionIdentifiers") {
    val table = catalog.loadTable(ident)
    val partTable = new InMemoryPartitionTable(
      table.name(), table.schema(), table.partitioning(), table.properties())
    assert(!hasPartitions(partTable))

    val partIdent = InternalRow.apply("3")
    partTable.createPartition(partIdent, new util.HashMap[String, String]())
    assert(partTable.listPartitionIdentifiers(Array.empty, InternalRow.empty).length == 1)

    val partIdent1 = InternalRow.apply("4")
    partTable.createPartition(partIdent1, new util.HashMap[String, String]())
    assert(partTable.listPartitionIdentifiers(Array.empty, InternalRow.empty).length == 2)
    assert(partTable.listPartitionIdentifiers(Array("dt"), partIdent1).length == 1)

    partTable.dropPartition(partIdent)
    assert(partTable.listPartitionIdentifiers(Array.empty, InternalRow.empty).length == 1)
    partTable.dropPartition(partIdent1)
    assert(!hasPartitions(partTable))
  }

  private def createMultiPartTable(): InMemoryPartitionTable = {
    val partCatalog = new InMemoryPartitionTableCatalog
    partCatalog.initialize("test", CaseInsensitiveStringMap.empty())
    val table = partCatalog.createTable(
      ident,
      new StructType()
        .add("col0", IntegerType)
        .add("part0", IntegerType)
        .add("part1", StringType),
      Array(LogicalExpressions.identity(ref("part0")), LogicalExpressions.identity(ref("part1"))),
      util.Collections.emptyMap[String, String])

    val partTable = table.asInstanceOf[InMemoryPartitionTable]
    Seq(
      InternalRow(0, "abc"),
      InternalRow(0, "def"),
      InternalRow(1, "abc")).foreach { partIdent =>
      partTable.createPartition(partIdent, new util.HashMap[String, String]())
    }

    partTable
  }

  test("listPartitionByNames") {
    val partTable = createMultiPartTable()

    Seq(
      (Array("part0", "part1"), InternalRow(0, "abc")) -> Set(InternalRow(0, "abc")),
      (Array("part0"), InternalRow(0)) -> Set(InternalRow(0, "abc"), InternalRow(0, "def")),
      (Array("part1"), InternalRow("abc")) -> Set(InternalRow(0, "abc"), InternalRow(1, "abc")),
      (Array.empty[String], InternalRow.empty) ->
        Set(InternalRow(0, "abc"), InternalRow(0, "def"), InternalRow(1, "abc")),
      (Array("part0", "part1"), InternalRow(3, "xyz")) -> Set(),
      (Array("part1"), InternalRow(3.14f)) -> Set()
    ).foreach { case ((names, idents), expected) =>
      assert(partTable.listPartitionIdentifiers(names, idents).toSet === expected)
    }
    // Check invalid parameters
    Seq(
      (Array("part0", "part1"), InternalRow(0)),
      (Array("col0", "part1"), InternalRow(0, 1)),
      (Array("wrong"), InternalRow("invalid"))
    ).foreach { case (names, idents) =>
      intercept[AssertionError](partTable.listPartitionIdentifiers(names, idents))
    }
  }

  test("partitionExists") {
    val partTable = createMultiPartTable()

    assert(partTable.partitionExists(InternalRow(0, "def")))
    assert(!partTable.partitionExists(InternalRow(-1, "def")))
    assert(!partTable.partitionExists(InternalRow("abc", "def")))

    val errMsg = intercept[IllegalArgumentException] {
      partTable.partitionExists(InternalRow(0))
    }.getMessage
    assert(errMsg.contains("The identifier might not refer to one partition"))
  }

  test("renamePartition") {
    val partTable = createMultiPartTable()

    val errMsg1 = intercept[PartitionsAlreadyExistException] {
      partTable.renamePartition(InternalRow(0, "abc"), InternalRow(1, "abc"))
    }.getMessage
    assert(errMsg1.contains("partitions already exist"))

    val newPart = InternalRow(2, "xyz")
    val errMsg2 = intercept[NoSuchPartitionException] {
      partTable.renamePartition(newPart, InternalRow(3, "abc"))
    }.getMessage
    assert(errMsg2.contains("Partition not found"))

    assert(partTable.renamePartition(InternalRow(0, "abc"), newPart))
    assert(partTable.partitionExists(newPart))
  }

  test("truncatePartition") {
    val table = catalog.loadTable(ident)
    val partTable = new InMemoryPartitionTable(
      table.name(), table.schema(), table.partitioning(), table.properties())
    assert(!hasPartitions(partTable))

    val partIdent = InternalRow.apply("3")
    val partIdent1 = InternalRow.apply("4")
    partTable.createPartition(partIdent, new util.HashMap[String, String]())
    partTable.createPartition(partIdent1, new util.HashMap[String, String]())
    assert(partTable.listPartitionIdentifiers(Array.empty, InternalRow.empty).length == 2)

    partTable.withData(Array(
      new BufferedRows("3").withRow(InternalRow(0, "abc", "3")),
      new BufferedRows("4").withRow(InternalRow(1, "def", "4"))
    ))

    partTable.truncatePartition(partIdent)
    assert(partTable.listPartitionIdentifiers(Array.empty, InternalRow.empty).length == 2)
    assert(partTable.rows === InternalRow(1, "def", "4") :: Nil)
  }
}
