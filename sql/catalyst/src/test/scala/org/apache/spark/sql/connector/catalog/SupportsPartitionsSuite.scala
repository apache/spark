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
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.connector.InMemoryPartitionCatalog
import org.apache.spark.sql.connector.expressions.{LogicalExpressions, NamedReference}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class SupportsPartitionsSuite extends SparkFunSuite {

  private val ident: Identifier = Identifier.of(Array("ns"), "test_table")
  def ref(name: String): NamedReference = LogicalExpressions.parseReference(name)

  private val catalog: InMemoryPartitionCatalog = {
    val newCatalog = new InMemoryPartitionCatalog
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

  test("createPartitions") {
    assert(catalog.listPartitionNames(ident).isEmpty)
    val part = new TablePartition(Map("dt" -> "3").asJava, new util.HashMap[String, String]())
    catalog.createPartitions(ident, Array(part))
    assert(catalog.listPartitionNames(ident).nonEmpty)
    val partition = catalog.getPartition(ident, Map("dt" -> "3").asJava)
    assert(partition.getPartitionSpec.get("dt") == "3")
    catalog.dropPartitions(ident, Array(Map("dt" -> "3").asJava))
    assert(catalog.listPartitionNames(ident).isEmpty)
  }

  test("dropPartitions") {
    assert(catalog.listPartitionNames(ident).isEmpty)
    val part = new TablePartition(Map("dt" -> "3").asJava, new util.HashMap[String, String]())
    val part1 = new TablePartition(Map("dt" -> "4").asJava, new util.HashMap[String, String]())
    catalog.createPartitions(ident, Array(part))
    catalog.createPartitions(ident, Array(part1))
    assert(catalog.listPartitionNames(ident).length == 2)
    catalog.dropPartitions(ident, Array(Map("dt" -> "3").asJava))
    assert(catalog.listPartitionNames(ident).length == 1)
    catalog.dropPartitions(ident, Array(Map("dt" -> "4").asJava))
    assert(catalog.listPartitionNames(ident).isEmpty)
  }

  test("renamePartition") {
    assert(catalog.listPartitionNames(ident).isEmpty)
    val part = new TablePartition(Map("dt" -> "3").asJava, new util.HashMap[String, String]())
    catalog.createPartitions(ident, Array(part))
    assert(catalog.listPartitionNames(ident).nonEmpty)
    val partition = catalog.getPartition(ident, Map("dt" -> "3").asJava)
    assert(partition.getPartitionSpec.get("dt") == "3")
    catalog.renamePartitions(
      ident,
      Array(Map("dt" -> "3").asJava),
      Array(Map("dt" -> "4").asJava))
    val partition1 = catalog.getPartition(ident, Map("dt" -> "4").asJava)
    assert(partition1.getPartitionSpec.get("dt") == "4")
    assertThrows[NoSuchPartitionException](
      catalog.getPartition(ident, Map("dt" -> "3").asJava))
    catalog.dropPartitions(ident, Array(Map("dt" -> "4").asJava))
    assert(catalog.listPartitionNames(ident).isEmpty)
  }

  test("alterPartition") {
    assert(catalog.listPartitionNames(ident).isEmpty)
    val part = new TablePartition(Map("dt" -> "3").asJava, new util.HashMap[String, String]())
    catalog.createPartitions(ident, Array(part))
    assert(catalog.listPartitionNames(ident).nonEmpty)
    val partition = catalog.getPartition(ident, Map("dt" -> "3").asJava)
    assert(partition.getPartitionSpec.get("dt") == "3")
    assert(partition.getParametes.isEmpty)
    val part1 = new TablePartition(Map("dt" -> "3").asJava, Map("dt" -> "3").asJava)
    catalog.alterPartitions(ident, Array(part1))
    assert(catalog.listPartitionNames(ident).nonEmpty)
    val partition1 = catalog.getPartition(ident, Map("dt" -> "3").asJava)
    assert(partition1.getPartitionSpec.get("dt") == "3")
    assert(!partition1.getParametes.isEmpty)
    assert(partition1.getParametes.get("dt") == "3")
    catalog.dropPartitions(ident, Array(Map("dt" -> "3").asJava))
    assert(catalog.listPartitionNames(ident).isEmpty)
  }

  test("getPartition") {
    assert(catalog.listPartitionNames(ident).isEmpty)
    val part = new TablePartition(Map("dt" -> "3").asJava, new util.HashMap[String, String]())
    catalog.createPartitions(ident, Array(part))
    assert(catalog.listPartitionNames(ident).nonEmpty)
    val partition = catalog.getPartition(ident, Map("dt" -> "3").asJava)
    assert(partition.getPartitionSpec.get("dt") == "3")
    assertThrows[NoSuchPartitionException](
      catalog.getPartition(ident, Map("dt" -> "4").asJava))
    catalog.dropPartitions(ident, Array(Map("dt" -> "3").asJava))
    assert(catalog.listPartitionNames(ident).isEmpty)
  }

  test("listPartitionNames") {
    assert(catalog.listPartitionNames(ident).isEmpty)
    val part = new TablePartition(Map("dt" -> "3").asJava, new util.HashMap[String, String]())
    catalog.createPartitions(ident, Array(part))
    assert(catalog.listPartitionNames(ident).nonEmpty)
    val partition = catalog.getPartition(ident, Map("dt" -> "3").asJava)
    assert(partition.getPartitionSpec.get("dt") == "3")
    val partitionNames = catalog.listPartitionNames(ident)
    assert(partitionNames.nonEmpty)
    assert(partitionNames.length == 1)
    val part1 = new TablePartition(Map("dt" -> "4").asJava, new util.HashMap[String, String]())
    catalog.createPartitions(ident, Array(part1))
    val partitionNames1 = catalog.listPartitionNames(ident)
    assert(partitionNames1.nonEmpty)
    assert(partitionNames1.length == 2)
    val partitionNames2 = catalog.listPartitionNames(ident, Map("dt" -> "3").asJava)
    assert(partitionNames2.nonEmpty)
    assert(partitionNames2.length == 1)
    val partitionNames3 = catalog.listPartitionNames(ident, Map("dt" -> "5").asJava)
    assert(partitionNames3.isEmpty)
    catalog.dropPartitions(ident, Array(Map("dt" -> "3").asJava))
    catalog.dropPartitions(ident, Array(Map("dt" -> "4").asJava))
    assert(catalog.listPartitionNames(ident).isEmpty)
  }

  test("listPartitions") {
    assert(catalog.listPartitions(ident).isEmpty)
    val part = new TablePartition(Map("dt" -> "3").asJava, new util.HashMap[String, String]())
    catalog.createPartitions(ident, Array(part))
    assert(catalog.listPartitions(ident).nonEmpty)
    val partition = catalog.getPartition(ident, Map("dt" -> "3").asJava)
    assert(partition.getPartitionSpec.get("dt") == "3")
    val partitions = catalog.listPartitions(ident)
    assert(partitions.nonEmpty)
    assert(partitions.length == 1)
    val part1 = new TablePartition(Map("dt" -> "4").asJava, new util.HashMap[String, String]())
    catalog.createPartitions(ident, Array(part1))
    val partitions1 = catalog.listPartitions(ident)
    assert(partitions1.nonEmpty)
    assert(partitions1.length == 2)
    val partitions2 = catalog.listPartitions(ident, Map("dt" -> "3").asJava)
    assert(partitions2.nonEmpty)
    assert(partitions2.length == 1)
    val partitions3 = catalog.listPartitions(ident, Map("dt" -> "5").asJava)
    assert(partitions3.isEmpty)
    catalog.dropPartitions(ident, Array(Map("dt" -> "3").asJava))
    catalog.dropPartitions(ident, Array(Map("dt" -> "4").asJava))
    assert(catalog.listPartitions(ident).isEmpty)
  }
}
