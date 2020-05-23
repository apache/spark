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

package org.apache.spark.sql.connector

import java.{lang, util}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsPartitions, TablePartition}


/**
 * This class is used to test SupportsPartitions API.
 */
class InMemoryPartitionCatalog extends InMemoryTableCatalog with SupportsPartitions {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  protected val memoryTablePartitions: util.Map[Identifier, mutable.HashSet[TablePartition]] =
    new ConcurrentHashMap[Identifier, mutable.HashSet[TablePartition]]()

  def createPartitions(
      ident: Identifier,
      partitions: Array[TablePartition],
      ignoreIfExists: lang.Boolean = false): Unit = {
    assert(tableExists(ident))
    val table = loadTable(ident).asInstanceOf[InMemoryTable]
    val tableSchema = table.schema.map(_.name)
    checkPartitionKeysExists(tableSchema, partitions.map(_.getPartitionSpec))
    val tablePartitions =
      memoryTablePartitions.getOrDefault(ident, new mutable.HashSet[TablePartition]())
    partitions.foreach(tablePartitions.add)
    memoryTablePartitions.put(ident, tablePartitions)
  }

  def dropPartitions(
      ident: Identifier,
      partitions: Array[util.Map[String, String]],
      ignoreIfNotExists: lang.Boolean = false): Unit = {
    assert(tableExists(ident))
    val table = loadTable(ident).asInstanceOf[InMemoryTable]
    val tableSchema = table.schema.map(_.name)
    checkPartitionKeysExists(tableSchema, partitions)
    if (memoryTablePartitions.containsKey(ident)) {
      val tablePartitions = memoryTablePartitions.get(ident)
      tablePartitions.filter { tablePartition =>
        partitions.contains(tablePartition.getPartitionSpec)
      }.foreach(tablePartitions.remove)
      memoryTablePartitions.put(ident, tablePartitions)
    }
  }

  def renamePartitions(
      ident: Identifier,
      oldPartitions: Array[util.Map[String, String]],
      newPartitions: Array[util.Map[String, String]]): Unit = {
    assert(tableExists(ident))
    val table = loadTable(ident).asInstanceOf[InMemoryTable]
    val tableSchema = table.schema.map(_.name)
    checkPartitionKeysExists(tableSchema, oldPartitions)
    checkPartitionKeysExists(tableSchema, newPartitions)
    if (memoryTablePartitions.containsKey(ident)) {
      val tablePartitions = memoryTablePartitions.get(ident)
      for (oldPartition <- oldPartitions;
          newPartition <- newPartitions) {
        tablePartitions.filter { tablePartition =>
          oldPartition == tablePartition.getPartitionSpec
        }.foreach { tablePartition =>
          tablePartition.setPartitionSpec(newPartition)
        }
      }
      memoryTablePartitions.put(ident, tablePartitions)
    }
  }

  def alterPartitions(
      ident: Identifier,
      partitions: Array[TablePartition]): Unit = {
    assert(tableExists(ident))
    assert(tableExists(ident))
    val table = loadTable(ident).asInstanceOf[InMemoryTable]
    val tableSchema = table.schema.map(_.name)
    checkPartitionKeysExists(tableSchema, partitions.map(_.getPartitionSpec))
    if (memoryTablePartitions.containsKey(ident)) {
      val tablePartitions = memoryTablePartitions.get(ident)
      partitions.foreach { partition =>
        tablePartitions.filter(_.getPartitionSpec == partition.getPartitionSpec)
          .foreach(tablePartitions.remove)
        tablePartitions.add(partition)
      }
    }
  }

  def getPartition(
      ident: Identifier,
      partition: util.Map[String, String]): TablePartition = {
    assert(tableExists(ident))
    val table = loadTable(ident).asInstanceOf[InMemoryTable]
    val tableSchema = table.schema.map(_.name)
    checkPartitionKeysExists(tableSchema, Array(partition))
    memoryTablePartitions.getOrDefault(ident, mutable.HashSet.empty[TablePartition])
      .find(_.getPartitionSpec == partition)
      .getOrElse {
        throw new NoSuchPartitionException(
          ident.namespace().quoted,
          ident.name(),
          partition.asScala.toMap)
      }
  }

  def listPartitionNames(
      ident: Identifier,
      partition: util.Map[String, String] = new util.HashMap()): Array[String] = {
    assert(tableExists(ident))
    memoryTablePartitions.getOrDefault(ident, mutable.HashSet.empty[TablePartition])
      .filter { tablePartition =>
        partition.asScala.toSet.subsetOf(tablePartition.getPartitionSpec.asScala.toSet)
      }.map { tablePartition =>
      tablePartition.getPartitionSpec.asScala.map { kv =>
        s"${kv._1}=${kv._2}"
      }.mkString("/")
    }.toArray
  }

  def listPartitions(
      ident: Identifier,
      partition: util.Map[String, String] = new util.HashMap()): Array[TablePartition] = {
    assert(tableExists(ident))
    memoryTablePartitions.getOrDefault(ident, mutable.HashSet.empty[TablePartition])
      .filter { tablePartition =>
        partition.asScala.toSet.subsetOf(tablePartition.getPartitionSpec.asScala.toSet)
      }.toArray
  }

  private def checkPartitionKeysExists(
      tableSchema: Seq[String],
      partitions: Array[util.Map[String, String]]): Unit = {
    partitions.foreach { partition =>
      val errorPartitionKeys = partition.keySet().asScala.filterNot(tableSchema.contains)
      if (errorPartitionKeys.nonEmpty) {
        throw new IllegalArgumentException(
          s"Partition Keys not exists, table schema: ${tableSchema.mkString("{", ",", "}")}")
      }
    }
  }
}
