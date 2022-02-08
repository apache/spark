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
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionException, PartitionAlreadyExistsException}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType

/**
 * This class is used to test SupportsPartitionManagement API.
 */
class InMemoryPartitionTable(
    name: String,
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryTable(name, schema, partitioning, properties) with SupportsPartitionManagement {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  protected val memoryTablePartitions: util.Map[InternalRow, util.Map[String, String]] =
    new ConcurrentHashMap[InternalRow, util.Map[String, String]]()

  def partitionSchema: StructType = {
    val partitionColumnNames = partitioning.toSeq.convertTransforms._1
    new StructType(schema.filter(p => partitionColumnNames.contains(p.name)).toArray)
  }

  def createPartition(
      ident: InternalRow,
      properties: util.Map[String, String]): Unit = {
    if (memoryTablePartitions.containsKey(ident)) {
      throw new PartitionAlreadyExistsException(name, ident, partitionSchema)
    } else {
      createPartitionKey(ident.toSeq(schema))
      memoryTablePartitions.put(ident, properties)
    }
  }

  def dropPartition(ident: InternalRow): Boolean = {
    if (memoryTablePartitions.containsKey(ident)) {
      memoryTablePartitions.remove(ident)
      removePartitionKey(ident.toSeq(schema))
      true
    } else {
      false
    }
  }

  def replacePartitionMetadata(ident: InternalRow, properties: util.Map[String, String]): Unit = {
    if (memoryTablePartitions.containsKey(ident)) {
      memoryTablePartitions.put(ident, properties)
    } else {
      throw new NoSuchPartitionException(name, ident, partitionSchema)
    }
  }

  def loadPartitionMetadata(ident: InternalRow): util.Map[String, String] = {
    if (memoryTablePartitions.containsKey(ident)) {
      memoryTablePartitions.get(ident)
    } else {
      throw new NoSuchPartitionException(name, ident, partitionSchema)
    }
  }

  override protected def addPartitionKey(key: Seq[Any]): Unit = {
    memoryTablePartitions.putIfAbsent(InternalRow.fromSeq(key), Map.empty[String, String].asJava)
  }

  override def listPartitionIdentifiers(
      names: Array[String],
      ident: InternalRow): Array[InternalRow] = {
    assert(names.length == ident.numFields,
      s"Number of partition names (${names.length}) must be equal to " +
      s"the number of partition values (${ident.numFields}).")
    val schema = partitionSchema
    assert(names.forall(fieldName => schema.fieldNames.contains(fieldName)),
      s"Some partition names ${names.mkString("[", ", ", "]")} don't belong to " +
      s"the partition schema '${schema.sql}'.")
    val indexes = names.map(schema.fieldIndex)
    val dataTypes = names.map(schema(_).dataType)
    val currentRow = new GenericInternalRow(new Array[Any](names.length))
    memoryTablePartitions.keySet().asScala.filter { key =>
      for (i <- 0 until names.length) {
        currentRow.values(i) = key.get(indexes(i), dataTypes(i))
      }
      currentRow == ident
    }.toArray
  }

  override def renamePartition(from: InternalRow, to: InternalRow): Boolean = {
    if (memoryTablePartitions.containsKey(to)) {
      throw new PartitionAlreadyExistsException(name, to, partitionSchema)
    } else {
      val partValue = memoryTablePartitions.remove(from)
      if (partValue == null) {
        throw new NoSuchPartitionException(name, from, partitionSchema)
      }
      memoryTablePartitions.put(to, partValue) == null &&
        renamePartitionKey(partitionSchema, from.toSeq(schema), to.toSeq(schema))
    }
  }

  override def truncatePartition(ident: InternalRow): Boolean = {
    if (memoryTablePartitions.containsKey(ident)) {
      clearPartition(ident.toSeq(schema))
      true
    } else {
      throw new NoSuchPartitionException(name, ident, partitionSchema)
    }
  }
}
