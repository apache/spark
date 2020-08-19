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

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionException, PartitionAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.SupportsPartitionManagement
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
    val partitionColumnNames = partitioning.toSeq.asPartitionColumns
    new StructType(schema.filter(p => partitionColumnNames.contains(p.name)).toArray)
  }

  def createPartition(
      ident: InternalRow,
      properties: util.Map[String, String]): Unit = {
    if (memoryTablePartitions.containsKey(ident)) {
      throw new PartitionAlreadyExistsException(name, ident, partitionSchema)
    } else {
      memoryTablePartitions.put(ident, properties)
    }
  }

  def dropPartition(ident: InternalRow): Boolean = {
    if (memoryTablePartitions.containsKey(ident)) {
      memoryTablePartitions.remove(ident)
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

  def listPartitionIdentifiers(ident: InternalRow): Array[InternalRow] = {
    val prefixPartCols =
      new StructType(partitionSchema.dropRight(partitionSchema.length - ident.numFields).toArray)
    val prefixPart = ident.toSeq(prefixPartCols)
    memoryTablePartitions.keySet().asScala
      .filter(_.toSeq(partitionSchema).startsWith(prefixPart)).toArray
  }

  override def partitionExists(ident: InternalRow): Boolean =
    memoryTablePartitions.containsKey(ident)
}
