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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{PartitionAlreadyExistsException, PartitionsAlreadyExistException}
import org.apache.spark.sql.connector.catalog.SupportsAtomicPartitionManagement
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType

/**
 * This class is used to test SupportsAtomicPartitionManagement API.
 */
class InMemoryAtomicPartitionTable (
    name: String,
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryPartitionTable(name, schema, partitioning, properties)
  with SupportsAtomicPartitionManagement {

  override def createPartition(
      ident: InternalRow,
      properties: util.Map[String, String]): Unit = {
    if (memoryTablePartitions.containsKey(ident)) {
      throw new PartitionAlreadyExistsException(name, ident, partitionSchema)
    } else {
      createPartitionKey(ident.toSeq(schema))
      memoryTablePartitions.put(ident, properties)
    }
  }

  override def dropPartition(ident: InternalRow): Boolean = {
    if (memoryTablePartitions.containsKey(ident)) {
      memoryTablePartitions.remove(ident)
      removePartitionKey(ident.toSeq(schema))
      true
    } else {
      false
    }
  }

  override def createPartitions(
      idents: Array[InternalRow],
      properties: Array[util.Map[String, String]]): Unit = {
    if (idents.exists(partitionExists)) {
      throw new PartitionsAlreadyExistException(
        name, idents.filter(partitionExists), partitionSchema)
    }
    idents.zip(properties).foreach { case (ident, property) =>
      createPartition(ident, property)
    }
  }

  override def dropPartitions(idents: Array[InternalRow]): Boolean = {
    if (!idents.forall(partitionExists)) {
      return false;
    }
    idents.forall(dropPartition)
  }
}
