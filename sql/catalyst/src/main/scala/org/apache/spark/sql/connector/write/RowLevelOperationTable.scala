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

package org.apache.spark.sql.connector.write

import java.util

import org.apache.spark.sql.connector.catalog.{Column, SupportsRead, SupportsRowLevelOperations, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * An internal v2 table implementation that wraps the original table and a logical row-level
 * operation for DELETE, UPDATE, MERGE commands that require rewriting data.
 *
 * The purpose of this table is to make the existing scan and write planning rules work
 * with commands that require coordination between the scan and the write (so that the write
 * knows what to replace).
 */
private[sql] case class RowLevelOperationTable(
    table: Table with SupportsRowLevelOperations,
    operation: RowLevelOperation) extends Table with SupportsRead with SupportsWrite {

  override def name: String = table.name
  override def schema: StructType = table.schema
  override def columns: Array[Column] = table.columns()
  override def capabilities: util.Set[TableCapability] = table.capabilities
  override def toString: String = table.toString

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    operation.newScanBuilder(options)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    operation.newWriteBuilder(info)
  }
}
