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
package org.apache.spark.sql.execution.datasources.v2.python

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, STREAMING_WRITE, TRUNCATE}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap


class PythonTable(
    ds: PythonDataSourceV2,
    shortName: String,
    outputSchema: StructType
  ) extends Table with SupportsRead with SupportsWrite {
  override def name(): String = shortName

  override def capabilities(): java.util.Set[TableCapability] = java.util.EnumSet.of(
    BATCH_READ, BATCH_WRITE, STREAMING_WRITE, TRUNCATE)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new PythonScanBuilder(ds, shortName, outputSchema, options)
  }

  override def schema(): StructType = outputSchema

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new PythonWriteBuilder(ds, shortName, info)
  }
}
