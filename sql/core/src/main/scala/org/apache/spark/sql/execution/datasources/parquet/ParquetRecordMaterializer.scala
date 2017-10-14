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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.parquet.io.api.{GroupConverter, RecordMaterializer}
import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.StructType

/**
 * A [[RecordMaterializer]] for Catalyst rows.
 *
 * @param parquetSchema Parquet schema of the records to be read
 * @param catalystSchema Catalyst schema of the rows to be constructed
 * @param schemaConverter A Parquet-Catalyst schema converter that helps initializing row converters
 */
private[parquet] class ParquetRecordMaterializer(
    parquetSchema: MessageType, catalystSchema: StructType, schemaConverter: ParquetSchemaConverter)
  extends RecordMaterializer[UnsafeRow] {

  private val rootConverter =
    new ParquetRowConverter(schemaConverter, parquetSchema, catalystSchema, NoopUpdater)

  override def getCurrentRecord: UnsafeRow = rootConverter.currentRecord

  override def getRootConverter: GroupConverter = rootConverter
}
