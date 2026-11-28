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
package org.apache.spark.sql.execution.datasources.parquet.types.ops

import java.time.ZoneId

import org.apache.parquet.io.api.Converter
import org.apache.parquet.schema.Type

import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.execution.datasources.parquet.{HasParentContainerUpdater, ParentContainerUpdater, ParquetToSparkSchemaConverter}
import org.apache.spark.sql.types.StructType

/**
 * Base trait for struct-backed framework types: types stored as a Parquet group whose fields are
 * [[structSchema]]. It ties together the two decisions such a type would otherwise make
 * separately, declaring the struct schema and overriding the extended `newConverter`:
 *
 *   - [[structSchema]] is abstract and defines [[parquetStructSchema]], which is final.
 *   - The extended `newConverter` overload is abstract again, so a subclass implements the one
 *     that receives the schema converter, the time zone and the rebase specs.
 *   - The simple `newConverter` overload, which lacks that context and is never called for a
 *     struct-backed type, fails.
 *
 * The vectorized reader reads the group through [[structSchema]], so the updaters are those of
 * its fields: a struct-backed type that returns true from `isBatchReadSupported` supplies no
 * `getVectorUpdater`.
 */
private[parquet] trait StructBackedParquetTypeOps extends ParquetTypeOps {

  /** The Spark struct whose fields define the Parquet group layout of this type. */
  def structSchema: StructType

  final override def parquetStructSchema: Option[StructType] = Some(structSchema)

  final override def newConverter(
      parquetType: Type,
      updater: ParentContainerUpdater): Converter with HasParentContainerUpdater =
    throw new UnsupportedOperationException(
      s"${getClass.getSimpleName} is struct-backed; use the extended newConverter overload")

  override def newConverter(
      parquetType: Type,
      updater: ParentContainerUpdater,
      schemaConverter: ParquetToSparkSchemaConverter,
      convertTz: Option[ZoneId],
      datetimeRebaseSpec: RebaseSpec,
      int96RebaseSpec: RebaseSpec): Converter with HasParentContainerUpdater
}
