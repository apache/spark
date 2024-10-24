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

package org.apache.spark.sql.avro

import org.apache.avro.Schema

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.core.avro.{SchemaConverters => CoreSchemaConverters}

@deprecated("Use org.apache.spark.sql.core.avro.SchemaConverters instead", "4.0.0")
@Evolving
object DeprecatedSchemaConverters {
  @deprecated("Use org.apache.spark.sql.core.avro.SchemaConverters.SchemaType instead", "4.0.0")
  type SchemaType = CoreSchemaConverters.SchemaType

  @deprecated("Use org.apache.spark.sql.core.avro.SchemaConverters.toSqlType instead", "4.0.0")
  def toSqlType(avroSchema: Schema): SchemaType = {
    CoreSchemaConverters.toSqlType(avroSchema)
  }

  @deprecated("Use org.apache.spark.sql.core.avro.SchemaConverters.toSqlType instead", "4.0.0")
  def toSqlType(
      avroSchema: Schema,
      useStableIdForUnionType: Boolean,
      stableIdPrefixForUnionType: String,
      recursiveFieldMaxDepth: Int = -1): SchemaType = {
    CoreSchemaConverters.toSqlType(
      avroSchema,
      useStableIdForUnionType,
      stableIdPrefixForUnionType,
      recursiveFieldMaxDepth)
  }

  @deprecated("Use org.apache.spark.sql.core.avro.SchemaConverters.toAvroType instead", "4.0.0")
  def toAvroType(
      catalystType: org.apache.spark.sql.types.DataType,
      nullable: Boolean = false,
      recordName: String = "topLevelRecord",
      nameSpace: String = ""): Schema = {
    CoreSchemaConverters.toAvroType(catalystType, nullable, recordName, nameSpace)
  }
}
