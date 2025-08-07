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

import org.apache.spark.sql.catalyst.util.{ParseMode, PermissiveMode}
import org.apache.spark.unsafe.types.UTF8String

object AvroExpressionEvalUtils {

  def schemaOfAvro(
      avroOptions: AvroOptions,
      parseMode: ParseMode,
      expectedSchema: Schema): UTF8String = {
    val dt = SchemaConverters.toSqlType(
      expectedSchema,
      avroOptions.useStableIdForUnionType,
      avroOptions.stableIdPrefixForUnionType,
      avroOptions.recursiveFieldMaxDepth).dataType
    val schema = parseMode match {
      // With PermissiveMode, the output Catalyst row might contain columns of null values for
      // corrupt records, even if some of the columns are not nullable in the user-provided schema.
      // Therefore we force the schema to be all nullable here.
      case PermissiveMode => dt.asNullable
      case _ => dt
    }
    UTF8String.fromString(schema.sql)
  }
}
