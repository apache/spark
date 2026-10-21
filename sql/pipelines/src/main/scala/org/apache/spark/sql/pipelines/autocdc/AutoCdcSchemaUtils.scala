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

package org.apache.spark.sql.pipelines.autocdc

import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.types.StructType

private[autocdc] object AutoCdcSchemaUtils {

  /**
   * Returns `schema` without fields matching `columnNamesToExclude`, preserving field order.
   */
  def excludeColumns(
      schema: StructType,
      columnNamesToExclude: Iterable[String],
      resolver: Resolver): StructType =
    StructType(schema.fields.filterNot(field =>
      columnNamesToExclude.exists(resolver(_, field.name))))

  /**
   * Returns field-name paths after recursively flattening nested structs, preserving schema order.
   *
   * Fields of every other data type, including arrays and maps, remain a single path. Their
   * element, key, and value types are not traversed.
   */
  def flattenStructFieldPaths(schema: StructType): Seq[Seq[String]] =
    schema.fields.toSeq.flatMap { field =>
      field.dataType match {
        case nested: StructType =>
          flattenStructFieldPaths(nested).map(field.name +: _)
        case _ => Seq(Seq(field.name))
      }
    }
}
