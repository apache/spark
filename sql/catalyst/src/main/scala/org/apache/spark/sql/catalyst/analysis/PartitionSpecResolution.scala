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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.catalyst.plans.logical.V2PartitionCommand
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.SupportsPartitionManagement
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.PartitioningUtils.{castPartitionSpec, normalizePartitionSpec, requireExactMatchedPartitionSpec}
import org.apache.spark.util.ArrayImplicits._

/** Resolves and validates partition specifications for table commands. */
private[sql] object PartitionSpecResolution {
  def resolvePartitionSpec(
      tableName: String,
      partSpec: UnresolvedPartitionSpec,
      partSchema: StructType,
      allowPartitionSpec: Boolean,
      conf: => SQLConf): ResolvedPartitionSpec = {
    val normalizedSpec = normalizePartitionSpec(
      partSpec.spec,
      partSchema,
      tableName,
      conf.resolver)
    if (!allowPartitionSpec) {
      requireExactMatchedPartitionSpec(tableName, normalizedSpec,
        partSchema.fieldNames.toImmutableArraySeq)
    }
    val partitionNames = normalizedSpec.keySet
    val requestedFields = partSchema.filter(field => partitionNames.contains(field.name))
    ResolvedPartitionSpec(
      requestedFields.map(_.name),
      convertToPartIdent(normalizedSpec, requestedFields, conf),
      partSpec.location)
  }

  def convertToPartIdent(
      partitionSpec: TablePartitionSpec,
      schema: Seq[StructField],
      conf: => SQLConf): InternalRow = {
    val partValues = schema.map { part =>
      val raw = partitionSpec.get(part.name).orNull
      val dt = CharVarcharUtils.replaceCharVarcharWithString(part.dataType)
      if (conf.getConf(SQLConf.SKIP_TYPE_VALIDATION_ON_ALTER_PARTITION)) {
        Cast(Literal.create(raw, StringType), dt, Some(conf.sessionLocalTimeZone)).eval()
      } else {
        castPartitionSpec(raw, dt, conf).eval()
      }
    }
    InternalRow.fromSeq(partValues)
  }

  /** Validates that the target supports partition management with a nonempty schema. */
  def validate(
      command: V2PartitionCommand,
      errors: QueryErrorsBase): Unit = {
    command.table match {
      case r @ ResolvedTable(_, _, table, _) => table match {
        case t: SupportsPartitionManagement =>
          if (t.partitionSchema.isEmpty) {
            r.failAnalysis(
              errorClass = "INVALID_PARTITION_OPERATION.PARTITION_SCHEMA_IS_EMPTY",
              messageParameters = Map("name" -> errors.toSQLId(r.name)))
          }
        case _ =>
          r.failAnalysis(
            errorClass = "INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED",
            messageParameters = Map("name" -> errors.toSQLId(r.name)))
      }
      case _ =>
    }
  }
}
