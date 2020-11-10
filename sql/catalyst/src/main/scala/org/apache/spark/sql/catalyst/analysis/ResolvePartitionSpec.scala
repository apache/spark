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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.plans.logical.{AlterTableAddPartition, AlterTableDropPartition, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.SupportsPartitionManagement
import org.apache.spark.sql.types._

/**
 * Resolve [[UnresolvedPartitionSpec]] to [[ResolvedPartitionSpec]] in partition related commands.
 */
object ResolvePartitionSpec extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case r @ AlterTableAddPartition(
        ResolvedTable(_, _, table: SupportsPartitionManagement), partSpecs, _) =>
      r.copy(parts = resolvePartitionSpecs(partSpecs, table.partitionSchema()))

    case r @ AlterTableDropPartition(
        ResolvedTable(_, _, table: SupportsPartitionManagement), partSpecs, _, _, _) =>
      r.copy(parts = resolvePartitionSpecs(partSpecs, table.partitionSchema()))
  }

  private def resolvePartitionSpecs(
      partSpecs: Seq[PartitionSpec], partSchema: StructType): Seq[ResolvedPartitionSpec] =
    partSpecs.map {
      case unresolvedPartSpec: UnresolvedPartitionSpec =>
        ResolvedPartitionSpec(
          convertToPartIdent(unresolvedPartSpec.spec, partSchema), unresolvedPartSpec.location)
      case resolvedPartitionSpec: ResolvedPartitionSpec =>
        resolvedPartitionSpec
    }

  private def convertToPartIdent(
      partSpec: TablePartitionSpec, partSchema: StructType): InternalRow = {
    val conflictKeys = partSpec.keys.toSeq.diff(partSchema.map(_.name))
    if (conflictKeys.nonEmpty) {
      throw new AnalysisException(s"Partition key ${conflictKeys.mkString(",")} not exists")
    }

    val partValues = partSchema.map { part =>
      val partValue = partSpec.get(part.name).orNull
      if (partValue == null) {
        null
      } else {
        // TODO: Support other datatypes, such as DateType
        part.dataType match {
          case _: ByteType =>
            partValue.toByte
          case _: ShortType =>
            partValue.toShort
          case _: IntegerType =>
            partValue.toInt
          case _: LongType =>
            partValue.toLong
          case _: FloatType =>
            partValue.toFloat
          case _: DoubleType =>
            partValue.toDouble
          case _: StringType =>
            partValue
          case _ =>
            throw new AnalysisException(
              s"Type ${part.dataType.typeName} is not supported for partition.")
        }
      }
    }
    InternalRow.fromSeq(partValues)
  }
}
