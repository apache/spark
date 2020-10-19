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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.JavaConverters._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{PartitionSpec, ResolvedPartitionSpec, UnresolvedPartitionSpec}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.connector.catalog.{SupportsAtomicPartitionManagement, SupportsDelete, SupportsPartitionManagement, SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.types.{ByteType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

object DataSourceV2Implicits {
  implicit class TableHelper(table: Table) {
    def asReadable: SupportsRead = {
      table match {
        case support: SupportsRead =>
          support
        case _ =>
          throw new AnalysisException(s"Table does not support reads: ${table.name}")
      }
    }

    def asWritable: SupportsWrite = {
      table match {
        case support: SupportsWrite =>
          support
        case _ =>
          throw new AnalysisException(s"Table does not support writes: ${table.name}")
      }
    }

    def asDeletable: SupportsDelete = {
      table match {
        case support: SupportsDelete =>
          support
        case _ =>
          throw new AnalysisException(s"Table does not support deletes: ${table.name}")
      }
    }

    def asPartitionable: SupportsPartitionManagement = {
      table match {
        case support: SupportsPartitionManagement =>
          support
        case _ =>
          throw new AnalysisException(
            s"Table does not support partition management: ${table.name}")
      }
    }

    def asAtomicPartitionable: SupportsAtomicPartitionManagement = {
      table match {
        case support: SupportsAtomicPartitionManagement =>
          support
        case _ =>
          throw new AnalysisException(
            s"Table does not support atomic partition management: ${table.name}")
      }
    }

    def supports(capability: TableCapability): Boolean = table.capabilities.contains(capability)

    def supportsAny(capabilities: TableCapability*): Boolean = capabilities.exists(supports)
  }

  implicit class OptionsHelper(options: Map[String, String]) {
    def asOptions: CaseInsensitiveStringMap = {
      new CaseInsensitiveStringMap(options.asJava)
    }
  }

  implicit class PartitionSpecsHelper(partSpecs: Seq[PartitionSpec]) {
    def resolved: Boolean = partSpecs.forall(_.isInstanceOf[ResolvedPartitionSpec])

    def asResolved(partSchema: StructType): Seq[ResolvedPartitionSpec] =
      partSpecs.asInstanceOf[Seq[UnresolvedPartitionSpec]]
        .map { unresolvedPartSpec =>
          ResolvedPartitionSpec(
            unresolvedPartSpec.spec.asPartitionIdentifier(partSchema),
            unresolvedPartSpec.location)
        }
  }

  implicit class TablePartitionSpecHelper(partSpec: TablePartitionSpec) {
    def asPartitionIdentifier(partSchema: StructType): InternalRow = {
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
}
