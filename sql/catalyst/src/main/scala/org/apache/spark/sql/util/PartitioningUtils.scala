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

package org.apache.spark.sql.util

import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.DEFAULT_PARTITION_NAME
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.types.{CharType, DataType, StringType, StructField, StructType, VarcharType}
import org.apache.spark.unsafe.types.UTF8String

private[sql] object PartitioningUtils {

  def castPartitionSpec(value: String, dt: DataType, conf: SQLConf): Expression = {
    conf.storeAssignmentPolicy match {
      // SPARK-30844: try our best to follow StoreAssignmentPolicy for static partition
      // values but not completely follow because we can't do static type checking due to
      // the reason that the parser has erased the type info of static partition values
      // and converted them to string.
      case StoreAssignmentPolicy.ANSI | StoreAssignmentPolicy.STRICT =>
        val cast = Cast(Literal(value), dt, Option(conf.sessionLocalTimeZone),
          ansiEnabled = true)
        cast.setTagValue(Cast.BY_TABLE_INSERTION, ())
        cast
      case _ =>
        Cast(Literal(value), dt, Option(conf.sessionLocalTimeZone),
          ansiEnabled = false)
    }
  }

  private def normalizePartitionStringValue(value: String, field: StructField): String = {
    val casted = Cast(
      castPartitionSpec(value, field.dataType, SQLConf.get),
      StringType,
      Option(SQLConf.get.sessionLocalTimeZone)
    ).eval()
    if (casted != null) {
      casted.asInstanceOf[UTF8String].toString
    } else {
      null
    }
  }

  /**
   * Normalize the column names in partition specification, w.r.t. the real partition column names
   * and case sensitivity. e.g., if the partition spec has a column named `monTh`, and there is a
   * partition column named `month`, and it's case insensitive, we will normalize `monTh` to
   * `month`.
   */
  def normalizePartitionSpec[T](
      partitionSpec: Map[String, T],
      partCols: StructType,
      tblName: String,
      resolver: Resolver): Map[String, T] = {
    val rawSchema = CharVarcharUtils.getRawSchema(partCols, SQLConf.get)
    val normalizedPartSpec = partitionSpec.toSeq.map { case (key, value) =>
      val normalizedFiled = rawSchema.find(f => resolver(f.name, key)).getOrElse {
        throw QueryCompilationErrors.invalidPartitionColumnKeyInTableError(key, tblName)
      }

      val normalizedVal =
        if (SQLConf.get.charVarcharAsString) value else normalizedFiled.dataType match {
          case CharType(len) if value != null && value != DEFAULT_PARTITION_NAME =>
            val v = value match {
              case Some(str: String) => Some(charTypeWriteSideCheck(str, len))
              case str: String => charTypeWriteSideCheck(str, len)
              case other => other
            }
            v.asInstanceOf[T]
          case VarcharType(len) if value != null && value != DEFAULT_PARTITION_NAME =>
            val v = value match {
              case Some(str: String) => Some(varcharTypeWriteSideCheck(str, len))
              case str: String => varcharTypeWriteSideCheck(str, len)
              case other => other
            }
            v.asInstanceOf[T]
          case _ if !SQLConf.get.getConf(SQLConf.SKIP_TYPE_VALIDATION_ON_ALTER_PARTITION) &&
              value != null && value != DEFAULT_PARTITION_NAME =>
            val v = value match {
              case Some(str: String) => Some(normalizePartitionStringValue(str, normalizedFiled))
              case str: String => normalizePartitionStringValue(str, normalizedFiled)
              case other => other
            }
            v.asInstanceOf[T]
          case _ => value
        }
      normalizedFiled.name -> normalizedVal
    }

    SchemaUtils.checkColumnNameDuplication(
      normalizedPartSpec.map(_._1), "in the partition schema", resolver)

    normalizedPartSpec.toMap
  }

  private def charTypeWriteSideCheck(inputStr: String, limit: Int): String = {
    val toUtf8 = UTF8String.fromString(inputStr)
    CharVarcharCodegenUtils.charTypeWriteSideCheck(toUtf8, limit).toString
  }

  private def varcharTypeWriteSideCheck(inputStr: String, limit: Int): String = {
    val toUtf8 = UTF8String.fromString(inputStr)
    CharVarcharCodegenUtils.varcharTypeWriteSideCheck(toUtf8, limit).toString
  }

  /**
   * Verify if the input partition spec exactly matches the existing defined partition spec
   * The columns must be the same but the orders could be different.
   */
  def requireExactMatchedPartitionSpec(
      tableName: String,
      spec: TablePartitionSpec,
      partitionColumnNames: Seq[String]): Unit = {
    val defined = partitionColumnNames.sorted
    if (spec.keys.toSeq.sorted != defined) {
      throw QueryCompilationErrors.invalidPartitionSpecError(spec.keys.mkString(", "),
        partitionColumnNames, tableName)
    }
  }
}
