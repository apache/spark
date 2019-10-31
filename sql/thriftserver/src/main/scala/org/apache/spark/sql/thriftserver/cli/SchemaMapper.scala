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

package org.apache.spark.sql.thriftserver.cli

import scala.collection.JavaConverters._

import org.apache.spark.sql.thriftserver.cli.thrift.{TCLIServiceConstants, TTableSchema, TTypeEntry, TTypeId}
import org.apache.spark.sql.types._

private[thriftserver] object SchemaMapper {

  def toTTableSchema(fields: StructType): TTableSchema = {
    val tTableSchema = new TTableSchema
    if (fields != null) {
      fields.zipWithIndex.map {
        case (field, i) => ColumnDescriptor(field, i)
      }.map(_.toTColumnDesc).foreach(tTableSchema.addToColumns)
    }
    tTableSchema
  }

  def toStructType(fields: TTableSchema): StructType = {
    var schema = new StructType()
    if (fields != null) {
      val posToField: Map[Int, StructField] = fields.getColumns.asScala.map { tColumn =>
        tColumn.position ->
          new StructField(tColumn.columnName,
            toDataType(tColumn.typeDesc.getTypes.get(0)))
      }.toMap
      posToField.keys.toSeq.sorted.foreach { pos =>
        schema = schema.add(posToField(pos))
      }
    }
    schema
  }

  def toTTypeId(typ: DataType): TTypeId = typ match {
    case NullType => TTypeId.NULL_TYPE
    case BooleanType => TTypeId.BOOLEAN_TYPE
    case ByteType => TTypeId.TINYINT_TYPE
    case ShortType => TTypeId.SMALLINT_TYPE
    case IntegerType => TTypeId.INT_TYPE
    case LongType => TTypeId.BIGINT_TYPE
    case FloatType => TTypeId.FLOAT_TYPE
    case DoubleType => TTypeId.DOUBLE_TYPE
    case StringType => TTypeId.STRING_TYPE
    case DecimalType() => TTypeId.DECIMAL_TYPE
    case DateType => TTypeId.DATE_TYPE
    case TimestampType => TTypeId.TIMESTAMP_TYPE
    case BinaryType => TTypeId.BINARY_TYPE
    case _: ArrayType => TTypeId.ARRAY_TYPE
    case _: MapType => TTypeId.MAP_TYPE
    case _: StructType => TTypeId.STRUCT_TYPE
    case _: CalendarIntervalType => TTypeId.STRING_TYPE
    case _: UserDefinedType[_] => TTypeId.USER_DEFINED_TYPE
    case other =>
      val catalogString = if (other != null) {
        other.catalogString
      } else {
        null
      }
      throw new IllegalArgumentException("Unrecognized type name: " + catalogString)
  }

  def toDataType(entry: TTypeEntry): DataType =
    entry.getPrimitiveEntry.`type` match {
      case TTypeId.NULL_TYPE => NullType
      case TTypeId.BOOLEAN_TYPE => BooleanType
      case TTypeId.TINYINT_TYPE => ByteType
      case TTypeId.SMALLINT_TYPE => ShortType
      case TTypeId.INT_TYPE => IntegerType
      case TTypeId.BIGINT_TYPE => LongType
      case TTypeId.FLOAT_TYPE => FloatType
      case TTypeId.DOUBLE_TYPE => DoubleType
      case TTypeId.STRING_TYPE => StringType
      case TTypeId.DECIMAL_TYPE =>
        val tQualifiers = entry.getPrimitiveEntry
          .getTypeQualifiers.qualifiers
        DecimalType(tQualifiers.get(TCLIServiceConstants.PRECISION).getI32Value,
          tQualifiers.get(TCLIServiceConstants.SCALE).getI32Value)
      case TTypeId.DATE_TYPE => DateType
      case TTypeId.TIMESTAMP_TYPE => TimestampType
      case TTypeId.BINARY_TYPE => BinaryType
      case TTypeId.ARRAY_TYPE =>
        ArrayType(StringType, true)
      case TTypeId.MAP_TYPE =>
        MapType(StringType, StringType, true)
      case TTypeId.STRUCT_TYPE =>
        StringType
      case TTypeId.USER_DEFINED_TYPE => StringType
      case _ => StringType
    }
}
