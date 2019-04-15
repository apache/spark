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
 *
 */

package org.apache.spark.cypher.conversions

import org.apache.spark.cypher.conversions.TemporalConversions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue, CypherValueConverter}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.RecordHeader

object TypeConversions {

  val DEFAULT_PRECISION = 20

  implicit class CypherTypeOps(val ct: CypherType) extends AnyVal {

    def toStructField(column: String): StructField = {
      ct.toSparkType match {
        case Some(st) => StructField(column, st, ct.isNullable)
        case None => throw IllegalArgumentException("CypherType supported by CAPS", ct)
      }
    }

    def toSparkType: Option[DataType] = ct match {
      case CTNull => Some(NullType)
      case _ =>
        ct.material match {
          case CTString => Some(StringType)
          case CTInteger => Some(LongType)
          case CTBigDecimal(p, s) => Some(DataTypes.createDecimalType(p, s))
          case CTFloat => Some(DoubleType)
          case CTLocalDateTime => Some(TimestampType)
          case CTDate => Some(DateType)
          case CTDuration => Some(CalendarIntervalType)
          case CTIdentity => Some(BinaryType)
          case b if b.subTypeOf(CTBoolean) => Some(BooleanType)
          case n if n.subTypeOf(CTEntity.nullable) => Some(BinaryType)
          // Spark uses String as the default array inner type
          case CTMap(inner) => Some(StructType(inner.map { case (key, vType) => vType.toStructField(key) }.toSeq))
          case CTEmptyList => Some(ArrayType(StringType, containsNull = false))
          case CTList(CTNull) => Some(ArrayType(StringType, containsNull = true))
          case CTList(inner) if inner.subTypeOf(CTBoolean.nullable) => Some(ArrayType(BooleanType, containsNull = inner.isNullable))
          case CTList(elemType) if elemType.toSparkType.isDefined => elemType.toSparkType.map(ArrayType(_, elemType.isNullable))
          case l if l.subTypeOf(CTList(CTNumber.nullable)) => Some(ArrayType(DoubleType, containsNull = l.isNullable))
          case _ => None
        }
    }

    def getSparkType: DataType = toSparkType match {
      case Some(t) => t
      case None => throw NotImplementedException(s"Mapping of CypherType $ct to Spark type is unsupported")
    }

    def isSparkCompatible: Boolean = toSparkType.isDefined

    def ensureSparkCompatible(): Unit = getSparkType

  }

  implicit class StructTypeOps(val structType: StructType) {
    def toRecordHeader: RecordHeader = {

      val exprToColumn = structType.fields.map { field =>
        val cypherType = field.toCypherType match {
          case Some(ct) => ct
          case None => throw IllegalArgumentException("a supported Spark type", field.dataType)
        }
        Var(field.name)(cypherType) -> field.name
      }

      RecordHeader(exprToColumn.toMap)
    }

    def binaryColumns: Set[String] = structType.fields.filter(_.dataType == BinaryType).map(_.name).toSet

    def convertTypes(from: DataType, to: DataType): StructType = StructType(structType.map {
      case sf: StructField if sf.dataType == from => sf.copy(dataType = to)
      case sf: StructField => sf
    })
  }

  implicit class StructFieldOps(val field: StructField) extends AnyVal {
    def toCypherType: Option[CypherType] = field.dataType.toCypherType(field.nullable)
  }

  implicit class DataTypeOps(val dt: DataType) extends AnyVal {
    def toCypherType(nullable: Boolean = false): Option[CypherType] = {
      val result = dt match {
        case StringType => Some(CTString)
        case IntegerType => Some(CTInteger)
        case LongType => Some(CTInteger)
        case BooleanType => Some(CTBoolean)
        case DoubleType => Some(CTFloat)
        case dt: DecimalType => Some(CTBigDecimal(dt.precision, dt.scale))
        case TimestampType => Some(CTLocalDateTime)
        case DateType => Some(CTDate)
        case CalendarIntervalType => Some(CTDuration)
        case ArrayType(NullType, _) => Some(CTEmptyList)
        case BinaryType => Some(CTIdentity)
        case ArrayType(elemType, containsNull) =>
          elemType.toCypherType(containsNull).map(CTList(_))
        case NullType => Some(CTNull)
        case StructType(fields) =>
          val convertedFields = fields.map { field => field.name -> field.dataType.toCypherType(field.nullable) }.toMap
          val containsNone = convertedFields.exists {
            case (_, None) => true
            case _ => false
          }
          if (containsNone) None else Some(CTMap(convertedFields.mapValues(_.get)))
        case _ => None
      }

      if (nullable) result.map(_.nullable) else result.map(_.material)
    }

    /**
      * Checks if the given data type is supported within the Cypher type system.
      *
      * @return true, iff the data type is supported
      */
    def isCypherCompatible: Boolean = cypherCompatibleDataType.isDefined

    /**
      * Converts the given Spark data type into a Cypher type system compatible Spark data type.
      *
      * @return some Cypher-compatible Spark data type or none if not compatible
      */
    def cypherCompatibleDataType: Option[DataType] = dt match {
      case ByteType | ShortType | IntegerType => Some(LongType)
      case FloatType => Some(DoubleType)
      case compatible if dt.toCypherType().isDefined => Some(compatible)
      case _ => None
    }
  }

  implicit class RecordHeaderOps(header: RecordHeader) extends Serializable {

    def toStructType: StructType = {
      val structFields = header.columns.toSeq.sorted.map { column =>
        val expressions = header.expressionsFor(column)
        val commonType = expressions.map(_.cypherType).reduce(_ join _)
        assert(commonType.isSparkCompatible,
          s"""
             |Expressions $expressions with common super type $commonType mapped to column $column have no compatible data type.
         """.stripMargin)
        commonType.toStructField(column)
      }
      StructType(structFields)
    }

    def rowEncoder: ExpressionEncoder[Row] =
      RowEncoder(header.toStructType)
  }

  implicit class RowOps(row: Row) {

    def allNull: Boolean = allNull(row.size)

    def allNull(rowSize: Int): Boolean = (for (i <- 0 until rowSize) yield row.isNullAt(i)).reduce(_ && _)
  }


  object SparkCypherValueConverter extends CypherValueConverter {
    override def convert(v: Any): Option[CypherValue] = v match {
      case interval: CalendarInterval => Some(interval.toDuration)
      case row: Row =>
        val pairs: Seq[(String, Any)] = row.schema.fieldNames.map { field =>
          val index = row.fieldIndex(field)
          field -> row.get(index)
        }
        Some(CypherMap(pairs: _*))

      case _ => None
    }
  }

  implicit val sparkCypherValueConverter: CypherValueConverter = SparkCypherValueConverter
}

