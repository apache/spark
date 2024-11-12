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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._
import org.apache.spark.types.variant._
import org.apache.spark.unsafe.types._

case object SparkShreddingUtils {
  val VariantValueFieldName = "value";
  val TypedValueFieldName = "typed_value";
  val MetadataFieldName = "metadata";

  def buildVariantSchema(schema: DataType): VariantSchema = {
    schema match {
      case s: StructType => buildVariantSchema(s, topLevel = true)
      case _ => throw QueryCompilationErrors.invalidVariantShreddingSchema(schema)
    }
  }

  /**
   * Given an expected schema of a Variant value, returns a suitable schema for shredding, by
   * inserting appropriate intermediate value/typed_value fields at each level.
   * For example, to represent the JSON {"a": 1, "b": "hello"},
   * the schema struct<a: int, b: string> could be passed into this function, and it would return
   * the shredding schema:
   * struct<
   *  metadata: binary,
   *  value: binary,
   *  typed_value: struct<
   *   a: struct<typed_value: int, value: binary>,
   *   b: struct<typed_value: string, value: binary>>>
   *
   */
  def variantShreddingSchema(dataType: DataType, isTopLevel: Boolean = true): StructType = {
    val fields = dataType match {
      case ArrayType(elementType, containsNull) =>
        val arrayShreddingSchema =
          ArrayType(variantShreddingSchema(elementType, false), containsNull)
        Seq(
          StructField(VariantValueFieldName, BinaryType, nullable = true),
          StructField(TypedValueFieldName, arrayShreddingSchema, nullable = true)
        )
      case StructType(fields) =>
        val objectShreddingSchema = StructType(fields.map(f =>
            f.copy(dataType = variantShreddingSchema(f.dataType, false))))
        Seq(
          StructField(VariantValueFieldName, BinaryType, nullable = true),
          StructField(TypedValueFieldName, objectShreddingSchema, nullable = true)
        )
      case VariantType =>
        // For Variant, we don't need a typed column
        Seq(
          StructField(VariantValueFieldName, BinaryType, nullable = true)
        )
      case _: NumericType | BooleanType | _: StringType | BinaryType | _: DatetimeType =>
        Seq(
          StructField(VariantValueFieldName, BinaryType, nullable = true),
          StructField(TypedValueFieldName, dataType, nullable = true)
        )
      case _ =>
        // No other types have a corresponding shreddings schema.
        throw QueryCompilationErrors.invalidVariantShreddingSchema(dataType)
    }

    if (isTopLevel) {
      StructType(StructField(MetadataFieldName, BinaryType, nullable = false) +: fields)
    } else {
      StructType(fields)
    }
  }

  /*
   * Given a Spark schema that represents a valid shredding schema (e.g. constructed by
   * SparkShreddingUtils.variantShreddingSchema), return the corresponding VariantSchema.
   */
  private def buildVariantSchema(schema: StructType, topLevel: Boolean): VariantSchema = {
    var typedIdx = -1
    var variantIdx = -1
    var topLevelMetadataIdx = -1
    var scalarSchema: VariantSchema.ScalarType = null
    var objectSchema: Array[VariantSchema.ObjectField] = null
    var arraySchema: VariantSchema = null

    schema.fields.zipWithIndex.foreach { case (f, i) =>
      f.name match {
        case TypedValueFieldName =>
          if (typedIdx != -1) {
            throw QueryCompilationErrors.invalidVariantShreddingSchema(schema)
          }
          typedIdx = i
          f.dataType match {
            case StructType(fields) =>
              objectSchema =
                  new Array[VariantSchema.ObjectField](fields.length)
              fields.zipWithIndex.foreach { case (field, fieldIdx) =>
                field.dataType match {
                  case s: StructType =>
                    val fieldSchema = buildVariantSchema(s, topLevel = false)
                    objectSchema(fieldIdx) = new VariantSchema.ObjectField(field.name, fieldSchema)
                  case _ => throw QueryCompilationErrors.invalidVariantShreddingSchema(schema)
                }
              }
            case ArrayType(elementType, _) =>
              elementType match {
                case s: StructType => arraySchema = buildVariantSchema(s, topLevel = false)
                case _ => throw QueryCompilationErrors.invalidVariantShreddingSchema(schema)
              }
            case t => scalarSchema = (t match {
              case BooleanType => new VariantSchema.BooleanType
              case ByteType => new VariantSchema.IntegralType(VariantSchema.IntegralSize.BYTE)
              case ShortType => new VariantSchema.IntegralType(VariantSchema.IntegralSize.SHORT)
              case IntegerType => new VariantSchema.IntegralType(VariantSchema.IntegralSize.INT)
              case LongType => new VariantSchema.IntegralType(VariantSchema.IntegralSize.LONG)
              case FloatType => new VariantSchema.FloatType
              case DoubleType => new VariantSchema.DoubleType
              case StringType => new VariantSchema.StringType
              case BinaryType => new VariantSchema.BinaryType
              case DateType => new VariantSchema.DateType
              case TimestampType => new VariantSchema.TimestampType
              case TimestampNTZType => new VariantSchema.TimestampNTZType
              case d: DecimalType => new VariantSchema.DecimalType(d.precision, d.scale)
              case _ => throw QueryCompilationErrors.invalidVariantShreddingSchema(schema)
            })
          }
        case VariantValueFieldName =>
          if (variantIdx != -1 || f.dataType != BinaryType) {
            throw QueryCompilationErrors.invalidVariantShreddingSchema(schema)
          }
          variantIdx = i
        case MetadataFieldName =>
          if (topLevelMetadataIdx != -1 || f.dataType != BinaryType) {
            throw QueryCompilationErrors.invalidVariantShreddingSchema(schema)
          }
          topLevelMetadataIdx = i
        case _ => throw QueryCompilationErrors.invalidVariantShreddingSchema(schema)
      }
    }

    if (topLevel != (topLevelMetadataIdx >= 0)) {
      throw QueryCompilationErrors.invalidVariantShreddingSchema(schema)
    }
    new VariantSchema(typedIdx, variantIdx, topLevelMetadataIdx, schema.fields.length,
      scalarSchema, objectSchema, arraySchema)
  }

  class SparkShreddedResult(schema: VariantSchema) extends VariantShreddingWriter.ShreddedResult {
    // Result is stored as an InternalRow.
    val row = new GenericInternalRow(schema.numFields)

    override def addArray(array: Array[VariantShreddingWriter.ShreddedResult]): Unit = {
      val arrayResult = new GenericArrayData(
          array.map(_.asInstanceOf[SparkShreddedResult].row))
      row.update(schema.typedIdx, arrayResult)
    }

    override def addObject(values: Array[VariantShreddingWriter.ShreddedResult]): Unit = {
      val innerRow = new GenericInternalRow(schema.objectSchema.size)
      for (i <- 0 until values.length) {
        innerRow.update(i, values(i).asInstanceOf[SparkShreddedResult].row)
      }
      row.update(schema.typedIdx, innerRow)
    }

    override def addVariantValue(result: Array[Byte]): Unit = {
      row.update(schema.variantIdx, result)
    }

    override def addScalar(result: Any): Unit = {
      // Convert to native spark value, if necessary.
      val sparkValue = schema.scalarSchema match {
        case _: VariantSchema.StringType => UTF8String.fromString(result.asInstanceOf[String])
        case _: VariantSchema.DecimalType => Decimal(result.asInstanceOf[java.math.BigDecimal])
        case _ => result
      }
      row.update(schema.typedIdx, sparkValue)
    }

    override def addMetadata(result: Array[Byte]): Unit = {
      row.update(schema.topLevelMetadataIdx, result)
    }
  }

  class SparkShreddedResultBuilder() extends VariantShreddingWriter.ShreddedResultBuilder {
    override def createEmpty(schema: VariantSchema): VariantShreddingWriter.ShreddedResult = {
      new SparkShreddedResult(schema)
    }

    // Consider allowing this to be set via config?
    override def allowNumericScaleChanges(): Boolean = true
  }

  /**
   * Converts an input variant into shredded components. Returns the shredded result.
   */
  def castShredded(v: Variant, schema: VariantSchema): InternalRow = {
    VariantShreddingWriter.castShredded(v, schema, new SparkShreddedResultBuilder())
        .asInstanceOf[SparkShreddedResult]
        .row
  }
}
