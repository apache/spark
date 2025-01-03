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

import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.schema.{Type => ParquetType, Types => ParquetTypes}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.variant._
import org.apache.spark.sql.catalyst.expressions.variant.VariantPathParser.PathSegment
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.RowToColumnConverter
import org.apache.spark.sql.execution.datasources.VariantMetadata
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.types.variant._
import org.apache.spark.types.variant.VariantUtil.Type
import org.apache.spark.unsafe.types._

case class SparkShreddedRow(row: SpecializedGetters) extends ShreddingUtils.ShreddedRow {
  override def isNullAt(ordinal: Int): Boolean = row.isNullAt(ordinal)
  override def getBoolean(ordinal: Int): Boolean = row.getBoolean(ordinal)
  override def getByte(ordinal: Int): Byte = row.getByte(ordinal)
  override def getShort(ordinal: Int): Short = row.getShort(ordinal)
  override def getInt(ordinal: Int): Int = row.getInt(ordinal)
  override def getLong(ordinal: Int): Long = row.getLong(ordinal)
  override def getFloat(ordinal: Int): Float = row.getFloat(ordinal)
  override def getDouble(ordinal: Int): Double = row.getDouble(ordinal)
  override def getDecimal(ordinal: Int, precision: Int, scale: Int): java.math.BigDecimal =
    row.getDecimal(ordinal, precision, scale).toJavaBigDecimal
  override def getString(ordinal: Int): String = row.getUTF8String(ordinal).toString
  override def getBinary(ordinal: Int): Array[Byte] = row.getBinary(ordinal)
  override def getStruct(ordinal: Int, numFields: Int): SparkShreddedRow =
    SparkShreddedRow(row.getStruct(ordinal, numFields))
  override def getArray(ordinal: Int): SparkShreddedRow =
    SparkShreddedRow(row.getArray(ordinal))
  override def numElements(): Int = row.asInstanceOf[ArrayData].numElements()
}

// The search result of a `PathSegment` in a `VariantSchema`.
case class SchemaPathSegment(
    rawPath: PathSegment,
    // Whether this path segment is an object or array extraction.
    isObject: Boolean,
    // `schema.typedIdx`, if the path exists in the schema (for object extraction, the schema
    // should contain an object `typed_value` containing the requested field; similar for array
    // extraction). Negative otherwise.
    typedIdx: Int,
    // For object extraction, it is the index of the desired field in `schema.objectSchema`. If the
    // requested field doesn't exist, both `extractionIdx/typedIdx` are set to negative.
    // For array extraction, it is the array index. The information is already stored in `rawPath`,
    // but accessing a raw int should be more efficient than `rawPath`, which is an `Either`.
    extractionIdx: Int)

// Represent a single field in a variant struct (see `VariantMetadata` for definition), that is, a
// single requested field that the scan should produce by extracting from the variant column.
case class FieldToExtract(path: Array[SchemaPathSegment], reader: ParquetVariantReader)

// A helper class to cast from scalar `typed_value` into a scalar `dataType`. Need a custom
// expression because it has different error reporting code than `Cast`.
case class ScalarCastHelper(
    child: Expression,
    dataType: DataType,
    castArgs: VariantCastArgs) extends UnaryExpression {
  // The expression is only for the internal use of `ScalarReader`, which can guarantee the child
  // is not nullable.
  assert(!child.nullable)

  // If `cast` is null, it means the cast always fails because the type combination is not allowed.
  private val cast = if (Cast.canAnsiCast(child.dataType, dataType)) {
    Cast(child, dataType, castArgs.zoneStr, EvalMode.TRY)
  } else {
    null
  }
  // Cast the input to string. Only used for reporting an invalid cast.
  private val castToString = Cast(child, StringType, castArgs.zoneStr, EvalMode.ANSI)

  override def nullable: Boolean = !castArgs.failOnError
  override def withNewChildInternal(newChild: Expression): UnaryExpression = copy(child = newChild)

  // No need to define the interpreted version of `eval`: the codegen must succeed.
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Throw an error or do nothing, depending on `castArgs.failOnError`.
    val invalidCastCode = if (castArgs.failOnError) {
      val castToStringCode = castToString.genCode(ctx)
      val typeObj = ctx.addReferenceObj("dataType", dataType)
      val cls = classOf[ScalarCastHelper].getName
      s"""
        ${castToStringCode.code}
        $cls.throwInvalidVariantCast(${castToStringCode.value}, $typeObj);
      """
    } else {
      ""
    }
    val customCast = (child.dataType, dataType) match {
      case (_: LongType, _: TimestampType) => "castLongToTimestamp"
      case (_: DecimalType, _: TimestampType) => "castDecimalToTimestamp"
      case (_: DecimalType, _: StringType) => "castDecimalToString"
      case _ => null
    }
    if (customCast != null) {
      val childCode = child.genCode(ctx)
      // We can avoid the try-catch block for decimal -> string, but the performance benefit is
      // little. We can also be more specific in the exception type, like catching
      // `ArithmeticException` instead of `Exception`, but it is unnecessary. The `try_cast` codegen
      // also catches `Exception` instead of specific exceptions.
      val code = code"""
        ${childCode.code}
        boolean ${ev.isNull} = false;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        try {
          ${ev.value} = ${classOf[VariantGet].getName}.$customCast(${childCode.value});
        } catch (Exception e) {
          ${ev.isNull} = true;
          $invalidCastCode
        }
      """
      ev.copy(code = code)
    } else if (cast != null) {
      val castCode = cast.genCode(ctx)
      val code = code"""
        ${castCode.code}
        boolean ${ev.isNull} = ${castCode.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${castCode.value};
        if (${ev.isNull}) { $invalidCastCode }
      """
      ev.copy(code = code)
    } else {
      val code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if (${ev.isNull}) { $invalidCastCode }
      """
      ev.copy(code = code)
    }
  }
}

object ScalarCastHelper {
  // A helper function for codegen. The java compiler doesn't allow throwing a `Throwable` in a
  // method without `throws` annotation.
  def throwInvalidVariantCast(value: UTF8String, dataType: DataType): Any =
    throw QueryExecutionErrors.invalidVariantCast(value.toString, dataType)
}

// The base class to read Parquet variant values into a Spark type.
// For convenience, we also allow creating an instance of the base class itself. None of its
// functions can be used, but it can serve as a container of `targetType` and `castArgs`.
class ParquetVariantReader(
    val schema: VariantSchema, val targetType: DataType, val castArgs: VariantCastArgs) {
  // Read from a row containing a Parquet variant value (shredded or unshredded) and return a value
  // of `targetType`. The row schema is described by `schema`.
  // This function throws MALFORMED_VARIANT if the variant is missing. If the variant can be
  // legally missing (the only possible situation is struct fields in object `typed_value`), the
  // caller should check for it and avoid calling this function if the variant is missing.
  def read(row: InternalRow, topLevelMetadata: Array[Byte]): Any = {
    if (schema.typedIdx < 0 || row.isNullAt(schema.typedIdx)) {
      if (schema.variantIdx < 0 || row.isNullAt(schema.variantIdx)) {
        // Both `typed_value` and `value` are null, meaning the variant is missing.
        throw QueryExecutionErrors.malformedVariant()
      }
      val v = new Variant(row.getBinary(schema.variantIdx), topLevelMetadata)
      VariantGet.cast(v, targetType, castArgs)
    } else {
      readFromTyped(row, topLevelMetadata)
    }
  }

  // Subclasses should override it to produce the read result when `typed_value` is not null.
  protected def readFromTyped(row: InternalRow, topLevelMetadata: Array[Byte]): Any =
    throw QueryExecutionErrors.unreachableError()

  // A util function to rebuild the variant in binary format from a Parquet variant value.
  protected final def rebuildVariant(row: InternalRow, topLevelMetadata: Array[Byte]): Variant = {
    val builder = new VariantBuilder(false)
    ShreddingUtils.rebuild(SparkShreddedRow(row), topLevelMetadata, schema, builder)
    builder.result()
  }

  // A util function to throw error or return null when an invalid cast happens.
  protected final def invalidCast(row: InternalRow, topLevelMetadata: Array[Byte]): Any = {
    if (castArgs.failOnError) {
      throw QueryExecutionErrors.invalidVariantCast(
        rebuildVariant(row, topLevelMetadata).toJson(castArgs.zoneId), targetType)
    } else {
      null
    }
  }
}

object ParquetVariantReader {
  // Create a reader for `targetType`. If `schema` is null, meaning that the extraction path doesn't
  // exist in `typed_value`, it returns an instance of `ParquetVariantReader`. As described in the
  // class comment, the reader is only a container of `targetType` and `castArgs` in this case.
  def apply(schema: VariantSchema, targetType: DataType, castArgs: VariantCastArgs,
            isTopLevelUnshredded: Boolean = false): ParquetVariantReader = targetType match {
    case _ if schema == null => new ParquetVariantReader(schema, targetType, castArgs)
    case s: StructType => new StructReader(schema, s, castArgs)
    case a: ArrayType => new ArrayReader(schema, a, castArgs)
    case m@MapType(_: StringType, _, _) => new MapReader(schema, m, castArgs)
    case v: VariantType => new VariantReader(schema, v, castArgs, isTopLevelUnshredded)
    case s: AtomicType => new ScalarReader(schema, s, castArgs)
    case _ =>
      // Type check should have rejected map with non-string type.
      throw QueryExecutionErrors.unreachableError(s"Invalid target type: `${targetType.sql}`")
  }
}

// Read Parquet variant values into a Spark struct type. It reads unshredded fields (fields that are
// not in the typed object) from the `value`, and reads the shredded fields from the object
// `typed_value`.
// `value` must not contain any shredded field according to the shredding spec, but this requirement
// is not enforced. If `value` does contain a shredded field, no error will occur, and the field in
// object `typed_value` will be the final result.
private[this] final class StructReader(
  schema: VariantSchema, targetType: StructType, castArgs: VariantCastArgs)
  extends ParquetVariantReader(schema, targetType, castArgs) {
  // For each field in `targetType`, store the index of the field with the same name in object
  // `typed_value`, or -1 if it doesn't exist in object `typed_value`.
  private[this] val fieldInputIndices: Array[Int] = targetType.fields.map { f =>
    val inputIdx = if (schema.objectSchemaMap != null) schema.objectSchemaMap.get(f.name) else null
    if (inputIdx != null) inputIdx.intValue() else -1
  }
  // For each field in `targetType`, store the reader from the corresponding field in object
  // `typed_value`, or null if it doesn't exist in object `typed_value`.
  private[this] val fieldReaders: Array[ParquetVariantReader] =
    targetType.fields.zip(fieldInputIndices).map { case (f, inputIdx) =>
      if (inputIdx >= 0) {
        val fieldSchema = schema.objectSchema(inputIdx).schema
        ParquetVariantReader(fieldSchema, f.dataType, castArgs)
      } else {
        null
      }
    }
  // If all fields in `targetType` can be found in object `typed_value`, then the reader doesn't
  // need to read from `value`.
  private[this] val needUnshreddedObject: Boolean = fieldInputIndices.exists(_ < 0)

  override def readFromTyped(row: InternalRow, topLevelMetadata: Array[Byte]): Any = {
    if (schema.objectSchema == null) return invalidCast(row, topLevelMetadata)
    val obj = row.getStruct(schema.typedIdx, schema.objectSchema.length)
    val result = new GenericInternalRow(fieldInputIndices.length)
    var unshreddedObject: Variant = null
    if (needUnshreddedObject && schema.variantIdx >= 0 && !row.isNullAt(schema.variantIdx)) {
      unshreddedObject = new Variant(row.getBinary(schema.variantIdx), topLevelMetadata)
      if (unshreddedObject.getType != Type.OBJECT) throw QueryExecutionErrors.malformedVariant()
    }
    val numFields = fieldInputIndices.length
    var i = 0
    while (i < numFields) {
      val inputIdx = fieldInputIndices(i)
      if (inputIdx >= 0) {
        // Shredded field must not be null.
        if (obj.isNullAt(inputIdx)) throw QueryExecutionErrors.malformedVariant()
        val fieldSchema = schema.objectSchema(inputIdx).schema
        val fieldInput = obj.getStruct(inputIdx, fieldSchema.numFields)
        // Only read from the shredded field if it is not missing.
        if ((fieldSchema.typedIdx >= 0 && !fieldInput.isNullAt(fieldSchema.typedIdx)) ||
          (fieldSchema.variantIdx >= 0 && !fieldInput.isNullAt(fieldSchema.variantIdx))) {
          result.update(i, fieldReaders(i).read(fieldInput, topLevelMetadata))
        }
      } else if (unshreddedObject != null) {
        val fieldName = targetType.fields(i).name
        val fieldType = targetType.fields(i).dataType
        val unshreddedField = unshreddedObject.getFieldByKey(fieldName)
        if (unshreddedField != null) {
          result.update(i, VariantGet.cast(unshreddedField, fieldType, castArgs))
        }
      }
      i += 1
    }
    result
  }
}

// Read Parquet variant values into a Spark array type.
private[this] final class ArrayReader(
    schema: VariantSchema, targetType: ArrayType, castArgs: VariantCastArgs)
  extends ParquetVariantReader(schema, targetType, castArgs) {
  private[this] val elementReader = if (schema.arraySchema != null) {
    ParquetVariantReader(schema.arraySchema, targetType.elementType, castArgs)
  } else {
    null
  }

  override def readFromTyped(row: InternalRow, topLevelMetadata: Array[Byte]): Any = {
    if (schema.arraySchema == null) return invalidCast(row, topLevelMetadata)
    val elementNumFields = schema.arraySchema.numFields
    val arr = row.getArray(schema.typedIdx)
    val size = arr.numElements()
    val result = new Array[Any](size)
    var i = 0
    while (i < size) {
      // Shredded array element must not be null.
      if (arr.isNullAt(i)) throw QueryExecutionErrors.malformedVariant()
      result(i) = elementReader.read(arr.getStruct(i, elementNumFields), topLevelMetadata)
      i += 1
    }
    new GenericArrayData(result)
  }
}

// Read Parquet variant values into a Spark map type with string key type. The input must be object
// for a valid cast. The resulting map contains shredded fields from object `typed_value` and
// unshredded fields from object `value`.
// `value` must not contain any shredded field according to the shredding spec. Unlike
// `StructReader`, this requirement is enforced in `MapReader`. If `value` does contain a shredded
// field, throw a MALFORMED_VARIANT error. The purpose is to avoid duplicate map keys.
private[this] final class MapReader(
    schema: VariantSchema, targetType: MapType, castArgs: VariantCastArgs)
  extends ParquetVariantReader(schema, targetType, castArgs) {
  // Readers that convert each shredded field into the map value type.
  private[this] val valueReaders = if (schema.objectSchema != null) {
    schema.objectSchema.map { f =>
      ParquetVariantReader(f.schema, targetType.valueType, castArgs)
    }
  } else {
    null
  }
  // `UTF8String` representation of shredded field names. Do the `String -> UTF8String` once, so
  // that `readFromTyped` doesn't need to do it repeatedly.
  private[this] val shreddedFieldNames = if (schema.objectSchema != null) {
    schema.objectSchema.map { f => UTF8String.fromString(f.fieldName) }
  } else {
    null
  }

  override def readFromTyped(row: InternalRow, topLevelMetadata: Array[Byte]): Any = {
    if (schema.objectSchema == null) return invalidCast(row, topLevelMetadata)
    val obj = row.getStruct(schema.typedIdx, schema.objectSchema.length)
    val numShreddedFields = valueReaders.length
    var unshreddedObject: Variant = null
    if (schema.variantIdx >= 0 && !row.isNullAt(schema.variantIdx)) {
      unshreddedObject = new Variant(row.getBinary(schema.variantIdx), topLevelMetadata)
      if (unshreddedObject.getType != Type.OBJECT) throw QueryExecutionErrors.malformedVariant()
    }
    val numUnshreddedFields = if (unshreddedObject != null) unshreddedObject.objectSize() else 0
    var keyArray = new Array[UTF8String](numShreddedFields + numUnshreddedFields)
    var valueArray = new Array[Any](numShreddedFields + numUnshreddedFields)
    var mapLength = 0
    var i = 0
    while (i < numShreddedFields) {
      // Shredded field must not be null.
      if (obj.isNullAt(i)) throw QueryExecutionErrors.malformedVariant()
      val fieldSchema = schema.objectSchema(i).schema
      val fieldInput = obj.getStruct(i, fieldSchema.numFields)
      // Only add the shredded field to map if it is not missing.
      if ((fieldSchema.typedIdx >= 0 && !fieldInput.isNullAt(fieldSchema.typedIdx)) ||
        (fieldSchema.variantIdx >= 0 && !fieldInput.isNullAt(fieldSchema.variantIdx))) {
        keyArray(mapLength) = shreddedFieldNames(i)
        valueArray(mapLength) = valueReaders(i).read(fieldInput, topLevelMetadata)
        mapLength += 1
      }
      i += 1
    }
    i = 0
    while (i < numUnshreddedFields) {
      val field = unshreddedObject.getFieldAtIndex(i)
      if (schema.objectSchemaMap.containsKey(field.key)) {
        throw QueryExecutionErrors.malformedVariant()
      }
      keyArray(mapLength) = UTF8String.fromString(field.key)
      valueArray(mapLength) = VariantGet.cast(field.value, targetType.valueType, castArgs)
      mapLength += 1
      i += 1
    }
    // Need to shrink the arrays if there are missing shredded fields.
    if (mapLength < keyArray.length) {
      keyArray = keyArray.slice(0, mapLength)
      valueArray = valueArray.slice(0, mapLength)
    }
    ArrayBasedMapData(keyArray, valueArray)
  }
}

// Read Parquet variant values into a Spark variant type (the binary format).
private[this] final class VariantReader(
    schema: VariantSchema, targetType: DataType, castArgs: VariantCastArgs,
    // An optional optimization: the user can set it to true if the Parquet variant column is
    // unshredded and the extraction path is empty. We are not required to do anything special, bu
    // we can avoid rebuilding variant for optimization purpose.
    private[this] val isTopLevelUnshredded: Boolean)
  extends ParquetVariantReader(schema, targetType, castArgs) {
  override def read(row: InternalRow, topLevelMetadata: Array[Byte]): Any = {
    if (isTopLevelUnshredded) {
      if (row.isNullAt(schema.variantIdx)) throw QueryExecutionErrors.malformedVariant()
      return new VariantVal(row.getBinary(schema.variantIdx), topLevelMetadata)
    }
    val v = rebuildVariant(row, topLevelMetadata)
    new VariantVal(v.getValue, v.getMetadata)
  }
}

// Read Parquet variant values into a Spark scalar type. When `typed_value` is not null but not a
// scalar, all other target types should return an invalid cast, but only the string target type can
// still build a string from array/object `typed_value`. For scalar `typed_value`, it depends on
// `ScalarCastHelper` to perform the cast.
// According to the shredding spec, scalar `typed_value` and `value` must not be non-null at the
// same time. The requirement is not enforced in this reader. If they are both non-null, no error
// will occur, and the reader will read from `typed_value`.
private[this] final class ScalarReader(
    schema: VariantSchema, targetType: DataType, castArgs: VariantCastArgs)
  extends ParquetVariantReader(schema, targetType, castArgs) {
  private[this] val castProject = if (schema.scalarSchema != null) {
    val scalarType = SparkShreddingUtils.scalarSchemaToSparkType(schema.scalarSchema)
    // Read the cast input from ordinal `schema.typedIdx` in the input row. The cast input is never
    // null, because `readFromTyped` is only called when `typed_value` is not null.
    val input = BoundReference(schema.typedIdx, scalarType, nullable = false)
    MutableProjection.create(Seq(ScalarCastHelper(input, targetType, castArgs)))
  } else {
    null
  }

  override def readFromTyped(row: InternalRow, topLevelMetadata: Array[Byte]): Any = {
    if (castProject == null) {
      return if (targetType.isInstanceOf[StringType]) {
        UTF8String.fromString(rebuildVariant(row, topLevelMetadata).toJson(castArgs.zoneId))
      } else {
        invalidCast(row, topLevelMetadata)
      }
    }
    val result = castProject(row)
    if (result.isNullAt(0)) null else result.get(0, targetType)
  }
}

case object SparkShreddingUtils {
  val VariantValueFieldName = "value";
  val TypedValueFieldName = "typed_value";
  val MetadataFieldName = "metadata";

  val VARIANT_WRITE_SHREDDING_KEY: String = "__VARIANT_WRITE_SHREDDING_KEY"

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
      case ArrayType(elementType, _) =>
        // Always set containsNull to false. One of value or typed_value must always be set for
        // array elements.
        val arrayShreddingSchema =
          ArrayType(variantShreddingSchema(elementType, false), containsNull = false)
        Seq(
          StructField(VariantValueFieldName, BinaryType, nullable = true),
          StructField(TypedValueFieldName, arrayShreddingSchema, nullable = true)
        )
      case StructType(fields) =>
        // The field name level is always non-nullable: Variant null values are represented in the
        // "value" columna as "00", and missing values are represented by setting both "value" and
        // "typed_value" to null.
        val objectShreddingSchema = StructType(fields.map(f =>
            f.copy(dataType = variantShreddingSchema(f.dataType, false), nullable = false)))
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

  /**
   * Given a schema that represents a valid shredding schema (e.g. constructed by
   * SparkShreddingUtils.variantShreddingSchema), add metadata to the top-level fields to mark it
   * as a shredding schema for writers.
   */
  def addWriteShreddingMetadata(schema: StructType): StructType = {
    val newFields = schema.fields.map { f =>
      f.copy(metadata = new
          MetadataBuilder()
            .withMetadata(f.metadata)
            .putNull(VARIANT_WRITE_SHREDDING_KEY).build())
    }
    StructType(newFields)
  }

  // Check if the struct is marked with metadata set by addWriteShreddingMetadata - i.e. it
  // represents a Variant converted to a shredding schema for writing.
  def isVariantShreddingStruct(s: StructType): Boolean = {
    s.fields.length > 0 && s.fields.forall(_.metadata.contains(VARIANT_WRITE_SHREDDING_KEY))
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

    // The struct must not be empty or contain duplicate field names. The latter is enforced in the
    // loop below (`if (typedIdx != -1)` and other similar checks).
    if (schema.fields.isEmpty) {
      throw QueryCompilationErrors.invalidVariantShreddingSchema(schema)
    }
    schema.fields.zipWithIndex.foreach { case (f, i) =>
      f.name match {
        case TypedValueFieldName =>
          if (typedIdx != -1) {
            throw QueryCompilationErrors.invalidVariantShreddingSchema(schema)
          }
          typedIdx = i
          f.dataType match {
            case StructType(fields) =>
              // The struct must not be empty or contain duplicate field names.
              if (fields.isEmpty || fields.map(_.name).distinct.length != fields.length) {
                throw QueryCompilationErrors.invalidVariantShreddingSchema(schema)
              }
              objectSchema = new Array[VariantSchema.ObjectField](fields.length)
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

  // Convert a scalar variant schema into a Spark scalar type.
  def scalarSchemaToSparkType(scalar: VariantSchema.ScalarType): DataType = scalar match {
    case _: VariantSchema.StringType => StringType
    case it: VariantSchema.IntegralType => it.size match {
      case VariantSchema.IntegralSize.BYTE => ByteType
      case VariantSchema.IntegralSize.SHORT => ShortType
      case VariantSchema.IntegralSize.INT => IntegerType
      case VariantSchema.IntegralSize.LONG => LongType
    }
    case _: VariantSchema.FloatType => FloatType
    case _: VariantSchema.DoubleType => DoubleType
    case _: VariantSchema.BooleanType => BooleanType
    case _: VariantSchema.BinaryType => BinaryType
    case dt: VariantSchema.DecimalType => DecimalType(dt.precision, dt.scale)
    case _: VariantSchema.DateType => DateType
    case _: VariantSchema.TimestampType => TimestampType
    case _: VariantSchema.TimestampNTZType => TimestampNTZType
  }

  // Convert a Parquet type into a Spark data type.
  def parquetTypeToSparkType(parquetType: ParquetType): DataType = {
    val messageType = ParquetTypes.buildMessage().addField(parquetType).named("foo")
    val column = new ColumnIOFactory().getColumnIO(messageType)
    new ParquetToSparkSchemaConverter().convertField(column.getChild(0)).sparkType
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

  // Return a list of fields to extract. `targetType` must be either variant or variant struct.
  // If it is variant, return null because the target is the full variant and there is no field to
  // extract. If it is variant struct, return a list of fields matching the variant struct fields.
  def getFieldsToExtract(targetType: DataType, inputSchema: VariantSchema): Array[FieldToExtract] =
    targetType match {
      case _: VariantType => null
      case s: StructType if VariantMetadata.isVariantStruct(s) =>
        s.fields.map { f =>
          val metadata = VariantMetadata.fromMetadata(f.metadata)
          val rawPath = metadata.parsedPath()
          val schemaPath = new Array[SchemaPathSegment](rawPath.length)
          var schema = inputSchema
          // Search `rawPath` in `schema` to produce `schemaPath`. If a raw path segment cannot be
          // found at a certain level of the file type, then `typedIdx` will be -1 starting from
          // this position, and the final `schema` will be null.
          for (i <- rawPath.indices) {
            val isObject = rawPath(i).isLeft
            var typedIdx = -1
            var extractionIdx = -1
            rawPath(i) match {
              case scala.util.Left(key) if schema != null && schema.objectSchema != null =>
                val fieldIdx = schema.objectSchemaMap.get(key)
                if (fieldIdx != null) {
                  typedIdx = schema.typedIdx
                  extractionIdx = fieldIdx
                  schema = schema.objectSchema(fieldIdx).schema
                } else {
                  schema = null
                }
              case scala.util.Right(index) if schema != null && schema.arraySchema != null =>
                typedIdx = schema.typedIdx
                extractionIdx = index
                schema = schema.arraySchema
              case _ =>
                schema = null
            }
            schemaPath(i) = SchemaPathSegment(rawPath(i), isObject, typedIdx, extractionIdx)
          }
          val reader = ParquetVariantReader(schema, f.dataType, VariantCastArgs(
            metadata.failOnError,
            Some(metadata.timeZoneId),
            DateTimeUtils.getZoneId(metadata.timeZoneId)),
            isTopLevelUnshredded = schemaPath.isEmpty && inputSchema.isUnshredded)
          FieldToExtract(schemaPath, reader)
        }
      case _ =>
        throw QueryExecutionErrors.unreachableError(s"Invalid target type: `${targetType.sql}`")
    }

  // Extract a single variant struct field from a Parquet variant value. It steps into `inputRow`
  // according to the variant extraction path, and read the extracted value as the target type.
  private def extractField(
      inputRow: InternalRow,
      topLevelMetadata: Array[Byte],
      inputSchema: VariantSchema,
      pathList: Array[SchemaPathSegment],
      reader: ParquetVariantReader): Any = {
    var pathIdx = 0
    val pathLen = pathList.length
    var row = inputRow
    var schema = inputSchema
    while (pathIdx < pathLen) {
      val path = pathList(pathIdx)

      if (path.typedIdx < 0) {
        // The extraction doesn't exist in `typed_value`. Try to extract the remaining part of the
        // path in `value`.
        val variantIdx = schema.variantIdx
        if (variantIdx < 0 || row.isNullAt(variantIdx)) return null
        var v = new Variant(row.getBinary(variantIdx), topLevelMetadata)
        while (pathIdx < pathLen) {
          v = pathList(pathIdx).rawPath match {
            case scala.util.Left(key) if v.getType == Type.OBJECT => v.getFieldByKey(key)
            case scala.util.Right(index) if v.getType == Type.ARRAY => v.getElementAtIndex(index)
            case _ => null
          }
          if (v == null) return null
          pathIdx += 1
        }
        return VariantGet.cast(v, reader.targetType, reader.castArgs)
      }

      if (row.isNullAt(path.typedIdx)) return null
      if (path.isObject) {
        val obj = row.getStruct(path.typedIdx, schema.objectSchema.length)
        // Object field must not be null.
        if (obj.isNullAt(path.extractionIdx)) throw QueryExecutionErrors.malformedVariant()
        schema = schema.objectSchema(path.extractionIdx).schema
        row = obj.getStruct(path.extractionIdx, schema.numFields)
        // Return null if the field is missing.
        if ((schema.typedIdx < 0 || row.isNullAt(schema.typedIdx)) &&
          (schema.variantIdx < 0 || row.isNullAt(schema.variantIdx))) {
          return null
        }
      } else {
        val arr = row.getArray(path.typedIdx)
        // Return null if the extraction index is out of bound.
        if (path.extractionIdx >= arr.numElements()) return null
        // Array element must not be null.
        if (arr.isNullAt(path.extractionIdx)) throw QueryExecutionErrors.malformedVariant()
        schema = schema.arraySchema
        row = arr.getStruct(path.extractionIdx, schema.numFields)
      }
      pathIdx += 1
    }
    reader.read(row, topLevelMetadata)
  }

  // Assemble a variant (binary format) from a Parquet variant value.
  def assembleVariant(row: InternalRow, schema: VariantSchema): VariantVal = {
    val v = ShreddingUtils.rebuild(SparkShreddedRow(row), schema)
    new VariantVal(v.getValue, v.getMetadata)
  }

  // Assemble a variant struct, in which each field is extracted from the Parquet variant value.
  def assembleVariantStruct(
      inputRow: InternalRow,
      schema: VariantSchema,
      fields: Array[FieldToExtract]): InternalRow = {
    if (inputRow.isNullAt(schema.topLevelMetadataIdx)) {
      throw QueryExecutionErrors.malformedVariant()
    }
    val topLevelMetadata = inputRow.getBinary(schema.topLevelMetadataIdx)
    val numFields = fields.length
    val resultRow = new GenericInternalRow(numFields)
    var fieldIdx = 0
    while (fieldIdx < numFields) {
      resultRow.update(fieldIdx, extractField(inputRow, topLevelMetadata, schema,
        fields(fieldIdx).path, fields(fieldIdx).reader))
      fieldIdx += 1
    }
    resultRow
  }

  // Assemble a batch of variant (binary format) from a batch of Parquet variant values.
  def assembleVariantBatch(
      input: WritableColumnVector,
      output: WritableColumnVector,
      schema: VariantSchema): Unit = {
    val numRows = input.getElementsAppended
    output.reset()
    output.reserve(numRows)
    val valueChild = output.getChild(0)
    val metadataChild = output.getChild(1)
    var i = 0
    while (i < numRows) {
      if (input.isNullAt(i)) {
        output.appendStruct(true)
      } else {
        output.appendStruct(false)
        val v = SparkShreddingUtils.assembleVariant(input.getStruct(i), schema)
        valueChild.appendByteArray(v.getValue, 0, v.getValue.length)
        metadataChild.appendByteArray(v.getMetadata, 0, v.getMetadata.length)
      }
      i += 1
    }
  }

  // Assemble a batch of variant struct from a batch of Parquet variant values.
  def assembleVariantStructBatch(
      input: WritableColumnVector,
      output: WritableColumnVector,
      schema: VariantSchema,
      fields: Array[FieldToExtract]): Unit = {
    val numRows = input.getElementsAppended
    output.reset()
    output.reserve(numRows)
    val converter = new RowToColumnConverter(StructType(Array(StructField("", output.dataType()))))
    val converterVectors = Array(output)
    val converterRow = new GenericInternalRow(1)
    output.reset()
    output.reserve(input.getElementsAppended)
    var i = 0
    while (i < numRows) {
      if (input.isNullAt(i)) {
        converterRow.update(0, null)
      } else {
        converterRow.update(0, assembleVariantStruct(input.getStruct(i), schema, fields))
      }
      converter.convert(converterRow, converterVectors)
      i += 1
    }
  }
}
