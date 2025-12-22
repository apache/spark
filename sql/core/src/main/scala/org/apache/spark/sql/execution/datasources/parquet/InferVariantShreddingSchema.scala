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

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.types.variant._
import org.apache.spark.types.variant.VariantUtil.Type
import org.apache.spark.unsafe.types._

/**
 *
 * Infer a schema when there are Variant values in the shredding schema.
 * Only VariantType values at the top level or nested in struct fields are shredded.
 * VariantType nested in arrays or maps are not shredded.
 * @param schema The original schema, with no shredding.
 */
class InferVariantShreddingSchema(val schema: StructType) {

  /**
   * Create a list of paths to Variant values in the schema.
   * Variant fields nested in arrays or maps are not included.
   * For example, if the schema is
   * struct<v: variant, struct<a: int, b: int, c: variant>>
   * the function will return [[0], [1, 2]
   */
  private def getPathsToVariant(schema: StructType): Seq[Seq[Int]] = {
    schema.fields.zipWithIndex
      .map {
        case (field, idx) =>
          field.dataType match {
            case VariantType =>
              Seq(Seq(idx))
            case inner: StructType =>
              // Prepend this index to each downstream path.
              getPathsToVariant(inner).map { path =>
                idx +: path
              }
            case _ => Seq()
          }
      }
      .toSeq
      .flatten
  }

  /**
   * Return the VariantVal at the given path in the schema, or None if the Variant value or any of
   * its containing structs is null.
   */
  @scala.annotation.tailrec
  private def getValueAtPath(schema: StructType, row: InternalRow, path: Seq[Int]):
      Option[VariantVal] = {
    if (row.isNullAt(path.head)) {
      None
    } else if (path.length == 1) {
      // We've reached the Variant value.
      Some(row.getVariant(path.head))
    } else {
      // The field must be a struct.
      val childStruct = schema.fields(path.head).dataType.asInstanceOf[StructType]
      getValueAtPath(
        childStruct,
        row.getStruct(path.head, childStruct.length),
        path.tail
      )
    }
  }

  private val pathsToVariant = getPathsToVariant(schema)

  private val maxShreddedFieldsPerFile =
    SQLConf.get.getConf(SQLConf.VARIANT_SHREDDING_MAX_SCHEMA_WIDTH)

  private val maxShreddingDepth =
    SQLConf.get.getConf(SQLConf.VARIANT_SHREDDING_MAX_SCHEMA_DEPTH)

  private val COUNT_METADATA_KEY = "COUNT"

  /**
   * Return an appropriate schema for shredding a Variant value.
   * It is similar to the SchemaOfVariant expression, but the rules are somewhat different, because
   * we want the types to be consistent with what will be allowed during shredding. E.g.
   * SchemaOfVariant will consider the common type across Integer and Double to be double, but we
   * consider it to be VariantType, since shredding will not allow those types to be written to
   * the same typed_value.
   * We also maintain metadata on struct fields to track how frequently they occur. Rare fields
   * are dropped in the final schema.
   */
  private def schemaOf(v: Variant, maxDepth: Int): DataType = v.getType match {
    case Type.OBJECT =>
      if (maxDepth <= 0) return VariantType
      val size = v.objectSize()
      val fields = new Array[StructField](size)
      for (i <- 0 until size) {
        val field = v.getFieldAtIndex(i)
        fields(i) = StructField(field.key, schemaOf(field.value, maxDepth - 1),
          metadata = new MetadataBuilder().putLong(COUNT_METADATA_KEY, 1).build())
      }
      // According to the variant spec, object fields must be sorted alphabetically. So we don't
      // have to sort, but just need to validate they are sorted.
      for (i <- 1 until size) {
        if (fields(i - 1).name >= fields(i).name) {
          throw new SparkRuntimeException(
            errorClass = "MALFORMED_VARIANT",
            messageParameters = Map.empty
          )
        }
      }
      StructType(fields)
    case Type.ARRAY =>
      if (maxDepth <= 0) return VariantType
      var elementType: DataType = NullType
      for (i <- 0 until v.arraySize()) {
        elementType = mergeSchema(elementType, schemaOf(v.getElementAtIndex(i), maxDepth - 1))
      }
      ArrayType(elementType)
    case Type.NULL => NullType
    case Type.BOOLEAN => BooleanType
    case Type.LONG =>
      // Compute the smallest decimal that can contain this value.
      // This will allow us to merge with decimal later without introducing excessive precision.
      // If we only end up encountering integer values, we'll convert back to LongType when we
      // finalize.
      val d = BigDecimal(v.getLong())
      val precision = d.precision
      if (precision <= Decimal.MAX_LONG_DIGITS) {
        DecimalType(precision, 0)
      } else {
        // Value is too large for Decimal(18, 0), so record its type as long.
        LongType
      }
    case Type.STRING => StringType
    case Type.DOUBLE => DoubleType
    case Type.DECIMAL =>
      // Don't strip trailing zeros to determine scale. Even if we allow scale relaxation during
      // shredding, it's useful to take trailing zeros as a hint that the extra digits may be used
      // in later values, and use the larger scale.
      val d = Decimal(v.getDecimalWithOriginalScale())
      DecimalType(d.precision, d.scale)
    case Type.DATE => DateType
    case Type.TIMESTAMP => TimestampType
    case Type.TIMESTAMP_NTZ => TimestampNTZType
    case Type.FLOAT => FloatType
    case Type.BINARY => BinaryType
    // Spark doesn't support UUID, so shred it as an untyped value.
    case Type.UUID => VariantType
  }

  private def getFieldCount(field: StructField): Long = {
    field.metadata.getLong(COUNT_METADATA_KEY)
  }

  // Merge two decimals with possibly different scales.
  private def mergeDecimal(d1: DecimalType, d2: DecimalType): DataType = {
    val scale = Math.max(d1.scale, d2.scale)
    val range = Math.max(d1.precision - d1.scale, d2.precision - d2.scale)
    if (range + scale > DecimalType.MAX_PRECISION) {
      // DecimalType can't support precision > 38
      VariantType
    } else {
      DecimalType(range + scale, scale)
    }
  }

  private def mergeDecimalWithLong(d: DecimalType): DataType = {
    if (d.scale == 0 && d.precision <= 18) {
      // It's an integer-like Decimal. Rather than widen to a precision of 19, we can
      // use LongType
      LongType
    } else {
      // Long can always fit in a Decimal(19, 0)
      mergeDecimal(d, DecimalType(19, 0))
    }
  }

  private def mergeSchema(dt1: DataType, dt2: DataType): DataType = {
    (dt1, dt2) match {
      // Allow VariantNull to appear in any typed schema
      case (NullType, t) => t
      case (t, NullType) => t
      case (d1: DecimalType, d2: DecimalType) =>
        mergeDecimal(d1, d2)
      case (d: DecimalType, LongType) =>
        mergeDecimalWithLong(d)
      case (LongType, d: DecimalType) =>
        mergeDecimalWithLong(d)
      case (StructType(fields1), StructType(fields2)) =>
        // Rely on fields being sorted by name, and merge fields with the same name recursively.
        val newFields = new java.util.ArrayList[StructField]()

        var f1Idx = 0
        var f2Idx = 0
        // We end up dropping all but 300 fields in the final schema, but add a cap on how many
        // we'll try to track to avoid memory/time blow-ups in the intermediate state.
        val maxStructSize = 1000

        while (f1Idx < fields1.length && f2Idx < fields2.length && newFields.size < maxStructSize) {
          val f1Name = fields1(f1Idx).name
          val f2Name = fields2(f2Idx).name
          val comp = f1Name.compareTo(f2Name)
          if (comp == 0) {
            val dataType = mergeSchema(fields1(f1Idx).dataType, fields2(f2Idx).dataType)
            val c1 = getFieldCount(fields1(f1Idx))
            val c2 = getFieldCount(fields2(f2Idx))
            newFields.add(
              StructField(
                f1Name,
                dataType,
                metadata = new MetadataBuilder().putLong(COUNT_METADATA_KEY, c1 + c2).build()
              )
            )
            f1Idx += 1
            f2Idx += 1
          } else if (comp < 0) { // f1Name < f2Name
            newFields.add(fields1(f1Idx))
            f1Idx += 1
          } else { // f1Name > f2Name
            newFields.add(fields2(f2Idx))
            f2Idx += 1
          }
        }
        while (f1Idx < fields1.length && newFields.size < maxStructSize) {
          newFields.add(fields1(f1Idx))
          f1Idx += 1
        }
        while (f2Idx < fields2.length && newFields.size < maxStructSize) {
          newFields.add(fields2(f2Idx))
          f2Idx += 1
        }
        StructType(newFields.toArray(Array.empty[StructField]))
      case (ArrayType(e1, _), ArrayType(e2, _)) =>
        ArrayType(mergeSchema(e1, e2))
      // For any other scalar types, the types must be identical, or we give up and use Variant.
      case (_, _) if dt1 == dt2 => dt1
      case _ => VariantType
    }
  }

  /**
   * Return a new schema, with each VariantType replaced its inferred shredding schema.
   */
  private def updateSchema(
      schema: StructType,
      inferredSchemas: Map[Seq[Int], StructType],
      path: Seq[Int] = Seq()): StructType = {
    val newFields = schema.fields.zipWithIndex.map {
      case (field, idx) =>
        field.dataType match {
          case VariantType =>
            // Right now, we infer a schema for every VariantType that isn't nested in an array or
            // map, so we should always find a replacement.
            val fullPath = path :+ idx
            assert(inferredSchemas.contains(fullPath))
            field.copy(dataType = inferredSchemas(fullPath))
          case inner: StructType =>
            val newType = updateSchema(inner, inferredSchemas, path :+ idx)
            field.copy(dataType = newType)
          case dt => field
        }
    }
    StructType(newFields)
  }

  // Container for a mutable integer, to track the total number of shredded fields we can add across
  // the file. It should be initialized to the maximum allowed across the file schema.
  // `finalizeSimpleSchema` decrements it, and stops adding new fields once it hits 0.
  private case class MaxFields(var remaining: Int)

  /**
   * Given the schema of a Variant type, finalize the schema. Specifically:
   * 1) Widen integer types to LongType, since it adds flexibility for shredding, and
   *    shouldn't have much storage size impact after encoding.
   * 2) Replace empty structs with VariantType, since empty structs are invalid in Parquet.
   * 3) Limit the total number of shredded fields in the schema
   */
  private def finalizeSimpleSchema(
      dt: DataType,
      minCardinality: Int,
      maxFields: MaxFields): DataType = {
    // Every field uses a value column.
    maxFields.remaining -= 1
    if (maxFields.remaining <= 0) {
      // No space left for a typed_value. Use VariantType, which only consumes a value column.
      return VariantType
    }

    dt match {
      case StructType(fields) =>
        val newFields = new java.util.ArrayList[StructField]()
        // Drop fields with less than the required cardinality.
        fields
          .filter(getFieldCount(_) >= minCardinality)
          .foreach { field =>
            if (maxFields.remaining > 0) {
              newFields.add(
                field.copy(
                  dataType = finalizeSimpleSchema(field.dataType, minCardinality, maxFields)
                )
              )
            }
          }
        // If we weren't able to retain any fields, just use VariantType
        if (newFields.size() > 0) StructType(newFields) else VariantType
      case ArrayType(elementType, _) =>
        ArrayType(finalizeSimpleSchema(elementType, minCardinality, maxFields))
      case ByteType | ShortType | IntegerType | LongType =>
        maxFields.remaining -= 1
        // We widen all integer types to long. There isn't much benefit to shredding as a
        // narrower integer type.
        LongType
      case d: DecimalType if d.precision <= 18 && d.scale == 0 =>
        // This was probably an integer type originally, and we converted to Decimal(N, 0) to
        // allow it to merge with decimal. Since it still has 0 scale, we can convert back to
        // LongType in the final schema.
        maxFields.remaining -= 1
        LongType
      case d: DecimalType =>
        // Store as 8-byte if precision is small enough, otherwise use 16-byte decimal.
        maxFields.remaining -= 1
        if (d.precision <= Decimal.MAX_LONG_DIGITS) {
          DecimalType(Decimal.MAX_LONG_DIGITS, d.scale)
        } else {
          DecimalType(DecimalType.MAX_PRECISION, d.scale)
        }
      case VariantType | NullType =>
        // VariantType and NullType don't have a corresponding typed_value. They just write
        // to the value column.
        VariantType
      case t =>
        // All other scalar types use typed_value.
        maxFields.remaining -= 1
        t
    }
  }

  def inferSchema(rows: Seq[InternalRow]): StructType = {
    // For each path to a Variant value, iterate over all rows and update the inferred schema.
    // Add the result to a map, which we'll use to update the full schema.
    // maxShreddedFieldsPerFile is a global max for all fields, so initialize it here.
    val maxFields = MaxFields(maxShreddedFieldsPerFile)
    val inferredSchemas = pathsToVariant.map { path =>
      var numNonNullValues = 0
      val simpleSchema = rows.foldLeft(NullType: DataType) {
        case (partialSchema, row) =>
          getValueAtPath(schema, row, path).map { variantVal =>
            numNonNullValues += 1
            val v = new Variant(variantVal.getValue, variantVal.getMetadata)
            val schemaOfRow = schemaOf(v, maxShreddingDepth)
            mergeSchema(partialSchema, schemaOfRow)
          // If getValueAtPath returned None, the value is null in this row; just ignore.
          }
          .getOrElse(partialSchema)
        // If we didn't find any non-null rows, use an unshredded schema.
      }

      // Don't infer a schema for fields that appear in less than 10% of rows.
      // Ensure that minCardinality is at least 1 if we have any rows.
      val minCardinality = (numNonNullValues + 9) / 10

      val finalizedSchema = finalizeSimpleSchema(simpleSchema, minCardinality, maxFields)
      val shreddingSchema = SparkShreddingUtils.variantShreddingSchema(finalizedSchema)
      val schemaWithMetadata = SparkShreddingUtils.addWriteShreddingMetadata(shreddingSchema)
      (path, schemaWithMetadata)
    }.toMap

    // Insert each inferred schema into the full schema.
    updateSchema(schema, inferredSchemas)
  }
}
