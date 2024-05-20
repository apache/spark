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
package org.apache.spark.sql.catalyst.types

import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Literal}
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLId
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy.{ANSI, STRICT}
import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, Decimal, DecimalType, MapType, NullType, StructField, StructType, UserDefinedType}
import org.apache.spark.sql.types.DecimalType.{forType, fromDecimal}

object DataTypeUtils {
  /**
   * Check if `this` and `other` are the same data type when ignoring nullability
   * (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).
   */
  def sameType(left: DataType, right: DataType): Boolean = left.sameType(right)

  /**
   * Compares two types, ignoring nullability of ArrayType, MapType, StructType.
   */
  def equalsIgnoreNullability(left: DataType, right: DataType): Boolean = {
    DataType.equalsIgnoreNullability(left, right)
  }

  /**
   * Compares two types, ignoring nullability of ArrayType, MapType, StructType, and ignoring case
   * sensitivity of field names in StructType.
   */
  def equalsIgnoreCaseAndNullability(from: DataType, to: DataType): Boolean = {
    DataType.equalsIgnoreCaseAndNullability(from, to)
  }

  private val SparkGeneratedName = """col\d+""".r
  private def isSparkGeneratedName(name: String): Boolean = name match {
    case SparkGeneratedName(_*) => true
    case _ => false
  }

  /**
   * Returns true if the write data type can be read using the read data type.
   *
   * The write type is compatible with the read type if:
   * - Both types are arrays, the array element types are compatible, and element nullability is
   *   compatible (read allows nulls or write does not contain nulls).
   * - Both types are maps and the map key and value types are compatible, and value nullability
   *   is compatible  (read allows nulls or write does not contain nulls).
   * - Both types are structs and have the same number of fields. The type and nullability of each
   *   field from read/write is compatible. If byName is true, the name of each field from
   *   read/write needs to be the same.
   * - It is user defined type and its underlying sql type is same as the read type, or the read
   *   type is user defined type and its underlying sql type is same as the write type.
   * - Both types are atomic and the write type can be safely cast to the read type.
   *
   * Extra fields in write-side structs are not allowed to avoid accidentally writing data that
   * the read schema will not read, and to ensure map key equality is not changed when data is read.
   *
   * @param write a write-side data type to validate against the read type
   * @param read a read-side data type
   * @return true if data written with the write type can be read using the read type
   */
  def canWrite(
      tableName: String,
      write: DataType,
      read: DataType,
      byName: Boolean,
      resolver: Resolver,
      context: String,
      storeAssignmentPolicy: StoreAssignmentPolicy.Value,
      addError: String => Unit): Boolean = {
    (write, read) match {
      case (wArr: ArrayType, rArr: ArrayType) =>
        // run compatibility check first to produce all error messages
        val typesCompatible = canWrite(
          tableName, wArr.elementType, rArr.elementType, byName, resolver, context + ".element",
          storeAssignmentPolicy, addError)

        if (wArr.containsNull && !rArr.containsNull) {
          throw QueryCompilationErrors.incompatibleDataToTableNullableArrayElementsError(
            tableName, context
          )
        } else {
          typesCompatible
        }

      case (wMap: MapType, rMap: MapType) =>
        // map keys cannot include data fields not in the read schema without changing equality when
        // read. map keys can be missing fields as long as they are nullable in the read schema.

        // run compatibility check first to produce all error messages
        val keyCompatible = canWrite(
          tableName, wMap.keyType, rMap.keyType, byName, resolver, context + ".key",
          storeAssignmentPolicy, addError)
        val valueCompatible = canWrite(
          tableName, wMap.valueType, rMap.valueType, byName, resolver, context + ".value",
          storeAssignmentPolicy, addError)

        if (wMap.valueContainsNull && !rMap.valueContainsNull) {
          throw QueryCompilationErrors.incompatibleDataToTableNullableMapValuesError(
            tableName, context
          )
        } else {
          keyCompatible && valueCompatible
        }

      case (StructType(writeFields), StructType(readFields)) =>
        var fieldCompatible = true
        readFields.zip(writeFields).zipWithIndex.foreach {
          case ((rField, wField), i) =>
            val nameMatch = resolver(wField.name, rField.name) || isSparkGeneratedName(wField.name)
            val fieldContext = s"$context.${rField.name}"
            val typesCompatible = canWrite(
              tableName, wField.dataType, rField.dataType, byName, resolver, fieldContext,
              storeAssignmentPolicy, addError)

            if (byName && !nameMatch) {
              throw QueryCompilationErrors.incompatibleDataToTableUnexpectedColumnNameError(
                tableName, context, i, rField.name, wField.name)
            } else if (!rField.nullable && wField.nullable) {
              throw QueryCompilationErrors.incompatibleDataToTableNullableColumnError(
                tableName, fieldContext)
            } else if (!typesCompatible) {
              // errors are added in the recursive call to canWrite above
              fieldCompatible = false
            }
        }

        if (readFields.length > writeFields.length) {
          val missingFieldsStr = readFields.takeRight(readFields.length - writeFields.length)
            .map(f => s"${toSQLId(f.name)}").mkString(", ")
          if (missingFieldsStr.nonEmpty) {
            throw QueryCompilationErrors.incompatibleDataToTableStructMissingFieldsError(
              tableName, context, missingFieldsStr)
          }

        } else if (writeFields.length > readFields.length) {
          val extraFieldsStr = writeFields.takeRight(writeFields.length - readFields.length)
            .map(f => s"${toSQLId(f.name)}").mkString(", ")
          throw QueryCompilationErrors.incompatibleDataToTableExtraStructFieldsError(
            tableName, context, extraFieldsStr
          )
        }

        fieldCompatible

      case (w: AtomicType, r: AtomicType) if storeAssignmentPolicy == STRICT =>
        if (!Cast.canUpCast(w, r)) {
          throw QueryCompilationErrors.incompatibleDataToTableCannotSafelyCastError(
            tableName, context, w.catalogString, r.catalogString
          )
        } else {
          true
        }

      case (_: NullType, _) if storeAssignmentPolicy == ANSI => true

      case (w: AtomicType, r: AtomicType) if storeAssignmentPolicy == ANSI =>
        if (!Cast.canANSIStoreAssign(w, r)) {
          throw QueryCompilationErrors.incompatibleDataToTableCannotSafelyCastError(
            tableName, context, w.catalogString, r.catalogString
          )
        } else {
          true
        }

      case (w, r) if DataTypeUtils.sameType(w, r) && !w.isInstanceOf[NullType] =>
        true

      // If write-side data type is a user-defined type, check with its underlying data type.
      case (w, r) if w.isInstanceOf[UserDefinedType[_]] && !r.isInstanceOf[UserDefinedType[_]] =>
        canWrite(tableName, w.asInstanceOf[UserDefinedType[_]].sqlType, r, byName, resolver,
          context, storeAssignmentPolicy, addError)

      // If read-side data type is a user-defined type, check with its underlying data type.
      case (w, r) if r.isInstanceOf[UserDefinedType[_]] && !w.isInstanceOf[UserDefinedType[_]] =>
        canWrite(tableName, w, r.asInstanceOf[UserDefinedType[_]].sqlType, byName, resolver,
          context, storeAssignmentPolicy, addError)

      case (w, r) =>
        throw QueryCompilationErrors.incompatibleDataToTableCannotSafelyCastError(
          tableName, context, w.catalogString, r.catalogString
        )
    }
  }

  /**
   * Convert a StructField to a AttributeReference.
   */
  def toAttribute(field: StructField): AttributeReference =
    AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()

  /**
   * Convert a [[StructType]] into a Seq of [[AttributeReference]].
   */
  def toAttributes(schema: StructType): Seq[AttributeReference] = {
    schema.map(toAttribute)
  }

  def fromAttributes(attributes: Seq[Attribute]): StructType =
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

  /**
   * Convert a literal to a DecimalType.
   */
  def fromLiteral(literal: Literal): DecimalType = literal.value match {
    case v: Short => fromDecimal(Decimal(BigDecimal(v)))
    case v: Int => fromDecimal(Decimal(BigDecimal(v)))
    case v: Long => fromDecimal(Decimal(BigDecimal(v)))
    case _ => forType(literal.dataType)
  }
}

