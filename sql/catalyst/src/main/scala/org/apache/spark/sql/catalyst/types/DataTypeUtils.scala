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
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy.{ANSI, STRICT}
import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, MapType, NullType, StructType}

object DataTypeUtils {
  /**
   * Check if `this` and `other` are the same data type when ignoring nullability
   * (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).
   */
  def sameType(left: DataType, right: DataType): Boolean =
    if (SQLConf.get.caseSensitiveAnalysis) {
      equalsIgnoreNullability(left, right)
    } else {
      equalsIgnoreCaseAndNullability(left, right)
    }

  /**
   * Compares two types, ignoring nullability of ArrayType, MapType, StructType.
   */
  def equalsIgnoreNullability(left: DataType, right: DataType): Boolean = {
    (left, right) match {
      case (ArrayType(leftElementType, _), ArrayType(rightElementType, _)) =>
        equalsIgnoreNullability(leftElementType, rightElementType)
      case (MapType(leftKeyType, leftValueType, _), MapType(rightKeyType, rightValueType, _)) =>
        equalsIgnoreNullability(leftKeyType, rightKeyType) &&
          equalsIgnoreNullability(leftValueType, rightValueType)
      case (StructType(leftFields), StructType(rightFields)) =>
        leftFields.length == rightFields.length &&
          leftFields.zip(rightFields).forall { case (l, r) =>
            l.name == r.name && equalsIgnoreNullability(l.dataType, r.dataType)
          }
      case (l, r) => l == r
    }
  }

  /**
   * Compares two types, ignoring nullability of ArrayType, MapType, StructType, and ignoring case
   * sensitivity of field names in StructType.
   */
  def equalsIgnoreCaseAndNullability(from: DataType, to: DataType): Boolean = {
    (from, to) match {
      case (ArrayType(fromElement, _), ArrayType(toElement, _)) =>
        equalsIgnoreCaseAndNullability(fromElement, toElement)

      case (MapType(fromKey, fromValue, _), MapType(toKey, toValue, _)) =>
        equalsIgnoreCaseAndNullability(fromKey, toKey) &&
          equalsIgnoreCaseAndNullability(fromValue, toValue)

      case (StructType(fromFields), StructType(toFields)) =>
        fromFields.length == toFields.length &&
          fromFields.zip(toFields).forall { case (l, r) =>
            l.name.equalsIgnoreCase(r.name) &&
              equalsIgnoreCaseAndNullability(l.dataType, r.dataType)
          }

      case (fromDataType, toDataType) => fromDataType == toDataType
    }
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
          wArr.elementType, rArr.elementType, byName, resolver, context + ".element",
          storeAssignmentPolicy, addError)

        if (wArr.containsNull && !rArr.containsNull) {
          addError(s"Cannot write nullable elements to array of non-nulls: '$context'")
          false
        } else {
          typesCompatible
        }

      case (wMap: MapType, rMap: MapType) =>
        // map keys cannot include data fields not in the read schema without changing equality when
        // read. map keys can be missing fields as long as they are nullable in the read schema.

        // run compatibility check first to produce all error messages
        val keyCompatible = canWrite(
          wMap.keyType, rMap.keyType, byName, resolver, context + ".key",
          storeAssignmentPolicy, addError)
        val valueCompatible = canWrite(
          wMap.valueType, rMap.valueType, byName, resolver, context + ".value",
          storeAssignmentPolicy, addError)

        if (wMap.valueContainsNull && !rMap.valueContainsNull) {
          addError(s"Cannot write nullable values to map of non-nulls: '$context'")
          false
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
              wField.dataType, rField.dataType, byName, resolver, fieldContext,
              storeAssignmentPolicy, addError)

            if (byName && !nameMatch) {
              addError(s"Struct '$context' $i-th field name does not match " +
                s"(may be out of order): expected '${rField.name}', found '${wField.name}'")
              fieldCompatible = false
            } else if (!rField.nullable && wField.nullable) {
              addError(s"Cannot write nullable values to non-null field: '$fieldContext'")
              fieldCompatible = false
            } else if (!typesCompatible) {
              // errors are added in the recursive call to canWrite above
              fieldCompatible = false
            }
        }

        if (readFields.size > writeFields.size) {
          val missingFieldsStr = readFields.takeRight(readFields.size - writeFields.size)
            .map(f => s"'${f.name}'").mkString(", ")
          if (missingFieldsStr.nonEmpty) {
            addError(s"Struct '$context' missing fields: $missingFieldsStr")
            fieldCompatible = false
          }

        } else if (writeFields.size > readFields.size) {
          val extraFieldsStr = writeFields.takeRight(writeFields.size - readFields.size)
            .map(f => s"'${f.name}'").mkString(", ")
          addError(s"Cannot write extra fields to struct '$context': $extraFieldsStr")
          fieldCompatible = false
        }

        fieldCompatible

      case (w: AtomicType, r: AtomicType) if storeAssignmentPolicy == STRICT =>
        if (!Cast.canUpCast(w, r)) {
          addError(s"Cannot safely cast '$context': ${w.catalogString} to ${r.catalogString}")
          false
        } else {
          true
        }

      case (_: NullType, _) if storeAssignmentPolicy == ANSI => true

      case (w: AtomicType, r: AtomicType) if storeAssignmentPolicy == ANSI =>
        if (!Cast.canANSIStoreAssign(w, r)) {
          addError(s"Cannot safely cast '$context': ${w.catalogString} to ${r.catalogString}")
          false
        } else {
          true
        }

      case (w, r) if DataTypeUtils.sameType(w, r) && !w.isInstanceOf[NullType] =>
        true

      case (w, r) =>
        addError(s"Cannot write '$context': " +
          s"${w.catalogString} is incompatible with ${r.catalogString}")
        false
    }
  }
}

