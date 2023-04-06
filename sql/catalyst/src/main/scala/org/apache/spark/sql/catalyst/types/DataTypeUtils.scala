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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

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
}

