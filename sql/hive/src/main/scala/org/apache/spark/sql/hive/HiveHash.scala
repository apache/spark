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

package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

/**
 * Simulates Hive's hashing function at
 * org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils#hashcode() in Hive
 *
 * We should use this hash function for both shuffle and bucket of Hive tables, so that
 * we can guarantee shuffle and bucketing have same data distribution
 *
 * TODO: Support Decimal and date related types
 */
@ExpressionDescription(
  usage = "_FUNC_(a1, a2, ...) - Returns a hash value of the arguments.")
case class HiveHash(children: Seq[Expression], seed: Int) extends HashExpression[Int] {
  def this(arguments: Seq[Expression]) = this(arguments, 42)

  override def dataType: DataType = IntegerType

  override def prettyName: String = "hive-hash"

  override protected def hasherClassName: String = classOf[HiveHash].getName

  override protected def computeHash(value: Any, dataType: DataType, seed: Int): Int = {
    HiveHashFunction.hash(value, dataType, seed).toInt
  }
}

object HiveHashFunction extends InterpretedHashFunction {
  override protected def hashInt(i: Int, seed: Long): Long = {
    HiveHasher.hashInt(i, seed.toInt)
  }

  override protected def hashLong(l: Long, seed: Long): Long = {
    HiveHasher.hashLong(l, seed.toInt)
  }

  override protected def hashUnsafeBytes(base: AnyRef, offset: Long, len: Int, seed: Long): Long = {
    HiveHasher.hashUnsafeBytes(base, offset, len, seed.toInt)
  }

  override def hash(value: Any, dataType: DataType, seed: Long): Long = {
    value match {
      case s: UTF8String =>
        val bytes = s.getBytes
        var result: Int = 0
        var i = 0
        while (i < bytes.length) {
          result = (result * 31) + bytes(i).toInt
          i += 1
        }
        result


      case array: ArrayData =>
        val elementType = dataType match {
          case udt: UserDefinedType[_] => udt.sqlType.asInstanceOf[ArrayType].elementType
          case ArrayType(et, _) => et
        }
        var result: Int = 0
        var i = 0
        while (i < array.numElements()) {
          result = (31 * result) + hash(array.get(i, elementType), elementType, 0).toInt
          i += 1
        }
        result

      case map: MapData =>
        val (kt, vt) = dataType match {
          case udt: UserDefinedType[_] =>
            val mapType = udt.sqlType.asInstanceOf[MapType]
            mapType.keyType -> mapType.valueType
          case MapType(_kt, _vt, _) => _kt -> _vt
        }
        val keys = map.keyArray()
        val values = map.valueArray()
        var result: Int = 0
        var i = 0
        while (i < map.numElements()) {
          result += hash(keys.get(i, kt), kt, 0).toInt ^ hash(values.get(i, vt), vt, 0).toInt
          i += 1
        }
        result

      case struct: InternalRow =>
        val types: Array[DataType] = dataType match {
          case udt: UserDefinedType[_] =>
            udt.sqlType.asInstanceOf[StructType].map(_.dataType).toArray
          case StructType(fields) => fields.map(_.dataType)
        }

        var i = 0
        var result = 0
        val len = struct.numFields
        while (i < len) {
          result = (31 * result) + hash(struct.get(i, types(i)), types(i), seed).toInt
          i += 1
        }
        result

      case _ => super.hash(value, dataType, seed)
    }
  }
}

object HiveHasher {
  def hashInt(input: Int, seed: Int): Int = input

  def hashLong(input: Long, seed: Int): Int = ((input >>> 32) ^ input).toInt

  def hashUnsafeBytes(base: AnyRef, offset: Long, lengthInBytes: Int, seed: Int): Int = {
    assert(lengthInBytes >= 0, "lengthInBytes cannot be negative")

    var result: Int = 0
    var i: Int = lengthInBytes - lengthInBytes % 4
    while (i < lengthInBytes) {
      result = (result * 31) + Platform.getByte(base, offset + i).toInt
      i += 1
    }
    result
  }
}
