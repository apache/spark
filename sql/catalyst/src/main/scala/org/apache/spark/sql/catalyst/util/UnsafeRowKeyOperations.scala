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

package org.apache.spark.sql.catalyst.util

import com.ibm.icu.text.RawCollationKey

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecializedGetters, UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.unsafe.types.UTF8String

/** Hashes and compares unsafe grouping keys containing non-binary collated strings. */
final class UnsafeRowKeyOperations(val schema: StructType)
    extends BytesToBytesMap.KeyOperationsFactory {
  import UnsafeRowKeyOperations._

  require(
    schema.forall(field => supportsDataType(field.dataType)),
    s"Unsupported grouping key schema: $schema")

  private val operations = new RowOperations(schema, new HashScratch)

  def hash(row: UnsafeRow): Int = operations.hash(row, HASH_SEED)

  def areEqual(left: UnsafeRow, right: UnsafeRow): Boolean = {
    operations.areEqual(left, right)
  }

  override def create(): BytesToBytesMap.KeyOperations = new BytesToBytesMap.KeyOperations {
    private val localOperations = new RowOperations(schema, new HashScratch)
    private val left = new UnsafeRow(schema.length)
    private val right = new UnsafeRow(schema.length)

    override def hash(base: AnyRef, offset: Long, length: Int): Int = {
      left.pointTo(base, offset, length)
      localOperations.hash(left, HASH_SEED)
    }

    override def equals(
        leftBase: AnyRef,
        leftOffset: Long,
        leftLength: Int,
        rightBase: AnyRef,
        rightOffset: Long,
        rightLength: Int): Boolean = {
      left.pointTo(leftBase, leftOffset, leftLength)
      right.pointTo(rightBase, rightOffset, rightLength)
      localOperations.areEqual(left, right)
    }
  }
}

object UnsafeRowKeyOperations {
  private val HASH_SEED = 42

  /** Returns whether this type can be hashed and compared as part of an unsafe row key. */
  def supportsDataType(dataType: DataType): Boolean = {
    if (UnsafeRowUtils.isBinaryStable(dataType)) {
      true
    } else {
      dataType match {
        case st: StringType => !st.supportsBinaryEquality
        case ArrayType(elementType, _) => supportsDataType(elementType)
        case StructType(fields) => fields.forall(field => supportsDataType(field.dataType))
        case _ => false
      }
    }
  }

  private final class BinaryRegion {
    var base: AnyRef = _
    var offset: Long = 0L
    var length: Int = 0
  }

  private final class HashScratch {
    private var reusableRawCollationKey: RawCollationKey = _

    def rawCollationKey: RawCollationKey = {
      if (reusableRawCollationKey == null) {
        reusableRawCollationKey = new RawCollationKey()
      }
      reusableRawCollationKey
    }
  }

  private sealed trait FieldOperations {
    final def hash(input: SpecializedGetters, ordinal: Int, seed: Int): Int = {
      if (input.isNullAt(ordinal)) seed else hashNonNull(input, ordinal, seed)
    }

    final def areEqual(
        left: SpecializedGetters,
        leftOrdinal: Int,
        right: SpecializedGetters,
        rightOrdinal: Int): Boolean = {
      val leftIsNull = left.isNullAt(leftOrdinal)
      val rightIsNull = right.isNullAt(rightOrdinal)
      if (leftIsNull || rightIsNull) {
        leftIsNull == rightIsNull
      } else {
        areEqualNonNull(left, leftOrdinal, right, rightOrdinal)
      }
    }

    protected def hashNonNull(input: SpecializedGetters, ordinal: Int, seed: Int): Int

    protected def areEqualNonNull(
        left: SpecializedGetters,
        leftOrdinal: Int,
        right: SpecializedGetters,
        rightOrdinal: Int): Boolean
  }

  private final class BinaryStableOperations(dataType: DataType) extends FieldOperations {
    private val leftRegion = new BinaryRegion
    private val rightRegion = new BinaryRegion

    override protected def hashNonNull(
        input: SpecializedGetters, ordinal: Int, seed: Int): Int = {
      setRegion(input, ordinal, leftRegion)
      Murmur3_x86_32.hashUnsafeBytes(
        leftRegion.base, leftRegion.offset, leftRegion.length, seed)
    }

    override protected def areEqualNonNull(
        left: SpecializedGetters,
        leftOrdinal: Int,
        right: SpecializedGetters,
        rightOrdinal: Int): Boolean = {
      setRegion(left, leftOrdinal, leftRegion)
      setRegion(right, rightOrdinal, rightRegion)
      leftRegion.length == rightRegion.length && ByteArrayMethods.arrayEquals(
        leftRegion.base,
        leftRegion.offset,
        rightRegion.base,
        rightRegion.offset,
        leftRegion.length)
    }

    private def setRegion(
        input: SpecializedGetters, ordinal: Int, region: BinaryRegion): Unit = input match {
      case row: UnsafeRow =>
        region.base = row.getBaseObject
        if (UnsafeRow.isFixedLength(dataType)) {
          region.offset = row.getBaseOffset +
            UnsafeRow.calculateBitSetWidthInBytes(row.numFields()) + ordinal * 8L
          region.length = 8
        } else {
          setVariableLengthRegion(row.getBaseOffset, row.getLong(ordinal), region)
        }

      case other =>
        throw SparkException.internalError(
          s"Expected unsafe grouping-key storage, found ${other.getClass.getName}")
    }
  }

  private final class CollatedStringOperations(
      dataType: StringType,
      hashScratch: HashScratch) extends FieldOperations {
    private val collation = CollationFactory.fetchCollation(dataType.collationId)
    private val useRawCollationKey = collation.provider == CollationFactory.PROVIDER_ICU

    override protected def hashNonNull(
        input: SpecializedGetters, ordinal: Int, seed: Int): Int = {
      val value = input.getUTF8String(ordinal)
      if (useRawCollationKey) {
        hashICUCollationKey(value, seed)
      } else {
        val key = collation.sortKeyFunction.apply(value)
        Murmur3_x86_32.hashUnsafeBytes(key, Platform.BYTE_ARRAY_OFFSET, key.length, seed)
      }
    }

    /**
     * Hashes an ICU sort key from a reusable buffer after applying configured space trimming.
     * `toValidString` applies Spark's replacement policy for malformed UTF-8 before calling ICU.
     */
    private def hashICUCollationKey(value: UTF8String, seed: Int): Int = {
      val normalizedValue = if (collation.supportsSpaceTrimming) {
        CollationFactory.applyTrimmingPolicy(value, dataType.collationId)
      } else {
        value
      }
      val key = collation.getCollator.getRawCollationKey(
        normalizedValue.toValidString, hashScratch.rawCollationKey)
      Murmur3_x86_32.hashUnsafeBytes(
        key.bytes, Platform.BYTE_ARRAY_OFFSET, key.size, seed)
    }

    override protected def areEqualNonNull(
        left: SpecializedGetters,
        leftOrdinal: Int,
        right: SpecializedGetters,
        rightOrdinal: Int): Boolean = {
      collation.equalsFunction.apply(
        left.getUTF8String(leftOrdinal), right.getUTF8String(rightOrdinal))
    }
  }

  private final class ArrayOperations(
      elementType: DataType,
      hashScratch: HashScratch) extends FieldOperations {
    private val elementOperations = createFieldOperations(elementType, hashScratch)
    private val hashArray = new UnsafeArrayData
    private val leftArray = new UnsafeArrayData
    private val rightArray = new UnsafeArrayData

    override protected def hashNonNull(
        input: SpecializedGetters, ordinal: Int, seed: Int): Int = {
      pointToArray(input, ordinal, hashArray)
      var result = seed
      var index = 0
      while (index < hashArray.numElements()) {
        result = elementOperations.hash(hashArray, index, result)
        index += 1
      }
      result
    }

    override protected def areEqualNonNull(
        left: SpecializedGetters,
        leftOrdinal: Int,
        right: SpecializedGetters,
        rightOrdinal: Int): Boolean = {
      pointToArray(left, leftOrdinal, leftArray)
      pointToArray(right, rightOrdinal, rightArray)
      if (leftArray.numElements() != rightArray.numElements()) {
        return false
      }
      var index = 0
      while (index < leftArray.numElements()) {
        if (!elementOperations.areEqual(leftArray, index, rightArray, index)) {
          return false
        }
        index += 1
      }
      true
    }
  }

  private final class StructOperations(
      dataType: StructType,
      hashScratch: HashScratch) extends FieldOperations {
    private val operations = new RowOperations(dataType, hashScratch)
    private val hashRow = new UnsafeRow(dataType.length)
    private val leftRow = new UnsafeRow(dataType.length)
    private val rightRow = new UnsafeRow(dataType.length)

    override protected def hashNonNull(
        input: SpecializedGetters, ordinal: Int, seed: Int): Int = {
      pointToStruct(input, ordinal, hashRow)
      operations.hash(hashRow, seed)
    }

    override protected def areEqualNonNull(
        left: SpecializedGetters,
        leftOrdinal: Int,
        right: SpecializedGetters,
        rightOrdinal: Int): Boolean = {
      pointToStruct(left, leftOrdinal, leftRow)
      pointToStruct(right, rightOrdinal, rightRow)
      operations.areEqual(leftRow, rightRow)
    }
  }

  private final class RowOperations(dataType: StructType, hashScratch: HashScratch) {
    private val fieldOperations =
      dataType.fields.map(field => createFieldOperations(field.dataType, hashScratch))

    def hash(row: InternalRow, seed: Int): Int = {
      var result = seed
      var ordinal = 0
      while (ordinal < fieldOperations.length) {
        result = fieldOperations(ordinal).hash(row, ordinal, result)
        ordinal += 1
      }
      result
    }

    def areEqual(left: InternalRow, right: InternalRow): Boolean = {
      var ordinal = 0
      while (ordinal < fieldOperations.length) {
        if (!fieldOperations(ordinal).areEqual(left, ordinal, right, ordinal)) {
          return false
        }
        ordinal += 1
      }
      true
    }
  }

  private def createFieldOperations(
      dataType: DataType,
      hashScratch: HashScratch): FieldOperations = {
    if (UnsafeRowUtils.isBinaryStable(dataType)) {
      new BinaryStableOperations(dataType)
    } else {
      dataType match {
        case stringType: StringType => new CollatedStringOperations(stringType, hashScratch)
        case ArrayType(elementType, _) => new ArrayOperations(elementType, hashScratch)
        case structType: StructType => new StructOperations(structType, hashScratch)
        case _ =>
          throw SparkException.internalError(s"Unsupported grouping key type: $dataType")
      }
    }
  }

  private def setVariableLengthRegion(
      baseOffset: Long, offsetAndSize: Long, region: BinaryRegion): Unit = {
    region.offset = baseOffset + (offsetAndSize >> 32).toInt
    region.length = offsetAndSize.toInt
  }

  private def pointToArray(
      input: SpecializedGetters,
      ordinal: Int,
      target: UnsafeArrayData): Unit = input match {
    case row: UnsafeRow =>
      pointToArray(row.getBaseObject, row.getBaseOffset, row.getLong(ordinal), target)
    case array: UnsafeArrayData =>
      pointToArray(array.getBaseObject, array.getBaseOffset, array.getLong(ordinal), target)
    case other =>
      throw SparkException.internalError(
        s"Expected unsafe grouping-key storage, found ${other.getClass.getName}")
  }

  private def pointToArray(
      base: AnyRef,
      baseOffset: Long,
      offsetAndSize: Long,
      target: UnsafeArrayData): Unit = {
    target.pointTo(
      base,
      baseOffset + (offsetAndSize >> 32).toInt,
      offsetAndSize.toInt)
  }

  private def pointToStruct(
      input: SpecializedGetters,
      ordinal: Int,
      target: UnsafeRow): Unit = input match {
    case row: UnsafeRow =>
      pointToStruct(row.getBaseObject, row.getBaseOffset, row.getLong(ordinal), target)
    case array: UnsafeArrayData =>
      pointToStruct(array.getBaseObject, array.getBaseOffset, array.getLong(ordinal), target)
    case other =>
      throw SparkException.internalError(
        s"Expected unsafe grouping-key storage, found ${other.getClass.getName}")
  }

  private def pointToStruct(
      base: AnyRef,
      baseOffset: Long,
      offsetAndSize: Long,
      target: UnsafeRow): Unit = {
    target.pointTo(
      base,
      baseOffset + (offsetAndSize >> 32).toInt,
      offsetAndSize.toInt)
  }
}
