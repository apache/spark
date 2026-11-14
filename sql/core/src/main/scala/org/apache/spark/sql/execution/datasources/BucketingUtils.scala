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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SpecializedGetters, SpecificInternalRow, UnaryExpression, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.st.STExpressionUtils.isGeoSpatialType
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{ArrayType, DataType, GeographyType, GeometryType, MapType, StructType, UserDefinedType}
import org.apache.spark.sql.util.SchemaUtils

object BucketingUtils {
  // The file name of bucketed data should have 3 parts:
  //   1. some other information in the head of file name
  //   2. bucket id part, some numbers, starts with "_"
  //      * The other-information part may use `-` as separator and may have numbers at the end,
  //        e.g. a normal parquet file without bucketing may have name:
  //        part-r-00000-2dd664f9-d2c4-4ffe-878f-431234567891.gz.parquet, and we will mistakenly
  //        treat `431234567891` as bucket id. So here we pick `_` as separator.
  //   3. optional file extension part, in the tail of file name, starts with `.`
  // An example of bucketed parquet file name with bucket id 3:
  //   part-r-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003.gz.parquet
  private val bucketedFileName = """.*_(\d+)(?:\..*)?$""".r

  // The reserved option name for data source to write Hive-compatible bucketed table
  val optionForHiveCompatibleBucketWrite = "__hive_compatible_bucketed_table_insertion__"

  def getBucketId(fileName: String): Option[Int] = fileName match {
    case bucketedFileName(bucketId) => Some(bucketId.toInt)
    case other => None
  }

  // Given bucketColumn, numBuckets and value, returns the corresponding bucketId
  def getBucketIdFromValue(bucketColumn: Attribute, numBuckets: Int, value: Any): Int = {
    val mutableInternalRow = new SpecificInternalRow(Seq(bucketColumn.dataType))
    mutableInternalRow.update(0, value)

    val bucketIdGenerator = UnsafeProjection.create(
      HashPartitioning(Seq(bucketColumn), numBuckets).partitionIdExpression :: Nil,
      bucketColumn :: Nil)
    bucketIdGenerator(mutableInternalRow).getInt(0)
  }

  def canBucketOn(dataType: DataType): Boolean = !SchemaUtils.hasNonUTF8BinaryCollation(dataType)

  // Whether the type is or contains GEOMETRY or GEOGRAPHY. Like the bucket id hash, this looks
  // through user-defined types.
  def containsGeoSpatialType(dataType: DataType): Boolean = dataType.existsRecursively {
    case udt: UserDefinedType[_] => containsGeoSpatialType(udt.sqlType)
    case other => isGeoSpatialType(other)
  }

  def bucketIdToString(id: Int): String = f"_$id%05d"
}

/**
 * Returns the value of a bucket column, and fails the write on the first non-NULL GEOMETRY or
 * GEOGRAPHY value in it, at the top level or nested. The bucket id of such a value hashes the WKB
 * the value was created from, but Parquet stores the value as re-encoded little-endian WKB, so
 * the value read back can hash to a different bucket than the file it is in. A NULL value hashes
 * the same before and after the round trip, so it is written.
 *
 * TODO: Support bucketed writes of GEOMETRY and GEOGRAPHY values, with bucket ids computed from
 * the bytes that the file format stores.
 */
case class RejectGeoSpatialBucketValue(child: Expression)
  extends UnaryExpression with CodegenFallback {

  override def dataType: DataType = child.dataType

  // Built once from the type, so that the check reads only the parts of a value whose type can
  // hold a GEOMETRY or GEOGRAPHY value.
  @transient private lazy val containsGeoSpatialValue: Any => Boolean = valueChecker(dataType)

  override protected def nullSafeEval(input: Any): Any = {
    if (containsGeoSpatialValue(input)) {
      throw QueryExecutionErrors.invalidBucketColumnDataTypeError(dataType)
    }
    input
  }

  // Returns whether a non-NULL value of the type holds a non-NULL GEOMETRY or GEOGRAPHY value.
  private def valueChecker(dataType: DataType): Any => Boolean = dataType match {
    case _ if !BucketingUtils.containsGeoSpatialType(dataType) => _ => false
    case _: GeometryType | _: GeographyType => _ => true
    case udt: UserDefinedType[_] => valueChecker(udt.sqlType)
    case st: StructType =>
      val fieldCheckers = st.fields.indices.map(i => (i, entryChecker(st(i).dataType)))
      value => fieldCheckers.exists { case (i, check) => check(value.asInstanceOf[InternalRow], i) }
    case ArrayType(elementType, _) =>
      val check = entryChecker(elementType)
      value => {
        val array = value.asInstanceOf[ArrayData]
        (0 until array.numElements()).exists(check(array, _))
      }
    case MapType(keyType, valueType, _) =>
      val keysCheck = valueChecker(ArrayType(keyType))
      val valuesCheck = valueChecker(ArrayType(valueType))
      value => {
        val map = value.asInstanceOf[MapData]
        keysCheck(map.keyArray()) || valuesCheck(map.valueArray())
      }
  }

  // Returns whether the entry at an ordinal of a struct or an array holds a non-NULL GEOMETRY or
  // GEOGRAPHY value. A GEOMETRY or GEOGRAPHY entry is only tested for NULL.
  private def entryChecker(dataType: DataType): (SpecializedGetters, Int) => Boolean =
    dataType match {
      case _ if !BucketingUtils.containsGeoSpatialType(dataType) => (_, _) => false
      case _: GeometryType | _: GeographyType => (data, i) => !data.isNullAt(i)
      case udt: UserDefinedType[_] => entryChecker(udt.sqlType)
      case _ =>
        val check = valueChecker(dataType)
        (data, i) => !data.isNullAt(i) && check(data.get(i, dataType))
    }

  override protected def withNewChildInternal(newChild: Expression): RejectGeoSpatialBucketValue =
    copy(child = newChild)
}
