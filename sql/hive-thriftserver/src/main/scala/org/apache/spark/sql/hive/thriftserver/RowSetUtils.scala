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

package org.apache.spark.sql.hive.thriftserver

import java.nio.ByteBuffer

import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

import org.apache.hive.service.rpc.thrift._

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.HiveResult._
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DoubleType, FloatType, GeographyType, GeometryType, IntegerType, LongType, ShortType, StringType}

object RowSetUtils {

  implicit def bitSetToBuffer(bitSet: java.util.BitSet): ByteBuffer = {
    ByteBuffer.wrap(bitSet.toByteArray)
  }

  def toTRowSet(
      startRowOffSet: Long,
      rows: Seq[Row],
      schema: Array[DataType],
      protocolVersion: TProtocolVersion): TRowSet = {
    if (protocolVersion.getValue < TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      toRowBasedSet(startRowOffSet, rows, schema, getTimeFormatters, getBinaryFormatter)
    } else {
      toColumnBasedSet(startRowOffSet, rows, schema, getTimeFormatters, getBinaryFormatter)
    }
  }

  private def toRowBasedSet(
      startRowOffSet: Long,
      rows: Seq[Row],
      schema: Array[DataType],
      timeFormatters: TimeFormatters,
      binaryFormatter: BinaryFormatter): TRowSet = {
    val tRows = rows.map { row =>
      val tRow = new TRow()
      var j = 0
      val columnSize = row.length
      while (j < columnSize) {
        val columnValue = toTColumnValue(j, row, schema(j), timeFormatters, binaryFormatter)
        tRow.addToColVals(columnValue)
        j += 1
      }
      tRow
    }.asJava
    new TRowSet(startRowOffSet, tRows)
  }

  // Dense `Int` codes for the per-column dispatch in `toColumnBasedSet`, resolved from `DataType`
  // once per column up front. Dispatching on one of these inside the single pass over all rows
  // compiles to a `tableswitch`, avoiding a virtual call through a column-type-specific writer:
  // with as few as 3 distinct column types in the schema (e.g. Int/Double/String round-robin),
  // that call site becomes megamorphic and the JIT can no longer inline or speculate on it, which
  // benchmarking showed can make a polymorphic-writer design slower than not doing a single pass
  // at all for moderately-sized pages (tens of thousands of rows).
  private final val KIND_BOOLEAN = 0
  private final val KIND_BYTE = 1
  private final val KIND_SHORT = 2
  private final val KIND_INT = 3
  private final val KIND_LONG = 4
  private final val KIND_FLOAT = 5
  private final val KIND_DOUBLE = 6
  private final val KIND_STRING = 7
  private final val KIND_BINARY = 8
  // Fallback for types with no fixed-width Thrift column representation (CHAR, VARCHAR,
  // non-default-collation strings, geospatial types, etc.), rendered with `toHiveString`.
  private final val KIND_HIVE_STRING = 9

  // Boxed singletons for null-cell filler values, so that a run of nulls in a column doesn't
  // allocate a fresh wrapper per cell (java.lang.Double has no small-value cache the way
  // java.lang.Integer/Long do, and a fresh empty ByteBuffer would otherwise be allocated per null
  // binary cell).
  private val DEFAULT_DOUBLE: java.lang.Double = java.lang.Double.valueOf(0.0)
  private val EMPTY_BYTE_BUFFER: ByteBuffer = ByteBuffer.wrap(Array.empty[Byte])

  private def kindOf(typ: DataType): Int = typ match {
    case BooleanType => KIND_BOOLEAN
    case ByteType => KIND_BYTE
    case ShortType => KIND_SHORT
    case IntegerType => KIND_INT
    case LongType => KIND_LONG
    case FloatType => KIND_FLOAT
    case DoubleType => KIND_DOUBLE
    case StringType => KIND_STRING
    case BinaryType => KIND_BINARY
    case _ => KIND_HIVE_STRING
  }

  // Cast back from the Any-typed buffer `toColumnBasedSet` accumulates into to the concrete
  // element type a Thrift T*Column constructor expects. Safe because `colValues(i)` is only ever
  // populated by the branch matching `kinds(i)`, which always adds the same boxed type for a
  // given kind.
  private def asList[T](values: java.util.ArrayList[Any]): java.util.List[T] =
    values.asInstanceOf[java.util.List[T]]

  private def toColumnBasedSet(
      startRowOffSet: Long,
      rows: Seq[Row],
      schema: Array[DataType],
      timeFormatters: TimeFormatters,
      binaryFormatter: BinaryFormatter): TRowSet = {
    val rowSize = rows.length
    val columnSize = schema.length

    val kinds = schema.map(kindOf)
    // Only meaningful for KIND_HIVE_STRING columns. `nested` was historically set to true for all
    // types by default, but ideally it should be false in general. This was never a problem
    // because other types that reach this branch do not use the `nested` flag in `toHiveString`.
    // Now, Geospatial types use it for wrapping EWKT in quotes when nested = true, so we need to
    // set `nested` here to false to avoid spurious quotes for standalone geo values. String types
    // need the same treatment: the fast path in `kindOf` matches only the default-collation
    // StringType singleton, so CHAR, VARCHAR and collated strings land here and would otherwise be
    // rendered as "value" instead of value.
    val hiveStringNested = schema.map {
      case _: GeometryType | _: GeographyType => false
      case _: StringType => false
      case _ => true
    }
    val defaultVals = kinds.map {
      case KIND_BOOLEAN => java.lang.Boolean.TRUE
      case KIND_BYTE => java.lang.Byte.valueOf(0.toByte)
      case KIND_SHORT => java.lang.Short.valueOf(0.toShort)
      case KIND_INT => java.lang.Integer.valueOf(0)
      case KIND_LONG => java.lang.Long.valueOf(0L)
      case KIND_FLOAT | KIND_DOUBLE => DEFAULT_DOUBLE
      case KIND_STRING | KIND_HIVE_STRING => ""
      case KIND_BINARY => EMPTY_BYTE_BUFFER
    }
    // Thrift's T*Column constructors all take a java.util.List, so one Any-typed ArrayList per
    // column (cast back to its concrete element type via `asList` when building the TColumn below)
    // does the same job as a separate Array[ArrayList[_]] per kind, without the bookkeeping of nine
    // mostly-empty arrays. Column-level setup here is O(columnSize), unlike the O(rowSize) work
    // below, so it doesn't need to be folded into a single pass.
    val colValues = Array.fill(columnSize)(new java.util.ArrayList[Any](rowSize))
    // Unlike colValues, not pre-sized to rowSize: colValues always ends up fully populated, but
    // most columns have few or no nulls in practice, and BitSet's backing long[] only grows (by
    // doubling) the first time `set` actually needs a bit beyond its current capacity. Pre-sizing
    // to rowSize would pay for a rowSize-bit array on every column up front, even ones that never
    // call `set` at all.
    val nulls = Array.fill(columnSize)(new java.util.BitSet())

    // Plain while/Iterator rather than `rows.foreach`: `rowIndex` is a var mutated from inside the
    // loop body, so a closure capturing it would force the compiler to box it into a heap-allocated
    // `IntRef` and add a `Function1` dispatch per row; up to a million rows, that's a million extra
    // indirections on the one loop in this method that actually scales with row count.
    var rowIndex = 0
    val rowIter = rows.iterator
    while (rowIter.hasNext) {
      val row = rowIter.next()
      var col = 0
      while (col < columnSize) {
        val v = row.get(col)
        val value = if (v == null) {
          // BitSet bits already default to false; only touch a bit when it needs to become true.
          // `set(idx, false)` on an untouched BitSet is a cheap no-op, but once any bit in the same
          // word has been set, `set(idx, false)` degrades to a clear() that also rescans backward
          // for the new highest in-use word, so unconditionally calling it on every non-null cell
          // can turn into O(rowSize) work per call for columns that do contain nulls.
          nulls(col).set(rowIndex)
          defaultVals(col)
        } else {
          kinds(col) match {
            case KIND_FLOAT =>
              // Floats are converted to doubles during thrift transportation. Passing float to
              // Double.valueOf directly causes precision loss, e.g. Double.valueOf(0.1f) is
              // 0.10000000149011612, so go through toString first, e.g. 0.1f.toString is "0.1".
              java.lang.Double.valueOf(v.toString)
            case KIND_BINARY =>
              ByteBuffer.wrap(v.asInstanceOf[Array[Byte]])
            case KIND_HIVE_STRING =>
              toHiveString(
                (v, schema(col)), hiveStringNested(col), timeFormatters, binaryFormatter)
            case _ =>
              v
          }
        }
        colValues(col).add(value)
        col += 1
      }
      rowIndex += 1
    }

    // Also O(columnSize), so built declaratively like `toRowBasedSet` above rather than with an
    // explicit accumulator loop.
    val columns = (0 until columnSize).map { i =>
      val values = colValues(i)
      kinds(i) match {
        case KIND_BOOLEAN => TColumn.boolVal(new TBoolColumn(asList(values), nulls(i)))
        case KIND_BYTE => TColumn.byteVal(new TByteColumn(asList(values), nulls(i)))
        case KIND_SHORT => TColumn.i16Val(new TI16Column(asList(values), nulls(i)))
        case KIND_INT => TColumn.i32Val(new TI32Column(asList(values), nulls(i)))
        case KIND_LONG => TColumn.i64Val(new TI64Column(asList(values), nulls(i)))
        case KIND_FLOAT | KIND_DOUBLE =>
          TColumn.doubleVal(new TDoubleColumn(asList(values), nulls(i)))
        case KIND_STRING | KIND_HIVE_STRING =>
          TColumn.stringVal(new TStringColumn(asList(values), nulls(i)))
        case KIND_BINARY => TColumn.binaryVal(new TBinaryColumn(asList(values), nulls(i)))
      }
    }.asJava
    val tRowSet = new TRowSet(startRowOffSet, new java.util.ArrayList[TRow]())
    tRowSet.setColumns(columns)
    tRowSet
  }

  private def toTColumnValue(
      ordinal: Int,
      row: Row,
      dataType: DataType,
      timeFormatters: TimeFormatters,
      binaryFormatter: BinaryFormatter): TColumnValue = {
    dataType match {
      case BooleanType =>
        val boolValue = new TBoolValue
        if (!row.isNullAt(ordinal)) boolValue.setValue(row.getBoolean(ordinal))
        TColumnValue.boolVal(boolValue)

      case ByteType =>
        val byteValue = new TByteValue
        if (!row.isNullAt(ordinal)) byteValue.setValue(row.getByte(ordinal))
        TColumnValue.byteVal(byteValue)

      case ShortType =>
        val tI16Value = new TI16Value
        if (!row.isNullAt(ordinal)) tI16Value.setValue(row.getShort(ordinal))
        TColumnValue.i16Val(tI16Value)

      case IntegerType =>
        val tI32Value = new TI32Value
        if (!row.isNullAt(ordinal)) tI32Value.setValue(row.getInt(ordinal))
        TColumnValue.i32Val(tI32Value)

      case LongType =>
        val tI64Value = new TI64Value
        if (!row.isNullAt(ordinal)) tI64Value.setValue(row.getLong(ordinal))
        TColumnValue.i64Val(tI64Value)

      case FloatType =>
        val tDoubleValue = new TDoubleValue
        if (!row.isNullAt(ordinal)) {
          // Floats are converted to doubles during thrift transportation.
          // Passing float to Double.valueOf causes precision loss, e.g.
          // scala> java.lang.Double.valueOf(0.1f)
          // res0: Double = 0.10000000149011612
          //
          // hereby toString is called ahead.
          // scala> java.lang.Double.valueOf(0.1f.toString)
          // res1: Double = 0.1
          val doubleValue = java.lang.Double.valueOf(row.getFloat(ordinal).toString)
          tDoubleValue.setValue(doubleValue)
        }
        TColumnValue.doubleVal(tDoubleValue)

      case DoubleType =>
        val tDoubleValue = new TDoubleValue
        if (!row.isNullAt(ordinal)) tDoubleValue.setValue(row.getDouble(ordinal))
        TColumnValue.doubleVal(tDoubleValue)

      case StringType =>
        val tStringValue = new TStringValue
        if (!row.isNullAt(ordinal)) tStringValue.setValue(row.getString(ordinal))
        TColumnValue.stringVal(tStringValue)

      case _ =>
        val tStrValue = new TStringValue
        if (!row.isNullAt(ordinal)) {
          val value = toHiveString(
            (row.get(ordinal), dataType), nested = false, timeFormatters, binaryFormatter)
          tStrValue.setValue(value)
        }
        TColumnValue.stringVal(tStrValue)
    }
  }
}
