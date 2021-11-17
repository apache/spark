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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Aggregation, Count, CountStar, Max, Min}
import org.apache.spark.sql.execution.RowToColumnConverter
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Utility class for aggregate push down to Parquet and ORC.
 */
object AggregatePushDownUtils {

  /**
   * Get the data schema for aggregate to be pushed down.
   */
  def getSchemaForPushedAggregation(
      aggregation: Aggregation,
      schema: StructType,
      partitionNames: Set[String],
      dataFilters: Seq[Expression]): Option[StructType] = {

    var finalSchema = new StructType()

    def getStructFieldForCol(col: NamedReference): StructField = {
      schema.apply(col.fieldNames.head)
    }

    def isPartitionCol(col: NamedReference) = {
      partitionNames.contains(col.fieldNames.head)
    }

    def processMinOrMax(agg: AggregateFunc): Boolean = {
      val (column, aggType) = agg match {
        case max: Max => (max.column, "max")
        case min: Min => (min.column, "min")
        case _ =>
          throw new IllegalArgumentException(s"Unexpected type of AggregateFunc ${agg.describe}")
      }

      if (isPartitionCol(column)) {
        // don't push down partition column, footer doesn't have max/min for partition column
        return false
      }
      val structField = getStructFieldForCol(column)

      structField.dataType match {
        // not push down complex type
        // not push down Timestamp because INT96 sort order is undefined,
        // Parquet doesn't return statistics for INT96
        // not push down Parquet Binary because min/max could be truncated
        // (https://issues.apache.org/jira/browse/PARQUET-1685), Parquet Binary
        // could be Spark StringType, BinaryType or DecimalType.
        // not push down for ORC with same reason.
        case BooleanType | ByteType | ShortType | IntegerType
             | LongType | FloatType | DoubleType | DateType =>
          finalSchema = finalSchema.add(structField.copy(s"$aggType(" + structField.name + ")"))
          true
        case _ =>
          false
      }
    }

    if (aggregation.groupByColumns.nonEmpty || dataFilters.nonEmpty) {
      // Parquet/ORC footer has max/min/count for columns
      // e.g. SELECT COUNT(col1) FROM t
      // but footer doesn't have max/min/count for a column if max/min/count
      // are combined with filter or group by
      // e.g. SELECT COUNT(col1) FROM t WHERE col2 = 8
      //      SELECT COUNT(col1) FROM t GROUP BY col2
      // However, if the filter is on partition column, max/min/count can still be pushed down
      // Todo:  add support if groupby column is partition col
      //        (https://issues.apache.org/jira/browse/SPARK-36646)
      return None
    }

    aggregation.aggregateExpressions.foreach {
      case max: Max =>
        if (!processMinOrMax(max)) return None
      case min: Min =>
        if (!processMinOrMax(min)) return None
      case count: Count =>
        if (count.column.fieldNames.length != 1 || count.isDistinct) return None
        finalSchema =
          finalSchema.add(StructField(s"count(" + count.column.fieldNames.head + ")", LongType))
      case _: CountStar =>
        finalSchema = finalSchema.add(StructField("count(*)", LongType))
      case _ =>
        return None
    }

    Some(finalSchema)
  }

  /**
   * Check if two Aggregation `a` and `b` is equal or not.
   */
  def equivalentAggregations(a: Aggregation, b: Aggregation): Boolean = {
    a.aggregateExpressions.sortBy(_.hashCode())
      .sameElements(b.aggregateExpressions.sortBy(_.hashCode())) &&
      a.groupByColumns.sortBy(_.hashCode()).sameElements(b.groupByColumns.sortBy(_.hashCode()))
  }

  /**
   * Convert the aggregates result from `InternalRow` to `ColumnarBatch`.
   * This is used for columnar reader.
   */
  def convertAggregatesRowToBatch(
      aggregatesAsRow: InternalRow,
      aggregatesSchema: StructType,
      offHeap: Boolean): ColumnarBatch = {
    val converter = new RowToColumnConverter(aggregatesSchema)
    val columnVectors = if (offHeap) {
      OffHeapColumnVector.allocateColumns(1, aggregatesSchema)
    } else {
      OnHeapColumnVector.allocateColumns(1, aggregatesSchema)
    }
    converter.convert(aggregatesAsRow, columnVectors.toArray)
    new ColumnarBatch(columnVectors.asInstanceOf[Array[ColumnVector]], 1)
  }
}
