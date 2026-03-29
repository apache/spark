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
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, FieldReference}
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Aggregation, Count, CountStar, Max, Min}
import org.apache.spark.sql.execution.RowToColumnConverter
import org.apache.spark.sql.execution.datasources.v2.V2ColumnUtils
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

    def getStructFieldForCol(colName: String): StructField = {
      schema.apply(colName)
    }

    def isPartitionCol(colName: String) = {
      partitionNames.contains(colName)
    }

    def processMinOrMax(agg: AggregateFunc): Boolean = {
      val (columnName, aggType) = agg match {
        case max: Max if V2ColumnUtils.extractV2Column(max.column).isDefined =>
          (V2ColumnUtils.extractV2Column(max.column).get, "max")
        case min: Min if V2ColumnUtils.extractV2Column(min.column).isDefined =>
          (V2ColumnUtils.extractV2Column(min.column).get, "min")
        case _ => return false
      }

      if (isPartitionCol(columnName)) {
        // don't push down partition column, footer doesn't have max/min for partition column
        return false
      }
      val structField = getStructFieldForCol(columnName)

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

    if (dataFilters.nonEmpty) {
      // Parquet/ORC footer has max/min/count for columns
      // e.g. SELECT COUNT(col1) FROM t
      // but footer doesn't have max/min/count for a column if max/min/count
      // are combined with filter or group by
      // e.g. SELECT COUNT(col1) FROM t WHERE col2 = 8
      //      SELECT COUNT(col1) FROM t GROUP BY col2
      // However, if the filter is on partition column, max/min/count can still be pushed down
      return None
    }

    if (aggregation.groupByExpressions.nonEmpty &&
      partitionNames.size != aggregation.groupByExpressions.length) {
      // If there are group by columns, we only push down if the group by columns are the same as
      // the partition columns. In theory, if group by columns are a subset of partition columns,
      // we should still be able to push down. e.g. if table t has partition columns p1, p2, and p3,
      // SELECT MAX(c) FROM t GROUP BY p1, p2 should still be able to push down. However, the
      // partial aggregation pushed down to data source needs to be
      // SELECT p1, p2, p3, MAX(c) FROM t GROUP BY p1, p2, p3, and Spark layer
      // needs to have a final aggregation such as SELECT MAX(c) FROM t GROUP BY p1, p2, then the
      // pushed down query schema is different from the query schema at Spark. We will keep
      // aggregate push down simple and don't handle this complicate case for now.
      return None
    }
    aggregation.groupByExpressions.map(extractColName).foreach { colName =>
      // don't push down if the group by columns are not the same as the partition columns (orders
      // doesn't matter because reorder can be done at data source layer)
      if (colName.isEmpty || !isPartitionCol(colName.get)) return None
      finalSchema = finalSchema.add(getStructFieldForCol(colName.get))
    }

    aggregation.aggregateExpressions.foreach {
      case max: Max =>
        if (!processMinOrMax(max)) return None
      case min: Min =>
        if (!processMinOrMax(min)) return None
      case count: Count
        if V2ColumnUtils.extractV2Column(count.column).isDefined && !count.isDistinct =>
        val columnName = V2ColumnUtils.extractV2Column(count.column).get
        finalSchema = finalSchema.add(StructField(s"count($columnName)", LongType))
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
      a.groupByExpressions.sortBy(_.hashCode())
        .sameElements(b.groupByExpressions.sortBy(_.hashCode()))
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

  /**
   * Return the schema for aggregates only (exclude group by columns)
   */
  def getSchemaWithoutGroupingExpression(
      aggSchema: StructType,
      aggregation: Aggregation): StructType = {
    val numOfGroupByColumns = aggregation.groupByExpressions.length
    if (numOfGroupByColumns > 0) {
      new StructType(aggSchema.fields.drop(numOfGroupByColumns))
    } else {
      aggSchema
    }
  }

  /**
   * Reorder partition cols if they are not in the same order as group by columns
   */
  def reOrderPartitionCol(
      partitionSchema: StructType,
      aggregation: Aggregation,
      partitionValues: InternalRow): InternalRow = {
    val groupByColNames = aggregation.groupByExpressions.flatMap(extractColName)
    assert(groupByColNames.length == partitionSchema.length &&
      groupByColNames.length == partitionValues.numFields, "The number of group by columns " +
      s"${groupByColNames.length} should be the same as partition schema length " +
      s"${partitionSchema.length} and the number of fields ${partitionValues.numFields} " +
      s"in partitionValues")
    var reorderedPartColValues = Array.empty[Any]
    if (!partitionSchema.names.sameElements(groupByColNames)) {
      groupByColNames.foreach { col =>
        val index = partitionSchema.names.indexOf(col)
        val v = partitionValues.asInstanceOf[GenericInternalRow].values(index)
        reorderedPartColValues = reorderedPartColValues :+ v
      }
      new GenericInternalRow(reorderedPartColValues)
    } else {
      partitionValues
    }
  }

  private def extractColName(v2Expr: V2Expression): Option[String] = v2Expr match {
    case f: FieldReference if f.fieldNames.length == 1 => Some(f.fieldNames.head)
    case _ => None
  }
}
