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

package org.apache.spark.sql.parquet

import parquet.filter._
import parquet.column.ColumnReader
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Equals
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.types.{IntegerType, BooleanType, NativeType}
import scala.reflect.runtime.universe.{typeTag, TypeTag}
import scala.reflect.ClassTag
import com.google.common.io.BaseEncoding
import parquet.filter
import parquet.filter.ColumnPredicates.BooleanPredicateFunction

// Implicits
import collection.JavaConversions._

object ParquetFilters {
  val PARQUET_FILTER_DATA = "org.apache.spark.sql.parquet.row.filter"

  def createFilter(filterExpressions: Seq[Expression]): UnboundRecordFilter = {
    def createEqualityFilter(name: String, literal: Literal) = literal.dataType match {
      case BooleanType => new ComparisonFilter(name, literal.value.asInstanceOf[Boolean])
      case IntegerType => new ComparisonFilter(name, _ == literal.value.asInstanceOf[Int])
    }

    val filters: Seq[UnboundRecordFilter] = filterExpressions.map {
      case Equals(left: Literal, right: NamedExpression) => {
        val name: String = right.name
        createEqualityFilter(name, left)
      }
      case Equals(left: NamedExpression, right: Literal) => {
        val name: String = left.name
        createEqualityFilter(name, right)
      }
    }

    if (filters.length > 0) filters.reduce(AndRecordFilter.and) else null
  }

  // Note: Inside the Hadoop API we only have access to `Configuration`, not to
  // [[SparkContext]], so we cannot use broadcasts to convey the actual filter
  // predicate.
  def serializeFilterExpressions(filters: Seq[Expression], conf: Configuration): Unit = {
    val serialized: Array[Byte] = SparkSqlSerializer.serialize(filters)
    val encoded: String = BaseEncoding.base64().encode(serialized)
    conf.set(PARQUET_FILTER_DATA, encoded)
  }

  // Note: Inside the Hadoop API we only have access to `Configuration`, not to
  // [[SparkContext]], so we cannot use broadcasts to convey the actual filter
  // predicate.
  def deserializeFilterExpressions(conf: Configuration): Option[Seq[Expression]] = {
    val data = conf.get(PARQUET_FILTER_DATA)
    if (data != null) {
      val decoded: Array[Byte] = BaseEncoding.base64().decode(data)
      Some(SparkSqlSerializer.deserialize(decoded))
    } else {
      None
    }
  }
}

class ComparisonFilter(
    private val columnName: String,
    private var filter: UnboundRecordFilter)
  extends UnboundRecordFilter {
  def this(columnName: String, value: Boolean) =
    this(
      columnName,
      ColumnRecordFilter.column(
        columnName,
        ColumnPredicates.applyFunctionToBoolean(
          new ColumnPredicates.BooleanPredicateFunction {
            def functionToApply(input: Boolean): Boolean = input == value
        })))
  def this(columnName: String, func: Int => Boolean) =
    this(
      columnName,
      ColumnRecordFilter.column(
        columnName,
        ColumnPredicates.applyFunctionToInteger(
          new ColumnPredicates.IntegerPredicateFunction {
            def functionToApply(input: Int) = if (input != null) func(input) else false
          })))
  override def bind(readers: java.lang.Iterable[ColumnReader]): RecordFilter = {
    filter.bind(readers)
  }
}

/*class EqualityFilter(
    private val columnName: String,
    private var filter: UnboundRecordFilter)
  extends UnboundRecordFilter {
  def this(columnName: String, value: Boolean) =
    this(columnName, ColumnRecordFilter.column(columnName, ColumnPredicates.equalTo(value)))
  def this(columnName: String, value: Int) =
    this(columnName, ColumnRecordFilter.column(columnName, ColumnPredicates.equalTo(value)))
  def this(columnName: String, value: Long) =
    this(columnName, ColumnRecordFilter.column(columnName, ColumnPredicates.equalTo(value)))
  def this(columnName: String, value: Double) =
    this(columnName, ColumnRecordFilter.column(columnName, ColumnPredicates.equalTo(value)))
  def this(columnName: String, value: Float) =
    this(columnName, ColumnRecordFilter.column(columnName, ColumnPredicates.equalTo(value)))
  def this(columnName: String, value: String) =
    this(columnName, ColumnRecordFilter.column(columnName, ColumnPredicates.equalTo(value)))
  override def bind(readers: java.lang.Iterable[ColumnReader]): RecordFilter = {
    filter.bind(readers)
  }
}*/

