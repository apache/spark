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

package org.apache.spark.sql.execution.datasources.orc

import org.apache.orc.storage.ql.io.sarg.{PredicateLeaf, SearchArgument, SearchArgumentFactory}
import org.apache.orc.storage.ql.io.sarg.SearchArgument.Builder
import org.apache.orc.storage.serde2.io.HiveDecimalWritable

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._

/**
 * Utility functions to convert Spark data source filters to ORC filters.
 */
private[orc] object OrcFilters {

  /**
   * Create ORC filter as a SearchArgument instance.
   */
  def createFilter(schema: StructType, filters: Seq[Filter]): Option[SearchArgument] = {
    val dataTypeMap = schema.map(f => f.name -> f.dataType).toMap

    // First, tries to convert each filter individually to see whether it's convertible, and then
    // collect all convertible ones to build the final `SearchArgument`.
    val convertibleFilters = for {
      filter <- filters
      _ <- buildSearchArgument(dataTypeMap, filter, SearchArgumentFactory.newBuilder())
    } yield filter

    for {
      conjunction <- convertibleFilters.reduceOption(org.apache.spark.sql.sources.And)
      builder <- buildSearchArgument(dataTypeMap, conjunction, SearchArgumentFactory.newBuilder())
    } yield builder.build()
  }

  /**
   * Return true if this is a searchable type in ORC.
   */
  private def isSearchableType(dataType: DataType) = dataType match {
    case ByteType | ShortType | FloatType | DoubleType => true
    case IntegerType | LongType | StringType | BooleanType => true
    case TimestampType | _: DecimalType => true
    case _ => false
  }

  /**
   * Get PredicateLeafType which is corresponding to the given DataType.
   */
  private def getPredicateLeafType(dataType: DataType) = dataType match {
    case BooleanType => PredicateLeaf.Type.BOOLEAN
    case ByteType | ShortType | IntegerType | LongType => PredicateLeaf.Type.LONG
    case FloatType | DoubleType => PredicateLeaf.Type.FLOAT
    case StringType => PredicateLeaf.Type.STRING
    case DateType => PredicateLeaf.Type.DATE
    case TimestampType => PredicateLeaf.Type.TIMESTAMP
    case _: DecimalType => PredicateLeaf.Type.DECIMAL
    case _ => throw new UnsupportedOperationException(s"DataType: $dataType")
  }

  /**
   * Cast literal values for filters.
   *
   * We need to cast to long because ORC raises exceptions
   * at 'checkLiteralType' of SearchArgumentImpl.java.
   */
  private def castLiteralValue(value: Any, dataType: DataType): Any = dataType match {
    case ByteType | ShortType | IntegerType | LongType =>
      value.asInstanceOf[Number].longValue
    case FloatType | DoubleType =>
      value.asInstanceOf[Number].doubleValue()
    case _: DecimalType =>
      val decimal = value.asInstanceOf[java.math.BigDecimal]
      val decimalWritable = new HiveDecimalWritable(decimal.longValue)
      decimalWritable.mutateEnforcePrecisionScale(decimal.precision, decimal.scale)
      decimalWritable
    case _ => value
  }

  /**
   * Build a SearchArgument and return the builder so far.
   */
  private def buildSearchArgument(
      dataTypeMap: Map[String, DataType],
      expression: Filter,
      builder: Builder): Option[Builder] = {
    def newBuilder = SearchArgumentFactory.newBuilder()

    def getType(attribute: String): PredicateLeaf.Type =
      getPredicateLeafType(dataTypeMap(attribute))

    import org.apache.spark.sql.sources._

    expression match {
      case And(left, right) =>
        // At here, it is not safe to just convert one side if we do not understand the
        // other side. Here is an example used to explain the reason.
        // Let's say we have NOT(a = 2 AND b in ('1')) and we do not understand how to
        // convert b in ('1'). If we only convert a = 2, we will end up with a filter
        // NOT(a = 2), which will generate wrong results.
        // Pushing one side of AND down is only safe to do at the top level.
        // You can see ParquetRelation's initializeLocalJobFunc method as an example.
        for {
          _ <- buildSearchArgument(dataTypeMap, left, newBuilder)
          _ <- buildSearchArgument(dataTypeMap, right, newBuilder)
          lhs <- buildSearchArgument(dataTypeMap, left, builder.startAnd())
          rhs <- buildSearchArgument(dataTypeMap, right, lhs)
        } yield rhs.end()

      case Or(left, right) =>
        for {
          _ <- buildSearchArgument(dataTypeMap, left, newBuilder)
          _ <- buildSearchArgument(dataTypeMap, right, newBuilder)
          lhs <- buildSearchArgument(dataTypeMap, left, builder.startOr())
          rhs <- buildSearchArgument(dataTypeMap, right, lhs)
        } yield rhs.end()

      case Not(child) =>
        for {
          _ <- buildSearchArgument(dataTypeMap, child, newBuilder)
          negate <- buildSearchArgument(dataTypeMap, child, builder.startNot())
        } yield negate.end()

      // NOTE: For all case branches dealing with leaf predicates below, the additional `startAnd()`
      // call is mandatory.  ORC `SearchArgument` builder requires that all leaf predicates must be
      // wrapped by a "parent" predicate (`And`, `Or`, or `Not`).

      case EqualTo(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startAnd().equals(attribute, getType(attribute), castedValue).end())

      case EqualNullSafe(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startAnd().nullSafeEquals(attribute, getType(attribute), castedValue).end())

      case LessThan(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startAnd().lessThan(attribute, getType(attribute), castedValue).end())

      case LessThanOrEqual(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startAnd().lessThanEquals(attribute, getType(attribute), castedValue).end())

      case GreaterThan(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startNot().lessThanEquals(attribute, getType(attribute), castedValue).end())

      case GreaterThanOrEqual(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startNot().lessThan(attribute, getType(attribute), castedValue).end())

      case IsNull(attribute) if isSearchableType(dataTypeMap(attribute)) =>
        Some(builder.startAnd().isNull(attribute, getType(attribute)).end())

      case IsNotNull(attribute) if isSearchableType(dataTypeMap(attribute)) =>
        Some(builder.startNot().isNull(attribute, getType(attribute)).end())

      case In(attribute, values) if isSearchableType(dataTypeMap(attribute)) =>
        val castedValues = values.map(v => castLiteralValue(v, dataTypeMap(attribute)))
        Some(builder.startAnd().in(attribute, getType(attribute),
          castedValues.map(_.asInstanceOf[AnyRef]): _*).end())

      case _ => None
    }
  }
}
