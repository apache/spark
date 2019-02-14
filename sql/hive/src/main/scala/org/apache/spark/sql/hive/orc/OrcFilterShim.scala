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

package org.apache.spark.sql.hive.orc

import java.lang.{Long => JLong}
import java.lang.reflect.Method
import java.math.{BigDecimal => JBigDecimal}

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf.Type
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory.newBuilder

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.orc.OrcFilters.buildTree
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

private[orc] trait Shim {
  def createFilter(schema: StructType, filters: Array[Filter]): Option[SearchArgument]

  protected def findMethod(klass: Class[_], name: String, args: Class[_]*): Method = {
    val method = klass.getMethod(name, args: _*)
    method.setAccessible(true)
    method
  }
}

private[orc] object OrcFilterShim_v1 extends Shim with Logging {

  override def createFilter(schema: StructType, filters: Array[Filter]): Option[SearchArgument] = {
    val dataTypeMap = schema.map(f => f.name -> f.dataType).toMap

    // First, tries to convert each filter individually to see whether it's convertible, and then
    // collect all convertible ones to build the final `SearchArgument`.
    val convertibleFilters = for {
      filter <- filters
      _ <- buildSearchArgument(dataTypeMap, filter, newBuilder)
    } yield filter

    for {
      // Combines all convertible filters using `And` to produce a single conjunction
      conjunction <- buildTree(convertibleFilters)
      // Then tries to build a single ORC `SearchArgument` for the conjunction predicate
      builder <- buildSearchArgument(dataTypeMap, conjunction, newBuilder)
    } yield builder.build()
  }

  def buildSearchArgument(
      dataTypeMap: Map[String, DataType],
      expression: Filter,
      builder: Builder): Option[Builder] = {
    createBuilder(dataTypeMap, expression, builder, canPartialPushDownConjuncts = true)
  }

  /**
   * @param dataTypeMap a map from the attribute name to its data type.
   * @param expression the input filter predicates.
   * @param builder the input SearchArgument.Builder.
   * @param canPartialPushDownConjuncts whether a subset of conjuncts of predicates can be pushed
   *                                    down safely. Pushing ONLY one side of AND down is safe to
   *                                    do at the top level or none of its ancestors is NOT and OR.
   * @return the builder so far.
   */
  private def createBuilder(
      dataTypeMap: Map[String, DataType],
      expression: Filter,
      builder: Builder,
      canPartialPushDownConjuncts: Boolean): Option[Builder] = {
    def isSearchableType(dataType: DataType): Boolean = dataType match {
      // Only the values in the Spark types below can be recognized by
      // the `SearchArgumentImpl.BuilderImpl.boxLiteral()` method.
      case ByteType | ShortType | FloatType | DoubleType => true
      case IntegerType | LongType | StringType | BooleanType => true
      case TimestampType | _: DecimalType => true
      case _ => false
    }

    expression match {
      case And(left, right) =>
        // At here, it is not safe to just convert one side and remove the other side
        // if we do not understand what the parent filters are.
        //
        // Here is an example used to explain the reason.
        // Let's say we have NOT(a = 2 AND b in ('1')) and we do not understand how to
        // convert b in ('1'). If we only convert a = 2, we will end up with a filter
        // NOT(a = 2), which will generate wrong results.
        //
        // Pushing one side of AND down is only safe to do at the top level or in the child
        // AND before hitting NOT or OR conditions, and in this case, the unsupported predicate
        // can be safely removed.
        val leftBuilderOption =
          createBuilder(dataTypeMap, left, newBuilder, canPartialPushDownConjuncts)
        val rightBuilderOption =
          createBuilder(dataTypeMap, right, newBuilder, canPartialPushDownConjuncts)
        (leftBuilderOption, rightBuilderOption) match {
          case (Some(_), Some(_)) =>
            for {
              lhs <- createBuilder(dataTypeMap, left,
                builder.startAnd(), canPartialPushDownConjuncts)
              rhs <- createBuilder(dataTypeMap, right, lhs, canPartialPushDownConjuncts)
            } yield rhs.end()

          case (Some(_), None) if canPartialPushDownConjuncts =>
            createBuilder(dataTypeMap, left, builder, canPartialPushDownConjuncts)

          case (None, Some(_)) if canPartialPushDownConjuncts =>
            createBuilder(dataTypeMap, right, builder, canPartialPushDownConjuncts)

          case _ => None
        }

      case Or(left, right) =>
        for {
          _ <- createBuilder(dataTypeMap, left, newBuilder, canPartialPushDownConjuncts = false)
          _ <- createBuilder(dataTypeMap, right, newBuilder, canPartialPushDownConjuncts = false)
          lhs <- createBuilder(dataTypeMap, left,
            builder.startOr(), canPartialPushDownConjuncts = false)
          rhs <- createBuilder(dataTypeMap, right, lhs, canPartialPushDownConjuncts = false)
        } yield rhs.end()

      case Not(child) =>
        for {
          _ <- createBuilder(dataTypeMap, child, newBuilder, canPartialPushDownConjuncts = false)
          negate <- createBuilder(dataTypeMap,
            child, builder.startNot(), canPartialPushDownConjuncts = false)
        } yield negate.end()

      // NOTE: For all case branches dealing with leaf predicates below, the additional `startAnd()`
      // call is mandatory.  ORC `SearchArgument` builder requires that all leaf predicates must be
      // wrapped by a "parent" predicate (`And`, `Or`, or `Not`).

      case EqualTo(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val method = findMethod(bd.getClass, "equals", classOf[String], classOf[Object])
        Some(method.invoke(bd, attribute, value.asInstanceOf[Object])
          .asInstanceOf[SearchArgument.Builder].end())

      case EqualNullSafe(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val method = findMethod(bd.getClass, "nullSafeEquals", classOf[String], classOf[Object])
        Some(method.invoke(bd, attribute, value.asInstanceOf[Object])
          .asInstanceOf[SearchArgument.Builder].end())

      case LessThan(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val method = findMethod(bd.getClass, "lessThan", classOf[String], classOf[Object])
        Some(method.invoke(bd, attribute, value.asInstanceOf[Object])
          .asInstanceOf[SearchArgument.Builder].end())

      case LessThanOrEqual(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val method = findMethod(bd.getClass, "lessThanEquals", classOf[String], classOf[Object])
        Some(method.invoke(bd, attribute, value.asInstanceOf[Object])
          .asInstanceOf[SearchArgument.Builder].end())

      case GreaterThan(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startNot()
        val method = findMethod(bd.getClass, "lessThanEquals", classOf[String], classOf[Object])
        Some(method.invoke(bd, attribute, value.asInstanceOf[Object])
          .asInstanceOf[SearchArgument.Builder].end())

      case GreaterThanOrEqual(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startNot()
        val method = findMethod(bd.getClass, "lessThan", classOf[String], classOf[Object])
        Some(method.invoke(bd, attribute, value.asInstanceOf[Object])
          .asInstanceOf[SearchArgument.Builder].end())

      case IsNull(attribute) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val method = findMethod(bd.getClass, "isNull", classOf[String])
        Some(method.invoke(bd, attribute).asInstanceOf[Builder].end())

      case IsNotNull(attribute) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startNot()
        val method = findMethod(bd.getClass, "isNull", classOf[String])
        Some(method.invoke(bd, attribute).asInstanceOf[SearchArgument.Builder].end())

      case In(attribute, values) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val method = findMethod(bd.getClass, "in", classOf[String],
          Utils.classForName("[Ljava.lang.Object;"))
        Some(method.invoke(bd, attribute, values.map(_.asInstanceOf[AnyRef]))
          .asInstanceOf[SearchArgument.Builder].end())

      case _ => None
    }
  }
}

private[orc] object OrcFilterShim_v2 extends Shim with Logging {

  override def createFilter(schema: StructType, filters: Array[Filter]): Option[SearchArgument] = {
    val dataTypeMap = schema.map(f => f.name -> f.dataType).toMap

    // First, tries to convert each filter individually to see whether it's convertible, and then
    // collect all convertible ones to build the final `SearchArgument`.
    val convertibleFilters = for {
      filter <- filters
      _ <- buildSearchArgument(dataTypeMap, filter, newBuilder)
    } yield filter

    for {
      // Combines all convertible filters using `And` to produce a single conjunction
      conjunction <- buildTree(convertibleFilters)
      // Then tries to build a single ORC `SearchArgument` for the conjunction predicate
      builder <- buildSearchArgument(dataTypeMap, conjunction, newBuilder)
    } yield builder.build()
  }

  private def buildSearchArgument(
      dataTypeMap: Map[String, DataType],
      expression: Filter,
      builder: Builder): Option[Builder] = {
    createBuilder(dataTypeMap, expression, builder, canPartialPushDownConjuncts = true)
  }

  /**
   * Get PredicateLeafType which is corresponding to the given DataType.
   */
  private def getPredicateLeafType(dataType: DataType) = dataType match {
    case BooleanType => Type.BOOLEAN
    case ByteType | ShortType | IntegerType | LongType => Type.LONG
    case FloatType | DoubleType => Type.FLOAT
    case StringType => Type.STRING
    case DateType => Type.DATE
    case TimestampType => Type.TIMESTAMP
    case _: DecimalType => Type.DECIMAL
    case _ => throw new UnsupportedOperationException(s"DataType: ${dataType.catalogString}")
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
      val decimal = value.asInstanceOf[JBigDecimal]
      val longValue: JLong = decimal.longValue
      val precision: Integer = decimal.precision
      val scale: Integer = decimal.scale
      val clazz = Utils.classForName("org.apache.hadoop.hive.serde2.io.HiveDecimalWritable")
      val decimalWritable = clazz.getConstructor(JLong.TYPE).newInstance(longValue)
      val method = clazz.getMethod("mutateEnforcePrecisionScale", Integer.TYPE, Integer.TYPE)
      method.invoke(decimalWritable, precision, scale)
      decimalWritable
    case _ => value
  }

  /**
   * @param dataTypeMap a map from the attribute name to its data type.
   * @param expression the input filter predicates.
   * @param builder the input SearchArgument.Builder.
   * @param canPartialPushDownConjuncts whether a subset of conjuncts of predicates can be pushed
   *                                    down safely. Pushing ONLY one side of AND down is safe to
   *                                    do at the top level or none of its ancestors is NOT and OR.
   * @return the builder so far.
   */
  private def createBuilder(
      dataTypeMap: Map[String, DataType],
      expression: Filter,
      builder: Builder,
      canPartialPushDownConjuncts: Boolean): Option[Builder] = {
    def getType(attribute: String): Type =
      getPredicateLeafType(dataTypeMap(attribute))

    def isSearchableType(dataType: DataType): Boolean = dataType match {
      // Only the values in the Spark types below can be recognized by
      // the `SearchArgumentImpl.BuilderImpl.boxLiteral()` method.
      case ByteType | ShortType | FloatType | DoubleType => true
      case IntegerType | LongType | StringType | BooleanType => true
      case TimestampType | _: DecimalType => true
      case _ => false
    }

    expression match {
      case And(left, right) =>
        // At here, it is not safe to just convert one side and remove the other side
        // if we do not understand what the parent filters are.
        //
        // Here is an example used to explain the reason.
        // Let's say we have NOT(a = 2 AND b in ('1')) and we do not understand how to
        // convert b in ('1'). If we only convert a = 2, we will end up with a filter
        // NOT(a = 2), which will generate wrong results.
        //
        // Pushing one side of AND down is only safe to do at the top level or in the child
        // AND before hitting NOT or OR conditions, and in this case, the unsupported predicate
        // can be safely removed.
        val leftBuilderOption =
          createBuilder(dataTypeMap, left, newBuilder, canPartialPushDownConjuncts)
        val rightBuilderOption =
          createBuilder(dataTypeMap, right, newBuilder, canPartialPushDownConjuncts)
        (leftBuilderOption, rightBuilderOption) match {
          case (Some(_), Some(_)) =>
            for {
              lhs <- createBuilder(dataTypeMap, left,
                builder.startAnd(), canPartialPushDownConjuncts)
              rhs <- createBuilder(dataTypeMap, right, lhs, canPartialPushDownConjuncts)
            } yield rhs.end()

          case (Some(_), None) if canPartialPushDownConjuncts =>
            createBuilder(dataTypeMap, left, builder, canPartialPushDownConjuncts)

          case (None, Some(_)) if canPartialPushDownConjuncts =>
            createBuilder(dataTypeMap, right, builder, canPartialPushDownConjuncts)

          case _ => None
        }

      case Or(left, right) =>
        for {
          _ <- createBuilder(dataTypeMap, left, newBuilder, canPartialPushDownConjuncts = false)
          _ <- createBuilder(dataTypeMap, right, newBuilder, canPartialPushDownConjuncts = false)
          lhs <- createBuilder(dataTypeMap, left,
            builder.startOr(), canPartialPushDownConjuncts = false)
          rhs <- createBuilder(dataTypeMap, right, lhs, canPartialPushDownConjuncts = false)
        } yield rhs.end()

      case Not(child) =>
        for {
          _ <- createBuilder(dataTypeMap, child, newBuilder, canPartialPushDownConjuncts = false)
          negate <- createBuilder(dataTypeMap,
            child, builder.startNot(), canPartialPushDownConjuncts = false)
        } yield negate.end()

      // NOTE: For all case branches dealing with leaf predicates below, the additional `startAnd()`
      // call is mandatory.  ORC `SearchArgument` builder requires that all leaf predicates must be
      // wrapped by a "parent" predicate (`And`, `Or`, or `Not`).

      case EqualTo(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        val method =
          findMethod(bd.getClass, "equals", classOf[String], classOf[Type], classOf[Object])
        Some(method.invoke(bd, attribute, getType(attribute), castedValue.asInstanceOf[Object])
          .asInstanceOf[SearchArgument.Builder].end())

      case EqualNullSafe(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val method =
          findMethod(bd.getClass, "nullSafeEquals", classOf[String], classOf[Type], classOf[Object])
        val dataType = getPredicateLeafType(dataTypeMap(attribute))
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(method.invoke(bd, attribute, dataType, castedValue.asInstanceOf[Object])
          .asInstanceOf[SearchArgument.Builder].end())

      case LessThan(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val method =
          findMethod(bd.getClass, "lessThan", classOf[String], classOf[Type], classOf[Object])
        val dataType = getPredicateLeafType(dataTypeMap(attribute))
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(method.invoke(bd, attribute, dataType, castedValue.asInstanceOf[Object])
          .asInstanceOf[SearchArgument.Builder].end())

      case LessThanOrEqual(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val method =
          findMethod(bd.getClass, "lessThanEquals", classOf[String], classOf[Type], classOf[Object])
        val dataType = getPredicateLeafType(dataTypeMap(attribute))
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(method.invoke(bd, attribute, dataType, castedValue.asInstanceOf[Object])
          .asInstanceOf[SearchArgument.Builder].end())

      case GreaterThan(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startNot()
        val method =
          findMethod(bd.getClass, "lessThanEquals", classOf[String], classOf[Type], classOf[Object])
        val dataType = getPredicateLeafType(dataTypeMap(attribute))
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(method.invoke(bd, attribute, dataType, castedValue.asInstanceOf[Object])
          .asInstanceOf[SearchArgument.Builder].end())

      case GreaterThanOrEqual(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startNot()
        val method =
          findMethod(bd.getClass, "lessThan", classOf[String], classOf[Type], classOf[Object])
        val dataType = getPredicateLeafType(dataTypeMap(attribute))
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(method.invoke(bd, attribute, dataType, castedValue.asInstanceOf[Object])
          .asInstanceOf[SearchArgument.Builder].end())

      case IsNull(attribute) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val method = findMethod(bd.getClass, "isNull", classOf[String], classOf[Type])
        val dataType = getPredicateLeafType(dataTypeMap(attribute))
        Some(method.invoke(bd, attribute, dataType).asInstanceOf[SearchArgument.Builder].end())

      case IsNotNull(attribute) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startNot()
        val method = findMethod(bd.getClass, "isNull", classOf[String], classOf[Type])
        val dataType = getPredicateLeafType(dataTypeMap(attribute))
        Some(method.invoke(bd, attribute, dataType).asInstanceOf[SearchArgument.Builder].end())

      case In(attribute, values) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val method = findMethod(bd.getClass, "in",
          classOf[String], classOf[Type], Utils.classForName("[Ljava.lang.Object;"))
        val dataType = getPredicateLeafType(dataTypeMap(attribute))
        val castedValues = values.map(castLiteralValue(_, dataTypeMap(attribute)))
        Some(method.invoke(bd, attribute, dataType, castedValues.map(_.asInstanceOf[AnyRef]))
          .asInstanceOf[SearchArgument.Builder].end())

      case _ => None
    }
  }
}
