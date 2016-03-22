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

package org.apache.spark.sql.sources

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines all the filters that we can push down to the data sources.
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * A filter predicate for data sources.
 *
 * @since 1.3.0
 */
abstract class Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * equal to `value`.
 *
 * @since 1.3.0
 */
case class EqualTo(attribute: String, value: Any) extends Filter

/**
 * Performs equality comparison, similar to [[EqualTo]]. However, this differs from [[EqualTo]]
 * in that it returns `true` (rather than NULL) if both inputs are NULL, and `false`
 * (rather than NULL) if one of the input is NULL and the other is not NULL.
 *
 * @since 1.5.0
 */
case class EqualNullSafe(attribute: String, value: Any) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * greater than `value`.
 *
 * @since 1.3.0
 */
case class GreaterThan(attribute: String, value: Any) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * greater than or equal to `value`.
 *
 * @since 1.3.0
 */
case class GreaterThanOrEqual(attribute: String, value: Any) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * less than `value`.
 *
 * @since 1.3.0
 */
case class LessThan(attribute: String, value: Any) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * less than or equal to `value`.
 *
 * @since 1.3.0
 */
case class LessThanOrEqual(attribute: String, value: Any) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to one of the values in the array.
 *
 * @since 1.3.0
 */
case class In(attribute: String, values: Array[Any]) extends Filter {
  override def hashCode(): Int = {
    var h = attribute.hashCode
    values.foreach { v =>
      h *= 41
      h += v.hashCode()
    }
    h
  }
  override def equals(o: Any): Boolean = o match {
    case In(a, vs) =>
      a == attribute && vs.length == values.length && vs.zip(values).forall(x => x._1 == x._2)
    case _ => false
  }
  override def toString: String = {
    s"In($attribute, [${values.mkString(",")}]"
  }
}

/**
 * A filter that evaluates to `true` iff the attribute evaluates to null.
 *
 * @since 1.3.0
 */
case class IsNull(attribute: String) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a non-null value.
 *
 * @since 1.3.0
 */
case class IsNotNull(attribute: String) extends Filter

/**
 * A filter that evaluates to `true` iff both `left` or `right` evaluate to `true`.
 *
 * @since 1.3.0
 */
case class And(left: Filter, right: Filter) extends Filter

/**
 * A filter that evaluates to `true` iff at least one of `left` or `right` evaluates to `true`.
 *
 * @since 1.3.0
 */
case class Or(left: Filter, right: Filter) extends Filter

/**
 * A filter that evaluates to `true` iff `child` is evaluated to `false`.
 *
 * @since 1.3.0
 */
case class Not(child: Filter) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to
 * a string that starts with `value`.
 *
 * @since 1.3.1
 */
case class StringStartsWith(attribute: String, value: String) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to
 * a string that starts with `value`.
 *
 * @since 1.3.1
 */
case class StringEndsWith(attribute: String, value: String) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to
 * a string that contains the string `value`.
 *
 * @since 1.3.1
 */
case class StringContains(attribute: String, value: String) extends Filter
