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

/**
 * A filter predicate for data sources.
 */
abstract class Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * equal to `value`.
 */
case class EqualTo(attribute: String, value: Any) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * greater than `value`.
 */
case class GreaterThan(attribute: String, value: Any) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * greater than or equal to `value`.
 */
case class GreaterThanOrEqual(attribute: String, value: Any) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * less than `value`.
 */
case class LessThan(attribute: String, value: Any) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * less than or equal to `value`.
 */
case class LessThanOrEqual(attribute: String, value: Any) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to one of the values in the array.
 */
case class In(attribute: String, values: Array[Any]) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to null.
 */
case class IsNull(attribute: String) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a non-null value.
 */
case class IsNotNull(attribute: String) extends Filter

/**
 * A filter that evaluates to `true` iff both `left` or `right` evaluate to `true`.
 */
case class And(left: Filter, right: Filter) extends Filter

/**
 * A filter that evaluates to `true` iff at least one of `left` or `right` evaluates to `true`.
 */
case class Or(left: Filter, right: Filter) extends Filter

/**
 * A filter that evaluates to `true` iff `child` is evaluated to `false`.
 */
case class Not(child: Filter) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to
 * a string that starts with `value`.
 */
case class StringStartsWith(attribute: String, value: String) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to
 * a string that starts with `value`.
 */
case class StringEndsWith(attribute: String, value: String) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to
 * a string that contains the string `value`.
 */
case class StringContains(attribute: String, value: String) extends Filter
