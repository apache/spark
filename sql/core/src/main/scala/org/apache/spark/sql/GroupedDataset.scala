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

package org.apache.spark.sql

import java.util.{Iterator => JIterator}
import scala.collection.JavaConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.function.{Function2 => JFunction2, Function3 => JFunction3, _}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, encoderFor, Encoder}
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, Alias, Attribute}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution

/**
 * :: Experimental ::
 * A [[Dataset]] has been logically grouped by a user specified grouping key.  Users should not
 * construct a [[GroupedDataset]] directly, but should instead call `groupBy` on an existing
 * [[Dataset]].
 *
 * COMPATIBILITY NOTE: Long term we plan to make [[GroupedDataset)]] extend `GroupedData`.  However,
 * making this change to the class hierarchy would break some function signatures. As such, this
 * class should be considered a preview of the final API.  Changes will be made to the interface
 * after Spark 1.6.
 */
@Experimental
class GroupedDataset[K, T] private[sql](
    private val kEncoder: Encoder[K],
    private val tEncoder: Encoder[T],
    queryExecution: QueryExecution,
    private val dataAttributes: Seq[Attribute],
    private val groupingAttributes: Seq[Attribute]) extends Serializable {

  private implicit val kEnc = kEncoder match {
    case e: ExpressionEncoder[K] => e.unbind(groupingAttributes).resolve(groupingAttributes)
    case other =>
      throw new UnsupportedOperationException("Only expression encoders are currently supported")
  }

  private implicit val tEnc = tEncoder match {
    case e: ExpressionEncoder[T] => e.resolve(dataAttributes)
    case other =>
      throw new UnsupportedOperationException("Only expression encoders are currently supported")
  }

  /** Encoders for built in aggregations. */
  private implicit def newLongEncoder: Encoder[Long] = ExpressionEncoder[Long](flat = true)

  private def logicalPlan = queryExecution.analyzed
  private def sqlContext = queryExecution.sqlContext

  private def groupedData =
    new GroupedData(
      new DataFrame(sqlContext, logicalPlan), groupingAttributes, GroupedData.GroupByType)

  /**
   * Returns a new [[GroupedDataset]] where the type of the key has been mapped to the specified
   * type. The mapping of key columns to the type follows the same rules as `as` on [[Dataset]].
   */
  def asKey[L : Encoder]: GroupedDataset[L, T] =
    new GroupedDataset(
      encoderFor[L],
      tEncoder,
      queryExecution,
      dataAttributes,
      groupingAttributes)

  /**
   * Returns a [[Dataset]] that contains each unique key.
   */
  def keys: Dataset[K] = {
    new Dataset[K](
      sqlContext,
      Distinct(
        Project(groupingAttributes, logicalPlan)))
  }

  /**
   * Applies the given function to each group of data.  For each unique group, the function will
   * be passed the group key and an iterator that contains all of the elements in the group. The
   * function can return an iterator containing elements of an arbitrary type which will be returned
   * as a new [[Dataset]].
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory.  However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the memory
   * constraints of their cluster.
   */
  def flatMap[U : Encoder](f: (K, Iterator[T]) => TraversableOnce[U]): Dataset[U] = {
    new Dataset[U](
      sqlContext,
      MapGroups(f, groupingAttributes, logicalPlan))
  }

  def flatMap[U](
      f: JFunction2[K, JIterator[T], JIterator[U]],
      encoder: Encoder[U]): Dataset[U] = {
    flatMap((key, data) => f.call(key, data.asJava).asScala)(encoder)
  }

  /**
   * Applies the given function to each group of data.  For each unique group, the function will
   * be passed the group key and an iterator that contains all of the elements in the group. The
   * function can return an element of arbitrary type which will be returned as a new [[Dataset]].
   *
   * Internally, the implementation will spill to disk if any given group is too large to fit into
   * memory.  However, users must take care to avoid materializing the whole iterator for a group
   * (for example, by calling `toList`) unless they are sure that this is possible given the memory
   * constraints of their cluster.
   */
  def map[U : Encoder](f: (K, Iterator[T]) => U): Dataset[U] = {
    val func = (key: K, it: Iterator[T]) => Iterator(f(key, it))
    new Dataset[U](
      sqlContext,
      MapGroups(func, groupingAttributes, logicalPlan))
  }

  def map[U](
      f: JFunction2[K, JIterator[T], U],
      encoder: Encoder[U]): Dataset[U] = {
    map((key, data) => f.call(key, data.asJava))(encoder)
  }

  // To ensure valid overloading.
  protected def agg(expr: Column, exprs: Column*): DataFrame =
    groupedData.agg(expr, exprs: _*)

  /**
   * Internal helper function for building typed aggregations that return tuples.  For simplicity
   * and code reuse, we do this without the help of the type system and then use helper functions
   * that cast appropriately for the user facing interface.
   * TODO: does not handle aggrecations that return nonflat results,
   */
  protected def aggUntyped(columns: TypedColumn[_]*): Dataset[_] = {
    val aliases = (groupingAttributes ++ columns.map(_.expr)).map {
      case u: UnresolvedAttribute => UnresolvedAlias(u)
      case expr: NamedExpression => expr
      case expr: Expression => Alias(expr, expr.prettyString)()
    }

    val unresolvedPlan = Aggregate(groupingAttributes, aliases, logicalPlan)
    val execution = new QueryExecution(sqlContext, unresolvedPlan)

    val columnEncoders = columns.map(_.encoder.asInstanceOf[ExpressionEncoder[_]])

    // Rebind the encoders to the nested schema that will be produced by the aggregation.
    val encoders = (kEnc +: columnEncoders).zip(execution.analyzed.output).map {
      case (e: ExpressionEncoder[_], a) if !e.flat =>
        e.nested(a).resolve(execution.analyzed.output)
      case (e, a) =>
        e.unbind(a :: Nil).resolve(execution.analyzed.output)
    }
    new Dataset(sqlContext, execution, ExpressionEncoder.tuple(encoders))
  }

  /**
   * Computes the given aggregation, returning a [[Dataset]] of tuples for each unique key
   * and the result of computing this aggregation over all elements in the group.
   */
  def agg[A1](col1: TypedColumn[A1]): Dataset[(K, A1)] =
    aggUntyped(col1).asInstanceOf[Dataset[(K, A1)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key
   * and the result of computing these aggregations over all elements in the group.
   */
  def agg[A1, A2](col1: TypedColumn[A1], col2: TypedColumn[A2]): Dataset[(K, A1, A2)] =
    aggUntyped(col1, col2).asInstanceOf[Dataset[(K, A1, A2)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key
   * and the result of computing these aggregations over all elements in the group.
   */
  def agg[A1, A2, A3](
      col1: TypedColumn[A1],
      col2: TypedColumn[A2],
      col3: TypedColumn[A3]): Dataset[(K, A1, A2, A3)] =
    aggUntyped(col1, col2, col3).asInstanceOf[Dataset[(K, A1, A2, A3)]]

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples for each unique key
   * and the result of computing these aggregations over all elements in the group.
   */
  def agg[A1, A2, A3, A4](
      col1: TypedColumn[A1],
      col2: TypedColumn[A2],
      col3: TypedColumn[A3],
      col4: TypedColumn[A4]): Dataset[(K, A1, A2, A3, A4)] =
    aggUntyped(col1, col2, col3, col4).asInstanceOf[Dataset[(K, A1, A2, A3, A4)]]

  /**
   * Returns a [[Dataset]] that contains a tuple with each key and the number of items present
   * for that key.
   */
  def count(): Dataset[(K, Long)] = agg(functions.count("*").as[Long])

  /**
   * Applies the given function to each cogrouped data.  For each unique group, the function will
   * be passed the grouping key and 2 iterators containing all elements in the group from
   * [[Dataset]] `this` and `other`.  The function can return an iterator containing elements of an
   * arbitrary type which will be returned as a new [[Dataset]].
   */
  def cogroup[U, R : Encoder](
      other: GroupedDataset[K, U])(
      f: (K, Iterator[T], Iterator[U]) => Iterator[R]): Dataset[R] = {
    implicit def uEnc: Encoder[U] = other.tEncoder
    new Dataset[R](
      sqlContext,
      CoGroup(
        f,
        this.groupingAttributes,
        other.groupingAttributes,
        this.logicalPlan,
        other.logicalPlan))
  }

  def cogroup[U, R](
      other: GroupedDataset[K, U],
      f: JFunction3[K, JIterator[T], JIterator[U], JIterator[R]],
      encoder: Encoder[R]): Dataset[R] = {
    cogroup(other)((key, left, right) => f.call(key, left.asJava, right.asJava).asScala)(encoder)
  }
}
