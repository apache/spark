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

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.StructType

/**
 * A [[Dataset]] is a strongly typed collection of objects that can be transformed in parallel
 * using functional or relational operations.
 *
 * A [[Dataset]] differs from an [[RDD]] in the following ways:
 *  - Internally, a [[Dataset]] is represented by a Catalyst logical plan and the data is stored
 *    in the encoded form.  This representation allows for additional logical operations and
 *    enables many operations (sorting, shuffling, etc.) to be performed without deserializing to
 *    an object.
 *  - The creation of a [[Dataset]] requires the presence of an explicit [[Encoder]] that can be
 *    used to serialize the object into a binary format.  Encoders are also capable of mapping the
 *    schema of a given object to the Spark SQL type system.  In contrast, RDDs rely on runtime
 *    reflection based serialization. Operations that change the type of object stored in the
 *    dataset also need an encoder for the new type.
 *
 * A [[Dataset]] can be thought of as a specialized DataFrame, where the elements map to a specific
 * JVM object type, instead of to a generic [[Row]] container. A DataFrame can be transformed into
 * specific Dataset by calling `df.as[ElementType]`.  Similarly you can transform a strongly-typed
 * [[Dataset]] to a generic DataFrame by calling `ds.toDF()`.
 *
 * COMPATIBILITY NOTE: Long term we plan to make [[DataFrame]] extend `Dataset[Row]`.  However,
 * making this change to the class hierarchy would break the function signatures for the existing
 * functional operations (map, flatMap, etc).  As such, this class should be considered a preview
 * of the final API.  Changes will be made to the interface after Spark 1.6.
 *
 * @since 1.6.0
 */
@Experimental
class Dataset[T] private(
    @transient val sqlContext: SQLContext,
    @transient val queryExecution: QueryExecution,
    unresolvedEncoder: Encoder[T]) extends Serializable {

  /** The encoder for this [[Dataset]] that has been resolved to its output schema. */
  private[sql] implicit val encoder: ExpressionEncoder[T] = unresolvedEncoder match {
    case e: ExpressionEncoder[T] => e.resolve(queryExecution.analyzed.output)
    case _ => throw new IllegalArgumentException("Only expression encoders are currently supported")
  }

  private implicit def classTag = encoder.clsTag

  private[sql] def this(sqlContext: SQLContext, plan: LogicalPlan)(implicit encoder: Encoder[T]) =
    this(sqlContext, new QueryExecution(sqlContext, plan), encoder)

  /** Returns the schema of the encoded form of the objects in this [[Dataset]]. */
  def schema: StructType = encoder.schema

  /* ************* *
   *  Conversions  *
   * ************* */

  /**
   * Returns a new `Dataset` where each record has been mapped on to the specified type.
   * TODO: should bind here...
   * TODO: document binding rules
   * @since 1.6.0
   */
  def as[U : Encoder]: Dataset[U] = {
    new Dataset(sqlContext, queryExecution, encoderFor[U])
  }

  /**
   * Applies a logical alias to this [[Dataset]] that can be used to disambiguate columns that have
   * the same name after two Datasets have been joined.
   */
  def as(alias: String): Dataset[T] = withPlan(Subquery(alias, _))

  /**
   * Converts this strongly typed collection of data to generic Dataframe.  In contrast to the
   * strongly typed objects that Dataset operations work on, a Dataframe returns generic [[Row]]
   * objects that allow fields to be accessed by ordinal or name.
   */
  def toDF(): DataFrame = DataFrame(sqlContext, logicalPlan)


  /**
   * Returns this Dataset.
   * @since 1.6.0
   */
  def toDS(): Dataset[T] = this

  /**
   * Converts this Dataset to an RDD.
   * @since 1.6.0
   */
  def rdd: RDD[T] = {
    val tEnc = encoderFor[T]
    val input = queryExecution.analyzed.output
    queryExecution.toRdd.mapPartitions { iter =>
      val bound = tEnc.bind(input)
      iter.map(bound.fromRow)
    }
  }

  /* *********************** *
   *  Functional Operations  *
   * *********************** */

  /**
   * Concise syntax for chaining custom transformations.
   * {{{
   *   def featurize(ds: Dataset[T]) = ...
   *
   *   dataset
   *     .transform(featurize)
   *     .transform(...)
   * }}}
   *
   * @since 1.6.0
   */
  def transform[U](t: Dataset[T] => Dataset[U]): Dataset[U] = t(this)

  /**
   * Returns a new [[Dataset]] that only contains elements where `func` returns `true`.
   * @since 1.6.0
   */
  def filter(func: T => Boolean): Dataset[T] = mapPartitions(_.filter(func))

  /**
   * Returns a new [[Dataset]] that contains the result of applying `func` to each element.
   * @since 1.6.0
   */
  def map[U : Encoder](func: T => U): Dataset[U] = mapPartitions(_.map(func))

  /**
   * Returns a new [[Dataset]] that contains the result of applying `func` to each element.
   * @since 1.6.0
   */
  def mapPartitions[U : Encoder](func: Iterator[T] => Iterator[U]): Dataset[U] = {
    new Dataset(
      sqlContext,
      MapPartitions[T, U](
        func,
        encoderFor[T],
        encoderFor[U],
        encoderFor[U].schema.toAttributes,
        logicalPlan))
  }

  def flatMap[U : Encoder](func: T => TraversableOnce[U]): Dataset[U] =
    mapPartitions(_.flatMap(func))

  /* ************** *
   *  Side effects  *
   * ************** */

  /**
   * Runs `func` on each element of this Dataset.
   * @since 1.6.0
   */
  def foreach(func: T => Unit): Unit = rdd.foreach(func)

  /**
   * Runs `func` on each partition of this Dataset.
   * @since 1.6.0
   */
  def foreachPartition(func: Iterator[T] => Unit): Unit = rdd.foreachPartition(func)

  /* ************* *
   *  Aggregation  *
   * ************* */

  /**
   * Reduces the elements of this Dataset using the specified  binary function.  The given function
   * must be commutative and associative or the result may be non-deterministic.
   * @since 1.6.0
   */
  def reduce(func: (T, T) => T): T = rdd.reduce(func)

  /**
   * Aggregates the elements of each partition, and then the results for all the partitions, using a
   * given associative and commutative function and a neutral "zero value".
   *
   * This behaves somewhat differently than the fold operations implemented for non-distributed
   * collections in functional languages like Scala. This fold operation may be applied to
   * partitions individually, and then those results will be folded into the final result.
   * If op is not commutative, then the result may differ from that of a fold applied to a
   * non-distributed collection.
   * @since 1.6.0
   */
  def fold(zeroValue: T)(op: (T, T) => T): T = rdd.fold(zeroValue)(op)

  /**
   * Returns a [[GroupedDataset]] where the data is grouped by the given key function.
   * @since 1.6.0
   */
  def groupBy[K : Encoder](func: T => K): GroupedDataset[K, T] = {
    val inputPlan = queryExecution.analyzed
    val withGroupingKey = AppendColumn(func, inputPlan)
    val executed = sqlContext.executePlan(withGroupingKey)

    new GroupedDataset(
      encoderFor[K].resolve(withGroupingKey.newColumns),
      encoderFor[T].bind(inputPlan.output),
      executed,
      inputPlan.output,
      withGroupingKey.newColumns)
  }

  /* ****************** *
   *  Typed Relational  *
   * ****************** */

  /**
   * Selects a set of column based expressions.
   * {{{
   *   df.select($"colA", $"colB" + 1)
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  // Copied from Dataframe to make sure we don't have invalid overloads.
  @scala.annotation.varargs
  def select(cols: Column*): DataFrame = toDF().select(cols: _*)

  /**
   * Returns a new [[Dataset]] by computing the given [[Column]] expression for each element.
   *
   * {{{
   *   val ds = Seq(1, 2, 3).toDS()
   *   val newDS = ds.select(e[Int]("value + 1"))
   * }}}
   * @since 1.6.0
   */
  def select[U1: Encoder](c1: TypedColumn[U1]): Dataset[U1] = {
    new Dataset[U1](sqlContext, Project(Alias(c1.expr, "_1")() :: Nil, logicalPlan))
  }

  /**
   * Internal helper function for building typed selects that return tuples.  For simplicity and
   * code reuse, we do this without the help of the type system and then use helper functions
   * that cast appropriately for the user facing interface.
   */
  protected def selectUntyped(columns: TypedColumn[_]*): Dataset[_] = {
    val aliases = columns.zipWithIndex.map { case (c, i) => Alias(c.expr, s"_${i + 1}")() }
    val unresolvedPlan = Project(aliases, logicalPlan)
    val execution = new QueryExecution(sqlContext, unresolvedPlan)
    // Rebind the encoders to the nested schema that will be produced by the select.
    val encoders = columns.map(_.encoder.asInstanceOf[ExpressionEncoder[_]]).zip(aliases).map {
      case (e: ExpressionEncoder[_], a) if !e.flat =>
        e.nested(a.toAttribute).resolve(execution.analyzed.output)
      case (e, a) =>
        e.unbind(a.toAttribute :: Nil).resolve(execution.analyzed.output)
    }
    new Dataset(sqlContext, execution, ExpressionEncoder.tuple(encoders))
  }

  /**
   * Returns a new [[Dataset]] by computing the given [[Column]] expressions for each element.
   * @since 1.6.0
   */
  def select[U1, U2](c1: TypedColumn[U1], c2: TypedColumn[U2]): Dataset[(U1, U2)] =
    selectUntyped(c1, c2).asInstanceOf[Dataset[(U1, U2)]]

  /**
   * Returns a new [[Dataset]] by computing the given [[Column]] expressions for each element.
   * @since 1.6.0
   */
  def select[U1, U2, U3](
      c1: TypedColumn[U1],
      c2: TypedColumn[U2],
      c3: TypedColumn[U3]): Dataset[(U1, U2, U3)] =
    selectUntyped(c1, c2, c3).asInstanceOf[Dataset[(U1, U2, U3)]]

  /**
   * Returns a new [[Dataset]] by computing the given [[Column]] expressions for each element.
   * @since 1.6.0
   */
  def select[U1, U2, U3, U4](
      c1: TypedColumn[U1],
      c2: TypedColumn[U2],
      c3: TypedColumn[U3],
      c4: TypedColumn[U4]): Dataset[(U1, U2, U3, U4)] =
    selectUntyped(c1, c2, c3, c4).asInstanceOf[Dataset[(U1, U2, U3, U4)]]

  /**
   * Returns a new [[Dataset]] by computing the given [[Column]] expressions for each element.
   * @since 1.6.0
   */
  def select[U1, U2, U3, U4, U5](
      c1: TypedColumn[U1],
      c2: TypedColumn[U2],
      c3: TypedColumn[U3],
      c4: TypedColumn[U4],
      c5: TypedColumn[U5]): Dataset[(U1, U2, U3, U4, U5)] =
    selectUntyped(c1, c2, c3, c4, c5).asInstanceOf[Dataset[(U1, U2, U3, U4, U5)]]

  /* **************** *
   *  Set operations  *
   * **************** */

  /**
   * Returns a new [[Dataset]] that contains only the unique elements of this [[Dataset]].
   *
   * Note that, equality checking is performed directly on the encoded representation of the data
   * and thus is not affected by a custom `equals` function defined on `T`.
   * @since 1.6.0
   */
  def distinct: Dataset[T] = withPlan(Distinct)

  /**
   * Returns a new [[Dataset]] that contains only the elements of this [[Dataset]] that are also
   * present in `other`.
   *
   * Note that, equality checking is performed directly on the encoded representation of the data
   * and thus is not affected by a custom `equals` function defined on `T`.
   * @since 1.6.0
   */
  def intersect(other: Dataset[T]): Dataset[T] =
    withPlan[T](other)(Intersect)

  /**
   * Returns a new [[Dataset]] that contains the elements of both this and the `other` [[Dataset]]
   * combined.
   *
   * Note that, this function is not a typical set union operation, in that it does not eliminate
   * duplicate items.  As such, it is analagous to `UNION ALL` in SQL.
   * @since 1.6.0
   */
  def union(other: Dataset[T]): Dataset[T] =
    withPlan[T](other)(Union)

  /**
   * Returns a new [[Dataset]] where any elements present in `other` have been removed.
   *
   * Note that, equality checking is performed directly on the encoded representation of the data
   * and thus is not affected by a custom `equals` function defined on `T`.
   * @since 1.6.0
   */
  def subtract(other: Dataset[T]): Dataset[T] = withPlan[T](other)(Except)

  /* ****** *
   *  Joins *
   * ****** */

  /**
   * Joins this [[Dataset]] returning a [[Tuple2]] for each pair where `condition` evaluates to
   * true.
   *
   * This is similar to the relation `join` function with one important difference in the
   * result schema. Since `joinWith` preserves objects present on either side of the join, the
   * result schema is similarly nested into a tuple under the column names `_1` and `_2`.
   *
   * This type of join can be useful both for preserving type-safety with the original object
   * types as well as working with relational data where either side of the join has column
   * names in common.
   */
  def joinWith[U](other: Dataset[U], condition: Column): Dataset[(T, U)] = {
    val left = this.logicalPlan
    val right = other.logicalPlan

    val leftData = this.encoder match {
      case e if e.flat => Alias(left.output.head, "_1")()
      case _ => Alias(CreateStruct(left.output), "_1")()
    }
    val rightData = other.encoder match {
      case e if e.flat => Alias(right.output.head, "_2")()
      case _ => Alias(CreateStruct(right.output), "_2")()
    }
    val leftEncoder =
      if (encoder.flat) encoder else encoder.nested(leftData.toAttribute)
    val rightEncoder =
      if (other.encoder.flat) other.encoder else other.encoder.nested(rightData.toAttribute)
    implicit val tuple2Encoder: Encoder[(T, U)] =
      ExpressionEncoder.tuple(leftEncoder, rightEncoder)

    withPlan[(T, U)](other) { (left, right) =>
      Project(
        leftData :: rightData :: Nil,
        Join(left, right, Inner, Some(condition.expr)))
    }
  }

  /* ************************** *
   *  Gather to Driver Actions  *
   * ************************** */

  /** Returns the first element in this [[Dataset]]. */
  def first(): T = rdd.first()

  /** Collects the elements to an Array. */
  def collect(): Array[T] = rdd.collect()

  /** Returns the first `num` elements of this [[Dataset]] as an Array. */
  def take(num: Int): Array[T] = rdd.take(num)

  /* ******************** *
   *  Internal Functions  *
   * ******************** */

  private[sql] def logicalPlan = queryExecution.analyzed

  private[sql] def withPlan(f: LogicalPlan => LogicalPlan): Dataset[T] =
    new Dataset[T](sqlContext, sqlContext.executePlan(f(logicalPlan)), encoder)

  private[sql] def withPlan[R : Encoder](
      other: Dataset[_])(
      f: (LogicalPlan, LogicalPlan) => LogicalPlan): Dataset[R] =
    new Dataset[R](sqlContext, f(logicalPlan, other.logicalPlan))
}
