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
class Dataset[T] private[sql](
    @transient val sqlContext: SQLContext,
    @transient val queryExecution: QueryExecution)(
    implicit val encoder: Encoder[T]) extends Serializable {

  private implicit def classTag = encoder.clsTag

  private[sql] def this(sqlContext: SQLContext, plan: LogicalPlan)(implicit encoder: Encoder[T]) =
    this(sqlContext, new QueryExecution(sqlContext, plan))

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
  def as[U : Encoder]: Dataset[U] = new Dataset(sqlContext, queryExecution)(implicitly[Encoder[U]])

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
    val tEnc = implicitly[Encoder[T]]
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
        implicitly[Encoder[T]],
        implicitly[Encoder[U]],
        implicitly[Encoder[U]].schema.toAttributes,
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
      implicitly[Encoder[K]].bindOrdinals(withGroupingKey.newColumns),
      implicitly[Encoder[T]].bind(inputPlan.output),
      executed,
      inputPlan.output,
      withGroupingKey.newColumns)
  }

  /* ****************** *
   *  Typed Relational  *
   * ****************** */

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

  // Codegen
  // scalastyle:off

  /** sbt scalaShell; println(Seq(1).toDS().genSelect) */
  private def genSelect: String = {
    (2 to 5).map { n =>
      val types = (1 to n).map(i =>s"U$i").mkString(", ")
      val args = (1 to n).map(i => s"c$i: TypedColumn[U$i]").mkString(", ")
      val encoders = (1 to n).map(i => s"c$i.encoder").mkString(", ")
      val schema = (1 to n).map(i => s"""Alias(c$i.expr, "_$i")()""").mkString(" :: ")
      s"""
         |/**
         | * Returns a new [[Dataset]] by computing the given [[Column]] expressions for each element.
         | * @since 1.6.0
         | */
         |def select[$types]($args): Dataset[($types)] = {
         |  implicit val te = new Tuple${n}Encoder($encoders)
         |  new Dataset[($types)](sqlContext,
         |    Project(
         |      $schema :: Nil,
         |      logicalPlan))
         |}
         |
       """.stripMargin
    }.mkString("\n")
  }

  /**
   * Returns a new [[Dataset]] by computing the given [[Column]] expressions for each element.
   * @since 1.6.0
   */
  def select[U1, U2](c1: TypedColumn[U1], c2: TypedColumn[U2]): Dataset[(U1, U2)] = {
    implicit val te = new Tuple2Encoder(c1.encoder, c2.encoder)
    new Dataset[(U1, U2)](sqlContext,
      Project(
        Alias(c1.expr, "_1")() :: Alias(c2.expr, "_2")() :: Nil,
        logicalPlan))
  }



  /**
   * Returns a new [[Dataset]] by computing the given [[Column]] expressions for each element.
   * @since 1.6.0
   */
  def select[U1, U2, U3](c1: TypedColumn[U1], c2: TypedColumn[U2], c3: TypedColumn[U3]): Dataset[(U1, U2, U3)] = {
    implicit val te = new Tuple3Encoder(c1.encoder, c2.encoder, c3.encoder)
    new Dataset[(U1, U2, U3)](sqlContext,
      Project(
        Alias(c1.expr, "_1")() :: Alias(c2.expr, "_2")() :: Alias(c3.expr, "_3")() :: Nil,
        logicalPlan))
  }



  /**
   * Returns a new [[Dataset]] by computing the given [[Column]] expressions for each element.
   * @since 1.6.0
   */
  def select[U1, U2, U3, U4](c1: TypedColumn[U1], c2: TypedColumn[U2], c3: TypedColumn[U3], c4: TypedColumn[U4]): Dataset[(U1, U2, U3, U4)] = {
    implicit val te = new Tuple4Encoder(c1.encoder, c2.encoder, c3.encoder, c4.encoder)
    new Dataset[(U1, U2, U3, U4)](sqlContext,
      Project(
        Alias(c1.expr, "_1")() :: Alias(c2.expr, "_2")() :: Alias(c3.expr, "_3")() :: Alias(c4.expr, "_4")() :: Nil,
        logicalPlan))
  }



  /**
   * Returns a new [[Dataset]] by computing the given [[Column]] expressions for each element.
   * @since 1.6.0
   */
  def select[U1, U2, U3, U4, U5](c1: TypedColumn[U1], c2: TypedColumn[U2], c3: TypedColumn[U3], c4: TypedColumn[U4], c5: TypedColumn[U5]): Dataset[(U1, U2, U3, U4, U5)] = {
    implicit val te = new Tuple5Encoder(c1.encoder, c2.encoder, c3.encoder, c4.encoder, c5.encoder)
    new Dataset[(U1, U2, U3, U4, U5)](sqlContext,
      Project(
        Alias(c1.expr, "_1")() :: Alias(c2.expr, "_2")() :: Alias(c3.expr, "_3")() :: Alias(c4.expr, "_4")() :: Alias(c5.expr, "_5")() :: Nil,
        logicalPlan))
  }

  // scalastyle:on

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
    new Dataset[T](sqlContext, sqlContext.executePlan(f(logicalPlan)))

  private[sql] def withPlan[R : Encoder](
      other: Dataset[_])(
      f: (LogicalPlan, LogicalPlan) => LogicalPlan): Dataset[R] =
    new Dataset[R](
      sqlContext,
      sqlContext.executePlan(
        f(logicalPlan, other.logicalPlan)))
}
