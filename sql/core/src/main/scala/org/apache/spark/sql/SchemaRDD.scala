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

import java.util.{Map => JMap, List => JList}
import java.io.StringWriter

import scala.collection.JavaConversions._

import com.fasterxml.jackson.core.JsonFactory

import net.razorvine.pickle.Pickler

import org.apache.spark.{Dependency, OneToOneDependency, Partition, Partitioner, TaskContext}
import org.apache.spark.annotation.{AlphaComponent, Experimental}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.api.java.JavaSchemaRDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.json.JsonRDD
import org.apache.spark.sql.execution.{LogicalRDD, EvaluatePython}
import org.apache.spark.storage.StorageLevel

/**
 * :: AlphaComponent ::
 * An RDD of [[Row]] objects that has an associated schema. In addition to standard RDD functions,
 * SchemaRDDs can be used in relational queries, as shown in the examples below.
 *
 * Importing a SQLContext brings an implicit into scope that automatically converts a standard RDD
 * whose elements are scala case classes into a SchemaRDD.  This conversion can also be done
 * explicitly using the `createSchemaRDD` function on a [[SQLContext]].
 *
 * A `SchemaRDD` can also be created by loading data in from external sources.
 * Examples are loading data from Parquet files by using the `parquetFile` method on [[SQLContext]]
 * and loading JSON datasets by using `jsonFile` and `jsonRDD` methods on [[SQLContext]].
 *
 * == SQL Queries ==
 * A SchemaRDD can be registered as a table in the [[SQLContext]] that was used to create it.  Once
 * an RDD has been registered as a table, it can be used in the FROM clause of SQL statements.
 *
 * {{{
 *  // One method for defining the schema of an RDD is to make a case class with the desired column
 *  // names and types.
 *  case class Record(key: Int, value: String)
 *
 *  val sc: SparkContext // An existing spark context.
 *  val sqlContext = new SQLContext(sc)
 *
 *  // Importing the SQL context gives access to all the SQL functions and implicit conversions.
 *  import sqlContext._
 *
 *  val rdd = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i")))
 *  // Any RDD containing case classes can be registered as a table.  The schema of the table is
 *  // automatically inferred using scala reflection.
 *  rdd.registerTempTable("records")
 *
 *  val results: SchemaRDD = sql("SELECT * FROM records")
 * }}}
 *
 * == Language Integrated Queries ==
 *
 * {{{
 *
 *  case class Record(key: Int, value: String)
 *
 *  val sc: SparkContext // An existing spark context.
 *  val sqlContext = new SQLContext(sc)
 *
 *  // Importing the SQL context gives access to all the SQL functions and implicit conversions.
 *  import sqlContext._
 *
 *  val rdd = sc.parallelize((1 to 100).map(i => Record(i, "val_" + i)))
 *
 *  // Example of language integrated queries.
 *  rdd.where('key === 1).orderBy('value.asc).select('key).collect()
 * }}}
 *
 *  @groupname Query Language Integrated Queries
 *  @groupdesc Query Functions that create new queries from SchemaRDDs.  The
 *             result of all query functions is also a SchemaRDD, allowing multiple operations to be
 *             chained using a builder pattern.
 *  @groupprio Query -2
 *  @groupname schema SchemaRDD Functions
 *  @groupprio schema -1
 *  @groupname Ungrouped Base RDD Functions
 */
@AlphaComponent
class SchemaRDD(
    @transient val sqlContext: SQLContext,
    @transient val baseLogicalPlan: LogicalPlan)
  extends RDD[Row](sqlContext.sparkContext, Nil) with SchemaRDDLike {

  def baseSchemaRDD = this

  // =========================================================================================
  // RDD functions: Copy the internal row representation so we present immutable data to users.
  // =========================================================================================

  override def compute(split: Partition, context: TaskContext): Iterator[Row] =
    firstParent[Row].compute(split, context).map(ScalaReflection.convertRowToScala(_, this.schema))

  override def getPartitions: Array[Partition] = firstParent[Row].partitions

  override protected def getDependencies: Seq[Dependency[_]] = {
    schema // Force reification of the schema so it is available on executors.

    List(new OneToOneDependency(queryExecution.toRdd))
  }

  /**
   * Returns the schema of this SchemaRDD (represented by a [[StructType]]).
   *
   * @group schema
   */
  lazy val schema: StructType = queryExecution.analyzed.schema

  /**
   * Returns a new RDD with each row transformed to a JSON string.
   *
   * @group schema
   */
  def toJSON: RDD[String] = {
    val rowSchema = this.schema
    this.mapPartitions { iter =>
      val jsonFactory = new JsonFactory()
      iter.map(JsonRDD.rowToJSON(rowSchema, jsonFactory))
    }
  }


  // =======================================================================
  // Query DSL
  // =======================================================================

  /**
   * Changes the output of this relation to the given expressions, similar to the `SELECT` clause
   * in SQL.
   *
   * {{{
   *   schemaRDD.select('a, 'b + 'c, 'd as 'aliasedName)
   * }}}
   *
   * @param exprs a set of logical expression that will be evaluated for each input row.
   *
   * @group Query
   */
  def select(exprs: Expression*): SchemaRDD = {
    val aliases = exprs.zipWithIndex.map {
      case (ne: NamedExpression, _) => ne
      case (e, i) => Alias(e, s"c$i")()
    }
    new SchemaRDD(sqlContext, Project(aliases, logicalPlan))
  }

  /**
   * Filters the output, only returning those rows where `condition` evaluates to true.
   *
   * {{{
   *   schemaRDD.where('a === 'b)
   *   schemaRDD.where('a === 1)
   *   schemaRDD.where('a + 'b > 10)
   * }}}
   *
   * @group Query
   */
  def where(condition: Expression): SchemaRDD =
    new SchemaRDD(sqlContext, Filter(condition, logicalPlan))

  /**
   * Performs a relational join on two SchemaRDDs
   *
   * @param otherPlan the [[SchemaRDD]] that should be joined with this one.
   * @param joinType One of `Inner`, `LeftOuter`, `RightOuter`, or `FullOuter`. Defaults to `Inner.`
   * @param on       An optional condition for the join operation.  This is equivalent to the `ON`
   *                 clause in standard SQL.  In the case of `Inner` joins, specifying a
   *                 `condition` is equivalent to adding `where` clauses after the `join`.
   *
   * @group Query
   */
  def join(
      otherPlan: SchemaRDD,
      joinType: JoinType = Inner,
      on: Option[Expression] = None): SchemaRDD =
    new SchemaRDD(sqlContext, Join(logicalPlan, otherPlan.logicalPlan, joinType, on))

  /**
   * Sorts the results by the given expressions.
   * {{{
   *   schemaRDD.orderBy('a)
   *   schemaRDD.orderBy('a, 'b)
   *   schemaRDD.orderBy('a.asc, 'b.desc)
   * }}}
   *
   * @group Query
   */
  def orderBy(sortExprs: SortOrder*): SchemaRDD =
    new SchemaRDD(sqlContext, Sort(sortExprs, logicalPlan))

  @deprecated("use limit with integer argument", "1.1.0")
  def limit(limitExpr: Expression): SchemaRDD =
    new SchemaRDD(sqlContext, Limit(limitExpr, logicalPlan))

  /**
   * Limits the results by the given integer.
   * {{{
   *   schemaRDD.limit(10)
   * }}}
   */
  def limit(limitNum: Int): SchemaRDD =
    new SchemaRDD(sqlContext, Limit(Literal(limitNum), logicalPlan))

  /**
   * Performs a grouping followed by an aggregation.
   *
   * {{{
   *   schemaRDD.groupBy('year)(Sum('sales) as 'totalSales)
   * }}}
   *
   * @group Query
   */
  def groupBy(groupingExprs: Expression*)(aggregateExprs: Expression*): SchemaRDD = {
    val aliasedExprs = aggregateExprs.map {
      case ne: NamedExpression => ne
      case e => Alias(e, e.toString)()
    }
    new SchemaRDD(sqlContext, Aggregate(groupingExprs, aliasedExprs, logicalPlan))
  }

  /**
   * Performs an aggregation over all Rows in this RDD.
   * This is equivalent to a groupBy with no grouping expressions.
   *
   * {{{
   *   schemaRDD.aggregate(Sum('sales) as 'totalSales)
   * }}}
   *
   * @group Query
   */
  def aggregate(aggregateExprs: Expression*): SchemaRDD = {
    groupBy()(aggregateExprs: _*)
  }

  /**
   * Applies a qualifier to the attributes of this relation.  Can be used to disambiguate attributes
   * with the same name, for example, when performing self-joins.
   *
   * {{{
   *   val x = schemaRDD.where('a === 1).as('x)
   *   val y = schemaRDD.where('a === 2).as('y)
   *   x.join(y).where("x.a".attr === "y.a".attr),
   * }}}
   *
   * @group Query
   */
  def as(alias: Symbol) =
    new SchemaRDD(sqlContext, Subquery(alias.name, logicalPlan))

  /**
   * Combines the tuples of two RDDs with the same schema, keeping duplicates.
   *
   * @group Query
   */
  def unionAll(otherPlan: SchemaRDD) =
    new SchemaRDD(sqlContext, Union(logicalPlan, otherPlan.logicalPlan))

  /**
   * Performs a relational except on two SchemaRDDs
   *
   * @param otherPlan the [[SchemaRDD]] that should be excepted from this one.
   *
   * @group Query
   */
  def except(otherPlan: SchemaRDD): SchemaRDD =
    new SchemaRDD(sqlContext, Except(logicalPlan, otherPlan.logicalPlan))

  /**
   * Performs a relational intersect on two SchemaRDDs
   *
   * @param otherPlan the [[SchemaRDD]] that should be intersected with this one.
   *
   * @group Query
   */
  def intersect(otherPlan: SchemaRDD): SchemaRDD =
    new SchemaRDD(sqlContext, Intersect(logicalPlan, otherPlan.logicalPlan))

  /**
   * Filters tuples using a function over the value of the specified column.
   *
   * {{{
   *   schemaRDD.where('a)((a: Int) => ...)
   * }}}
   *
   * @group Query
   */
  def where[T1](arg1: Symbol)(udf: (T1) => Boolean) =
    new SchemaRDD(
      sqlContext,
      Filter(ScalaUdf(udf, BooleanType, Seq(UnresolvedAttribute(arg1.name))), logicalPlan))

  /**
   * :: Experimental ::
   * Filters tuples using a function over a `Dynamic` version of a given Row.  DynamicRows use
   * scala's Dynamic trait to emulate an ORM of in a dynamically typed language.  Since the type of
   * the column is not known at compile time, all attributes are converted to strings before
   * being passed to the function.
   *
   * {{{
   *   schemaRDD.where(r => r.firstName == "Bob" && r.lastName == "Smith")
   * }}}
   *
   * @group Query
   */
  @Experimental
  def where(dynamicUdf: (DynamicRow) => Boolean) =
    new SchemaRDD(
      sqlContext,
      Filter(ScalaUdf(dynamicUdf, BooleanType, Seq(WrapDynamic(logicalPlan.output))), logicalPlan))

  /**
   * :: Experimental ::
   * Returns a sampled version of the underlying dataset.
   *
   * @group Query
   */
  @Experimental
  override
  def sample(
      withReplacement: Boolean = true,
      fraction: Double,
      seed: Long) =
    new SchemaRDD(sqlContext, Sample(fraction, withReplacement, seed, logicalPlan))

  /**
   * :: Experimental ::
   * Return the number of elements in the RDD. Unlike the base RDD implementation of count, this
   * implementation leverages the query optimizer to compute the count on the SchemaRDD, which
   * supports features such as filter pushdown.
   */
  @Experimental
  override def count(): Long = aggregate(Count(Literal(1))).collect().head.getLong(0)

  /**
   * :: Experimental ::
   * Applies the given Generator, or table generating function, to this relation.
   *
   * @param generator A table generating function.  The API for such functions is likely to change
   *                  in future releases
   * @param join when set to true, each output row of the generator is joined with the input row
   *             that produced it.
   * @param outer when set to true, at least one row will be produced for each input row, similar to
   *              an `OUTER JOIN` in SQL.  When no output rows are produced by the generator for a
   *              given row, a single row will be output, with `NULL` values for each of the
   *              generated columns.
   * @param alias an optional alias that can be used as qualifier for the attributes that are
   *              produced by this generate operation.
   *
   * @group Query
   */
  @Experimental
  def generate(
      generator: Generator,
      join: Boolean = false,
      outer: Boolean = false,
      alias: Option[String] = None) =
    new SchemaRDD(sqlContext, Generate(generator, join, outer, alias, logicalPlan))

  /**
   * Returns this RDD as a SchemaRDD.  Intended primarily to force the invocation of the implicit
   * conversion from a standard RDD to a SchemaRDD.
   *
   * @group schema
   */
  def toSchemaRDD = this

  /**
   * Returns this RDD as a JavaSchemaRDD.
   *
   * @group schema
   */
  def toJavaSchemaRDD: JavaSchemaRDD = new JavaSchemaRDD(sqlContext, logicalPlan)

  /**
   * Converts a JavaRDD to a PythonRDD. It is used by pyspark.
   */
  private[sql] def javaToPython: JavaRDD[Array[Byte]] = {
    val fieldTypes = schema.fields.map(_.dataType)
    val jrdd = this.map(EvaluatePython.rowToArray(_, fieldTypes)).toJavaRDD()
    SerDeUtil.javaToPython(jrdd)
  }

  /**
   * Serializes the Array[Row] returned by SchemaRDD's optimized collect(), using the same
   * format as javaToPython. It is used by pyspark.
   */
  private[sql] def collectToPython: JList[Array[Byte]] = {
    val fieldTypes = schema.fields.map(_.dataType)
    val pickle = new Pickler
    new java.util.ArrayList(collect().map { row =>
      EvaluatePython.rowToArray(row, fieldTypes)
    }.grouped(100).map(batched => pickle.dumps(batched.toArray)).toIterable)
  }

  /**
   * Creates SchemaRDD by applying own schema to derived RDD. Typically used to wrap return value
   * of base RDD functions that do not change schema.
   *
   * @param rdd RDD derived from this one and has same schema
   *
   * @group schema
   */
  private def applySchema(rdd: RDD[Row]): SchemaRDD = {
    new SchemaRDD(sqlContext,
      LogicalRDD(queryExecution.analyzed.output.map(_.newInstance()), rdd)(sqlContext))
  }

  // =======================================================================
  // Overridden RDD actions
  // =======================================================================

  override def collect(): Array[Row] = queryExecution.executedPlan.executeCollect()

  override def take(num: Int): Array[Row] = limit(num).collect()

  // =======================================================================
  // Base RDD functions that do NOT change schema
  // =======================================================================

  // Transformations (return a new RDD)

  override def coalesce(numPartitions: Int, shuffle: Boolean = false)
                       (implicit ord: Ordering[Row] = null): SchemaRDD =
    applySchema(super.coalesce(numPartitions, shuffle)(ord))

  override def distinct(): SchemaRDD =
    applySchema(super.distinct())

  override def distinct(numPartitions: Int)
                       (implicit ord: Ordering[Row] = null): SchemaRDD =
    applySchema(super.distinct(numPartitions)(ord))

  override def filter(f: Row => Boolean): SchemaRDD =
    applySchema(super.filter(f))

  override def intersection(other: RDD[Row]): SchemaRDD =
    applySchema(super.intersection(other))

  override def intersection(other: RDD[Row], partitioner: Partitioner)
                           (implicit ord: Ordering[Row] = null): SchemaRDD =
    applySchema(super.intersection(other, partitioner)(ord))

  override def intersection(other: RDD[Row], numPartitions: Int): SchemaRDD =
    applySchema(super.intersection(other, numPartitions))

  override def repartition(numPartitions: Int)
                          (implicit ord: Ordering[Row] = null): SchemaRDD =
    applySchema(super.repartition(numPartitions)(ord))

  override def subtract(other: RDD[Row]): SchemaRDD =
    applySchema(super.subtract(other))

  override def subtract(other: RDD[Row], numPartitions: Int): SchemaRDD =
    applySchema(super.subtract(other, numPartitions))

  override def subtract(other: RDD[Row], p: Partitioner)
                       (implicit ord: Ordering[Row] = null): SchemaRDD =
    applySchema(super.subtract(other, p)(ord))

  /** Overridden cache function will always use the in-memory columnar caching. */
  override def cache(): this.type = {
    sqlContext.cacheQuery(this)
    this
  }

  override def persist(newLevel: StorageLevel): this.type = {
    sqlContext.cacheQuery(this, None, newLevel)
    this
  }

  override def unpersist(blocking: Boolean): this.type = {
    sqlContext.uncacheQuery(this, blocking)
    this
  }
}
