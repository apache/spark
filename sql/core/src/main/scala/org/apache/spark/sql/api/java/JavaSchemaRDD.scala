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

package org.apache.spark.sql.api.java

import java.util.{List => JList}

import org.apache.spark.Partitioner
import org.apache.spark.api.java.{JavaRDDLike, JavaRDD}
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.sql.types.util.DataTypeConversions
import org.apache.spark.sql.{SQLContext, SchemaRDD, SchemaRDDLike}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import DataTypeConversions._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * An RDD of [[Row]] objects that is returned as the result of a Spark SQL query.  In addition to
 * standard RDD operations, a JavaSchemaRDD can also be registered as a table in the JavaSQLContext
 * that was used to create. Registering a JavaSchemaRDD allows its contents to be queried in
 * future SQL statement.
 *
 * @groupname schema SchemaRDD Functions
 * @groupprio schema -1
 * @groupname Ungrouped Base RDD Functions
 */
class JavaSchemaRDD(
     @transient val sqlContext: SQLContext,
     @transient val baseLogicalPlan: LogicalPlan)
  extends JavaRDDLike[Row, JavaRDD[Row]]
  with SchemaRDDLike {

  private[sql] val baseSchemaRDD = new SchemaRDD(sqlContext, logicalPlan)

  override val classTag = scala.reflect.classTag[Row]

  override def wrapRDD(rdd: RDD[Row]): JavaRDD[Row] = JavaRDD.fromRDD(rdd)

  val rdd = baseSchemaRDD.map(new Row(_))

  override def toString: String = baseSchemaRDD.toString

  /** Returns the schema of this JavaSchemaRDD (represented by a StructType). */
  def schema: StructType =
    asJavaDataType(baseSchemaRDD.schema).asInstanceOf[StructType]

  // =======================================================================
  // Base RDD functions that do NOT change schema
  // =======================================================================

  // Common RDD functions

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  def cache(): JavaSchemaRDD = {
    baseSchemaRDD.cache()
    this
  }

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  def persist(): JavaSchemaRDD = {
    baseSchemaRDD.persist()
    this
  }

  /**
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. This can only be used to assign a new storage level if the RDD does not
   * have a storage level set yet..
   */
  def persist(newLevel: StorageLevel): JavaSchemaRDD = {
    baseSchemaRDD.persist(newLevel)
    this
  }

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
   *
   * @param blocking Whether to block until all blocks are deleted.
   * @return This RDD.
   */
  def unpersist(blocking: Boolean = true): JavaSchemaRDD = {
    baseSchemaRDD.unpersist(blocking)
    this
  }

  /** Assign a name to this RDD */
  def setName(name: String): JavaSchemaRDD = {
    baseSchemaRDD.setName(name)
    this
  }

  // Overridden actions from JavaRDDLike.

  override def collect(): JList[Row] = {
    import scala.collection.JavaConversions._
    val arr: java.util.Collection[Row] = baseSchemaRDD.collect().toSeq.map(new Row(_))
    new java.util.ArrayList(arr)
  }

  override def take(num: Int): JList[Row] = {
    import scala.collection.JavaConversions._
    val arr: java.util.Collection[Row] = baseSchemaRDD.take(num).toSeq.map(new Row(_))
    new java.util.ArrayList(arr)
  }

  // Transformations (return a new RDD)

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
   */
  def coalesce(numPartitions: Int, shuffle: Boolean = false): JavaSchemaRDD =
    baseSchemaRDD.coalesce(numPartitions, shuffle).toJavaSchemaRDD

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(): JavaSchemaRDD =
    baseSchemaRDD.distinct().toJavaSchemaRDD

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(numPartitions: Int): JavaSchemaRDD =
    baseSchemaRDD.distinct(numPartitions).toJavaSchemaRDD

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   */
  def filter(f: JFunction[Row, java.lang.Boolean]): JavaSchemaRDD =
    baseSchemaRDD.filter(x => f.call(new Row(x)).booleanValue()).toJavaSchemaRDD

  /**
   * Return the intersection of this RDD and another one. The output will not contain any
   * duplicate elements, even if the input RDDs did.
   *
   * Note that this method performs a shuffle internally.
   */
  def intersection(other: JavaSchemaRDD): JavaSchemaRDD =
    this.baseSchemaRDD.intersection(other.baseSchemaRDD).toJavaSchemaRDD

  /**
   * Return the intersection of this RDD and another one. The output will not contain any
   * duplicate elements, even if the input RDDs did.
   *
   * Note that this method performs a shuffle internally.
   *
   * @param partitioner Partitioner to use for the resulting RDD
   */
  def intersection(other: JavaSchemaRDD, partitioner: Partitioner): JavaSchemaRDD =
    this.baseSchemaRDD.intersection(other.baseSchemaRDD, partitioner).toJavaSchemaRDD

  /**
   * Return the intersection of this RDD and another one. The output will not contain any
   * duplicate elements, even if the input RDDs did.  Performs a hash partition across the cluster
   *
   * Note that this method performs a shuffle internally.
   *
   * @param numPartitions How many partitions to use in the resulting RDD
   */
  def intersection(other: JavaSchemaRDD, numPartitions: Int): JavaSchemaRDD =
    this.baseSchemaRDD.intersection(other.baseSchemaRDD, numPartitions).toJavaSchemaRDD

  /**
   * Return a new RDD that has exactly `numPartitions` partitions.
   *
   * Can increase or decrease the level of parallelism in this RDD. Internally, this uses
   * a shuffle to redistribute data.
   *
   * If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
   * which can avoid performing a shuffle.
   */
  def repartition(numPartitions: Int): JavaSchemaRDD =
    baseSchemaRDD.repartition(numPartitions).toJavaSchemaRDD

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   *
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be <= us.
   */
  def subtract(other: JavaSchemaRDD): JavaSchemaRDD =
    this.baseSchemaRDD.subtract(other.baseSchemaRDD).toJavaSchemaRDD

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: JavaSchemaRDD, numPartitions: Int): JavaSchemaRDD =
    this.baseSchemaRDD.subtract(other.baseSchemaRDD, numPartitions).toJavaSchemaRDD

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: JavaSchemaRDD, p: Partitioner): JavaSchemaRDD =
    this.baseSchemaRDD.subtract(other.baseSchemaRDD, p).toJavaSchemaRDD
}
