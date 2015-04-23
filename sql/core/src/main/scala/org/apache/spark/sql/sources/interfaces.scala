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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

/**
 * ::DeveloperApi::
 * Implemented by objects that produce relations for a specific kind of data source.  When
 * Spark SQL is given a DDL operation with a USING clause specified (to specify the implemented
 * RelationProvider), this interface is used to pass in the parameters specified by a user.
 *
 * Users may specify the fully qualified class name of a given data source.  When that class is
 * not found Spark SQL will append the class name `DefaultSource` to the path, allowing for
 * less verbose invocation.  For example, 'org.apache.spark.sql.json' would resolve to the
 * data source 'org.apache.spark.sql.json.DefaultSource'
 *
 * A new instance of this class with be instantiated each time a DDL call is made.
 */
@DeveloperApi
trait RelationProvider {
  /**
   * Returns a new base relation with the given parameters.
   * Note: the parameters' keywords are case insensitive and this insensitivity is enforced
   * by the Map that is passed to the function.
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation
}

/**
 * ::DeveloperApi::
 * Implemented by objects that produce relations for a specific kind of data source
 * with a given schema.  When Spark SQL is given a DDL operation with a USING clause specified (
 * to specify the implemented SchemaRelationProvider) and a user defined schema, this interface
 * is used to pass in the parameters specified by a user.
 *
 * Users may specify the fully qualified class name of a given data source.  When that class is
 * not found Spark SQL will append the class name `DefaultSource` to the path, allowing for
 * less verbose invocation.  For example, 'org.apache.spark.sql.json' would resolve to the
 * data source 'org.apache.spark.sql.json.DefaultSource'
 *
 * A new instance of this class with be instantiated each time a DDL call is made.
 *
 * The difference between a [[RelationProvider]] and a [[SchemaRelationProvider]] is that
 * users need to provide a schema when using a SchemaRelationProvider.
 * A relation provider can inherits both [[RelationProvider]] and [[SchemaRelationProvider]]
 * if it can support both schema inference and user-specified schemas.
 */
@DeveloperApi
trait SchemaRelationProvider {
  /**
   * Returns a new base relation with the given parameters and user defined schema.
   * Note: the parameters' keywords are case insensitive and this insensitivity is enforced
   * by the Map that is passed to the function.
   */
  def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation
}

/**
 * ::DeveloperApi::
 * Implemented by objects that produce relations for a specific kind of data source
 * with a given schema and partitioned columns.  When Spark SQL is given a DDL operation with a
 * USING clause specified (to specify the implemented SchemaRelationProvider), a user defined
 * schema, and an optional list of partition columns, this interface is used to pass in the
 * parameters specified by a user.
 *
 * Users may specify the fully qualified class name of a given data source.  When that class is
 * not found Spark SQL will append the class name `DefaultSource` to the path, allowing for
 * less verbose invocation.  For example, 'org.apache.spark.sql.json' would resolve to the
 * data source 'org.apache.spark.sql.json.DefaultSource'
 *
 * A new instance of this class with be instantiated each time a DDL call is made.
 *
 * The difference between a [[RelationProvider]] and a [[FSBasedRelationProvider]] is
 * that users need to provide a schema and a (possibly empty) list of partition columns when
 * using a SchemaRelationProvider. A relation provider can inherits both [[RelationProvider]],
 * and [[FSBasedRelationProvider]] if it can support schema inference, user-specified
 * schemas, and accessing partitioned relations.
 */
trait FSBasedRelationProvider {
  /**
   * Returns a new base relation with the given parameters, a user defined schema, and a list of
   * partition columns. Note: the parameters' keywords are case insensitive and this insensitivity
   * is enforced by the Map that is passed to the function.
   */
  def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType,
      partitionColumns: StructType): BaseRelation
}

@DeveloperApi
trait CreatableRelationProvider {
  /**
    * Creates a relation with the given parameters based on the contents of the given
    * DataFrame. The mode specifies the expected behavior of createRelation when
    * data already exists.
    * Right now, there are three modes, Append, Overwrite, and ErrorIfExists.
    * Append mode means that when saving a DataFrame to a data source, if data already exists,
    * contents of the DataFrame are expected to be appended to existing data.
    * Overwrite mode means that when saving a DataFrame to a data source, if data already exists,
    * existing data is expected to be overwritten by the contents of the DataFrame.
    * ErrorIfExists mode means that when saving a DataFrame to a data source,
    * if data already exists, an exception is expected to be thrown.
    */
  def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation
}

/**
 * ::DeveloperApi::
 * Represents a collection of tuples with a known schema. Classes that extend BaseRelation must
 * be able to produce the schema of their data in the form of a [[StructType]]. Concrete
 * implementation should inherit from one of the descendant `Scan` classes, which define various
 * abstract methods for execution.
 *
 * BaseRelations must also define a equality function that only returns true when the two
 * instances will return the same data. This equality function is used when determining when
 * it is safe to substitute cached results for a given relation.
 */
@DeveloperApi
abstract class BaseRelation {
  def sqlContext: SQLContext
  def schema: StructType

  /**
   * Returns an estimated size of this relation in bytes. This information is used by the planner
   * to decided when it is safe to broadcast a relation and can be overridden by sources that
   * know the size ahead of time. By default, the system will assume that tables are too
   * large to broadcast. This method will be called multiple times during query planning
   * and thus should not perform expensive operations for each invocation.
   *
   * Note that it is always better to overestimate size than underestimate, because underestimation
   * could lead to execution plans that are suboptimal (i.e. broadcasting a very large table).
   */
  def sizeInBytes: Long = sqlContext.conf.defaultSizeInBytes

  /**
   * Whether does it need to convert the objects in Row to internal representation, for example:
   *  java.lang.String -> UTF8String
   *  java.lang.Decimal -> Decimal
   *
   * Note: The internal representation is not stable across releases and thus data sources outside
   * of Spark SQL should leave this as true.
   */
  def needConversion: Boolean = true
}

/**
 * ::DeveloperApi::
 * A BaseRelation that can produce all of its tuples as an RDD of Row objects.
 */
@DeveloperApi
trait TableScan {
  def buildScan(): RDD[Row]
}

/**
 * ::DeveloperApi::
 * A BaseRelation that can eliminate unneeded columns before producing an RDD
 * containing all of its tuples as Row objects.
 */
@DeveloperApi
trait PrunedScan {
  def buildScan(requiredColumns: Array[String]): RDD[Row]
}

/**
 * ::DeveloperApi::
 * A BaseRelation that can eliminate unneeded columns and filter using selected
 * predicates before producing an RDD containing all matching tuples as Row objects.
 *
 * The actual filter should be the conjunction of all `filters`,
 * i.e. they should be "and" together.
 *
 * The pushed down filters are currently purely an optimization as they will all be evaluated
 * again.  This means it is safe to use them with methods that produce false positives such
 * as filtering partitions based on a bloom filter.
 */
@DeveloperApi
trait PrunedFilteredScan {
  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row]
}

/**
 * ::DeveloperApi::
 * A BaseRelation that can be used to insert data into it through the insert method.
 * If overwrite in insert method is true, the old data in the relation should be overwritten with
 * the new data. If overwrite in insert method is false, the new data should be appended.
 *
 * InsertableRelation has the following three assumptions.
 * 1. It assumes that the data (Rows in the DataFrame) provided to the insert method
 * exactly matches the ordinal of fields in the schema of the BaseRelation.
 * 2. It assumes that the schema of this relation will not be changed.
 * Even if the insert method updates the schema (e.g. a relation of JSON or Parquet data may have a
 * schema update after an insert operation), the new schema will not be used.
 * 3. It assumes that fields of the data provided in the insert method are nullable.
 * If a data source needs to check the actual nullability of a field, it needs to do it in the
 * insert method.
 */
@DeveloperApi
trait InsertableRelation {
  def insert(data: DataFrame, overwrite: Boolean): Unit
}

/**
 * ::Experimental::
 * An interface for experimenting with a more direct connection to the query planner.  Compared to
 * [[PrunedFilteredScan]], this operator receives the raw expressions from the
 * [[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]].  Unlike the other APIs this
 * interface is NOT designed to be binary compatible across releases and thus should only be used
 * for experimentation.
 */
@Experimental
trait CatalystScan {
  def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row]
}

/**
 * ::Experimental::
 * [[OutputWriter]] is used together with [[FSBasedRelation]] for persisting rows to the
 * underlying file system.  Subclasses of [[OutputWriter]] must provide a zero-argument constructor.
 * An [[OutputWriter]] instance is created and initialized when a new output file is opened on
 * executor side.  This instance is used to persist rows to this single output file.
 */
@Experimental
abstract class OutputWriter {
  /**
   * Initializes this [[OutputWriter]] before any rows are persisted.
   *
   * @param path The file path to which this [[OutputWriter]] is supposed to write.
   * @param dataSchema Schema of the rows to be written. Partition columns are not included in the
   *        schema if the corresponding relation is partitioned.
   * @param options Data source options inherited from driver side.
   * @param conf Hadoop configuration inherited from driver side.
   */
  def init(
      path: String,
      dataSchema: StructType,
      options: java.util.Map[String, String],
      conf: Configuration): Unit = ()

  /**
   * Persists a single row.  Invoked on the executor side.  When writing to dynamically partitioned
   * tables, dynamic partition columns are not included in rows to be written.
   */
  def write(row: Row): Unit

  /**
   * Closes the [[OutputWriter]]. Invoked on the executor side after all rows are persisted, before
   * the task output is committed.
   */
  def close(): Unit = ()
}

/**
 * ::Experimental::
 * A [[BaseRelation]] that abstracts file system based data sources.
 *
 * For the read path, similar to [[PrunedFilteredScan]], it can eliminate unneeded columns and
 * filter using selected predicates before producing an RDD containing all matching tuples as
 * [[Row]] objects. In addition, when reading from Hive style partitioned tables stored in file
 * systems, it's able to discover partitioning information from the paths of input directories, and
 * perform partition pruning before start reading the data.Subclasses of [[FSBasedRelation()]] must
 * override one of the three `buildScan` methods to implement the read path.
 *
 * For the write path, it provides the ability to write to both non-partitioned and partitioned
 * tables.  Directory layout of the partitioned tables is compatible with Hive.
 */
@Experimental
abstract class FSBasedRelation extends BaseRelation {
  // Discovers partitioned columns, and merge them with `dataSchema`.  All partition columns not
  // existed in `dataSchema` should be appended to `dataSchema`.
  override val schema: StructType = ???

  /**
   * Base path of this relation.  For partitioned relations, `path` should be the root directory of
   * all partition directories.
   */
  def path: String

  /**
   * Specifies schema of actual data files.  For partitioned relations, if one or more partitioned
   * columns are contained in the data files, they should also appear in `dataSchema`.
   */
  def dataSchema: StructType

  /**
   * Builds an `RDD[Row` containing all rows within this relation.
   *
   * @param inputPaths Data files to be read. If the underlying relation is partitioned, only data
   *        files within required partition directories are included.
   */
  def buildScan(inputPaths: Array[String]): RDD[Row] = {
    throw new RuntimeException(
      "At least one buildScan() method should be overridden to read the relation.")
  }

  /**
   * Builds an `RDD[Row` containing all rows within this relation.
   *
   * @param requiredColumns Required columns.
   * @param inputPaths Data files to be read. If the underlying relation is partitioned, only data
   *        files within required partition directories are included.
   */
  def buildScan(requiredColumns: Array[String], inputPaths: Array[String]): RDD[Row] = {
    buildScan(inputPaths)
  }

  /**
   * Builds an `RDD[Row]` containing all rows within this relation.
   *
   * @param requiredColumns Required columns.
   * @param filters Candidate filters to be pushed down. The actual filter should be the conjunction
   *        of all `filters`.  The pushed down filters are currently purely an optimization as they
   *        will all be evaluated again. This means it is safe to use them with methods that produce
   *        false positives such as filtering partitions based on a bloom filter.
   * @param inputPaths Data files to be read. If the underlying relation is partitioned, only data
   *        files within required partition directories are included.
   */
  def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputPaths: Array[String]): RDD[Row] = {
    buildScan(requiredColumns, inputPaths)
  }

  /**
   * This method is responsible for producing a new [[OutputWriter]] for each newly opened output
   * file on the executor side.
   */
  def outputWriterClass: Class[_ <: OutputWriter]
}
