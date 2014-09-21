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

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkLogicalPlan

/**
 * Contains functions that are shared between all SchemaRDD types (i.e., Scala, Java)
 */
private[sql] trait SchemaRDDLike {
  @transient val sqlContext: SQLContext
  @transient val baseLogicalPlan: LogicalPlan

  private[sql] def baseSchemaRDD: SchemaRDD

  /**
   * :: DeveloperApi ::
   * A lazily computed query execution workflow.  All other RDD operations are passed
   * through to the RDD that is produced by this workflow. This workflow is produced lazily because
   * invoking the whole query optimization pipeline can be expensive.
   *
   * The query execution is considered a Developer API as phases may be added or removed in future
   * releases.  This execution is only exposed to provide an interface for inspecting the various
   * phases for debugging purposes.  Applications should not depend on particular phases existing
   * or producing any specific output, even for exactly the same query.
   *
   * Additionally, the RDD exposed by this execution is not designed for consumption by end users.
   * In particular, it does not contain any schema information, and it reuses Row objects
   * internally.  This object reuse improves performance, but can make programming against the RDD
   * more difficult.  Instead end users should perform RDD operations on a SchemaRDD directly.
   */
  @transient
  @DeveloperApi
  lazy val queryExecution = sqlContext.executePlan(baseLogicalPlan)

  @transient protected[spark] val logicalPlan: LogicalPlan = baseLogicalPlan match {
    // For various commands (like DDL) and queries with side effects, we force query optimization to
    // happen right away to let these side effects take place eagerly.
    case _: Command | _: InsertIntoTable | _: CreateTableAsSelect |_: WriteToFile =>
      queryExecution.toRdd
      SparkLogicalPlan(queryExecution.executedPlan)(sqlContext)
    case _ =>
      baseLogicalPlan
  }

  override def toString =
    s"""${super.toString}
       |== Query Plan ==
       |${queryExecution.simpleString}""".stripMargin.trim

  /**
   * Saves the contents of this `SchemaRDD` as a parquet file, preserving the schema.  Files that
   * are written out using this method can be read back in as a SchemaRDD using the `parquetFile`
   * function.
   *
   * @group schema
   */
  def saveAsParquetFile(path: String): Unit = {
    sqlContext.executePlan(WriteToFile(path, logicalPlan)).toRdd
  }

  /**
   * Registers this RDD as a temporary table using the given name.  The lifetime of this temporary
   * table is tied to the [[SQLContext]] that was used to create this SchemaRDD.
   *
   * @group schema
   */
  def registerTempTable(tableName: String): Unit = {
    sqlContext.registerRDDAsTable(baseSchemaRDD, tableName)
  }

  @deprecated("Use registerTempTable instead of registerAsTable.", "1.1")
  def registerAsTable(tableName: String): Unit = registerTempTable(tableName)

  /**
   * :: Experimental ::
   * Adds the rows from this RDD to the specified table, optionally overwriting the existing data.
   *
   * @group schema
   */
  @Experimental
  def insertInto(tableName: String, overwrite: Boolean): Unit =
    sqlContext.executePlan(
      InsertIntoTable(UnresolvedRelation(None, tableName), Map.empty, logicalPlan, overwrite)).toRdd

  /**
   * :: Experimental ::
   * Appends the rows from this RDD to the specified table.
   *
   * @group schema
   */
  @Experimental
  def insertInto(tableName: String): Unit = insertInto(tableName, overwrite = false)

  /**
   * :: Experimental ::
   * Creates a table from the the contents of this SchemaRDD.  This will fail if the table already
   * exists.
   *
   * Note that this currently only works with SchemaRDDs that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   *
   * @group schema
   */
  @Experimental
  def saveAsTable(tableName: String): Unit =
    sqlContext.executePlan(CreateTableAsSelect(None, tableName, logicalPlan)).toRdd

  /** Returns the schema as a string in the tree format.
   *
   * @group schema
   */
  def schemaString: String = baseSchemaRDD.schema.treeString

  /** Prints out the schema.
   *
   * @group schema
   */
  def printSchema(): Unit = println(schemaString)
}
