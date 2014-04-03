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

import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Contains functions that are shared between all SchemaRDD types (i.e., Scala, Java)
 */
trait SchemaRDDLike {
  @transient val sqlContext: SQLContext
  @transient protected[spark] val logicalPlan: LogicalPlan

  private[sql] def baseSchemaRDD: SchemaRDD

  /**
   * A lazily computed query execution workflow.  All other RDD operations are passed
   * through to the RDD that is produced by this workflow.
   *
   * We want this to be lazy because invoking the whole query optimization pipeline can be
   * expensive.
   */
  @transient
  protected[spark] lazy val queryExecution = sqlContext.executePlan(logicalPlan)

  override def toString =
    s"""${super.toString}
       |== Query Plan ==
       |${queryExecution.executedPlan}""".stripMargin.trim


  /**
   * Saves the contents of this `SchemaRDD` as a parquet file, preserving the schema.  Files that
   * are written out using this method can be read back in as a SchemaRDD using the ``function
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
  def registerAsTable(tableName: String): Unit = {
    sqlContext.registerRDDAsTable(baseSchemaRDD, tableName)
  }
}
