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

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan


/**
 * Thrown when a query fails to analyze, usually because the query itself is invalid.
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
class AnalysisException(
    val message: String,
    val line: Option[Int] = None,
    val startPosition: Option[Int] = None,
    // Some plans fail to serialize due to bugs in scala collections.
    @transient val plan: Option[LogicalPlan] = None,
    val cause: Option[Throwable] = None)
  extends Exception(message, cause.orNull) with Serializable {

  def withPlan(plan: LogicalPlan): AnalysisException = {
    withPosition(plan.origin.line, plan.origin.startPosition, Option(plan))
  }

  def withPosition(line: Option[Int], startPosition: Option[Int]): AnalysisException = {
    withPosition(line, startPosition, None)
  }

  private def withPosition(
      line: Option[Int],
      startPosition: Option[Int],
      plan: Option[LogicalPlan]): AnalysisException = {
    val newException = new AnalysisException(message, line, startPosition, plan)
    newException.setStackTrace(getStackTrace)
    newException
  }

  override def getMessage: String = {
    val planAnnotation = Option(plan).flatten.map(p => s";\n$p").getOrElse("")
    getSimpleMessage + planAnnotation
  }

  // Outputs an exception without the logical plan.
  // For testing only
  def getSimpleMessage: String = {
    val lineAnnotation = line.map(l => s" line $l").getOrElse("")
    val positionAnnotation = startPosition.map(p => s" pos $p").getOrElse("")
    s"$message;$lineAnnotation$positionAnnotation"
  }
}

object AnalysisException {
  /**
   * Create a no such database analysis exception.
   */
  def noSuchDatabase(db: String): AnalysisException = {
    new AnalysisException(s"Database '$db' not found")
  }

  /**
   * Create a database already exists analysis exception.
   */
  def databaseAlreadyExists(db: String): AnalysisException = {
    new AnalysisException(s"Database '$db' already exists")
  }

  /**
   * Create a no such table analysis exception.
   */
  def noSuchTable(db: String, table: String): AnalysisException = {
    new AnalysisException(s"Table or view '$table' not found in database '$db'")
  }

  /**
   * Create a table already exists analysis exception.
   */
  def tableAlreadyExists(db: String, table: String): AnalysisException = {
    new AnalysisException(s"Table or view '$table' already exists in database '$db'")
  }

  /**
   * Create a temporary table already exists analysis exception.
   */
  def tempTableAlreadyExists(table: String): AnalysisException = {
    new AnalysisException(s"Temporary table '$table' already exists")
  }

  /**
   * Create a no such partition analysis exception.
   */
  def noSuchPartition(db: String, table: String, spec: TablePartitionSpec): AnalysisException = {
    new AnalysisException(
      s"Partition not found in table '$table' database '$db':\n" + spec.mkString("\n"))
  }

  /**
   * Create a partition already exists analysis exception.
   */
  def partitionAlreadyExists(
      db: String,
      table: String,
      spec: TablePartitionSpec): AnalysisException = {
    new AnalysisException(
      s"Partition already exists in table '$table' database '$db':\n" + spec.mkString("\n"))
  }

  /**
   * Create a no such partitions analysis exception.
   */
  def noSuchPartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec]): AnalysisException = {
    new AnalysisException(
      s"The following partitions not found in table '$table' database '$db':\n"
        + specs.mkString("\n===\n"))
  }

  /**
   * Create a partitions already exists analysis exception.
   */
  def partitionsAlreadyExists(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec]): AnalysisException = {
    new AnalysisException(
      s"The following partitions already exists in table '$table' database '$db':\n"
        + specs.mkString("\n===\n"))
  }

  /**
   * Create a no such function exception.
   */
  def noSuchFunction(db: String, func: String): AnalysisException = {
    new AnalysisException(
      s"Undefined function: '$func'. This function is neither a registered temporary " +
        s"function nor a permanent function registered in the database '$db'.")
  }

  /**
   * Create a function already exists analysis exception.
   */
  def functionAlreadyExists(db: String, func: String): AnalysisException = {
    new AnalysisException(s"Function '$func' already exists in database '$db'")
  }

  /**
   * Create a no such permanent function exception.
   */
  def noSuchPermanentFunction(db: String, func: String): AnalysisException = {
    new AnalysisException(s"Function '$func' not found in database '$db'")
  }

  /**
   * Create a no such temporary function exception.
   */
  def noSuchTempFunction(func: String): AnalysisException = {
    new AnalysisException(s"Temporary function '$func' not found")
  }
}
