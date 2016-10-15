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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types._


/**
 * Used to represent the operation of create table using a data source.
 *
 * @param allowExisting If it is true, we will do nothing when the table already exists.
 *                      If it is false, an exception will be thrown
 */
case class CreateTableUsing(
    tableIdent: TableIdentifier,
    userSpecifiedSchema: Option[StructType],
    provider: String,
    temporary: Boolean,
    options: Map[String, String],
    partitionColumns: Array[String],
    bucketSpec: Option[BucketSpec],
    allowExisting: Boolean,
    managedIfNoPath: Boolean) extends logical.Command

/**
 * A node used to support CTAS statements and saveAsTable for the data source API.
 */
case class CreateTableUsingAsSelect(
    tableIdent: TableIdentifier,
    provider: String,
    partitionColumns: Array[String],
    bucketSpec: Option[BucketSpec],
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan) extends logical.Command {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(query)
  override lazy val resolved: Boolean = query.resolved
}

case class CreateTempViewUsing(
    tableIdent: TableIdentifier,
    userSpecifiedSchema: Option[StructType],
    replace: Boolean,
    provider: String,
    options: Map[String, String]) extends RunnableCommand {

  if (tableIdent.database.isDefined) {
    throw new AnalysisException(
      s"Temporary table '$tableIdent' should not have specified a database")
  }

  def run(sparkSession: SparkSession): Seq[Row] = {
    val dataSource = DataSource(
      sparkSession,
      userSpecifiedSchema = userSpecifiedSchema,
      className = provider,
      options = options)
    sparkSession.sessionState.catalog.createTempView(
      tableIdent.table,
      Dataset.ofRows(sparkSession, LogicalRelation(dataSource.resolveRelation())).logicalPlan,
      replace)

    Seq.empty[Row]
  }
}

case class RefreshTable(tableIdent: TableIdentifier)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Refresh the given table's metadata. If this table is cached as an InMemoryRelation,
    // drop the original cached version and make the new version cached lazily.
    sparkSession.catalog.refreshTable(tableIdent.quotedString)
    Seq.empty[Row]
  }
}

case class RefreshResource(path: String)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.catalog.refreshByPath(path)
    Seq.empty[Row]
  }
}

/**
 * Builds a map in which keys are case insensitive
 */
class CaseInsensitiveMap(map: Map[String, String]) extends Map[String, String]
  with Serializable {

  val baseMap = map.map(kv => kv.copy(_1 = kv._1.toLowerCase))

  override def get(k: String): Option[String] = baseMap.get(k.toLowerCase)

  override def + [B1 >: String](kv: (String, B1)): Map[String, B1] =
    baseMap + kv.copy(_1 = kv._1.toLowerCase)

  override def iterator: Iterator[(String, String)] = baseMap.iterator

  override def -(key: String): Map[String, String] = baseMap - key.toLowerCase
}
