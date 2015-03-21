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

package org.apache.spark.sql.hive.execution

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.metastore.api.FieldSchema

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions.{Attribute, Row}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.MetastoreRelation
import org.apache.spark.sql.hive.MetaStoreViewRelation
import org.apache.spark.sql.hive.MetastoreDataSourceRelation

/**
 * Implementation for "describe [extended] table".
 */
private[hive]
case class DescribeHiveTableCommand(
    table: AnyRef,
    override val output: Seq[Attribute],
    isExtended: Boolean) extends RunnableCommand {

  def process(m: MetastoreRelation): Seq[(String, String, String)] = {
    var results: Seq[(String, String, String)] = Nil

    val columns: Seq[FieldSchema] = m.hiveQlTable.getCols
    val partitionColumns: Seq[FieldSchema] = m.hiveQlTable.getPartCols
    results ++= columns.map(field => (field.getName, field.getType, field.getComment))
    if (partitionColumns.nonEmpty) {
      val partColumnInfo =
        partitionColumns.map(field => (field.getName, field.getType, field.getComment))
      results ++=
        partColumnInfo ++
          Seq(("# Partition Information", "", "")) ++
          Seq((s"# ${output.get(0).name}", output.get(1).name, output.get(2).name)) ++
          partColumnInfo
    }

    if (isExtended) {
      results ++= Seq(("Detailed Table Information", m.hiveQlTable.getTTable.toString, ""))
    }

    results
  }

  def process(m: MetaStoreViewRelation): Seq[(String, String, String)] = {
    var results: Seq[(String, String, String)] = Nil

    val columns: Seq[FieldSchema] = m.table.getCols
    results ++= columns.map(field => (field.getName, field.getType, field.getComment))

    if (isExtended) {
      results ++= Seq(("Detailed View Information", m.table.getTTable.toString, ""))
    }

    results
  }

  def process(m: MetastoreDataSourceRelation): Seq[(String, String, String)] = {
    var results: Seq[(String, String, String)] = Nil

    val columns: Seq[FieldSchema] = m.table.getCols
    results ++= columns.map(field => (field.getName, field.getType, field.getComment))

    if (isExtended) {
      results ++= Seq(
        ("Detailed DataSourceProvider Table Information",
        m.table.getTTable.toString, ""))
    }

    results
  }

  override def run(sqlContext: SQLContext) = {
    // Trying to mimic the format of Hive's output. But not exactly the same.
    val results = table match {
      case m: MetastoreRelation => process(m)
      case m: MetaStoreViewRelation => process(m)
      case m: MetastoreDataSourceRelation => process(m)
    }

    results.map { case (name, dataType, comment) =>
      Row(name, dataType, comment)
    }
  }
}
