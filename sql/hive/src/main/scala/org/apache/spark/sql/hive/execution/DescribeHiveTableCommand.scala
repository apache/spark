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

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.metastore.api.FieldSchema

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.hive.MetastoreRelation
import org.apache.spark.sql.{Row, SQLContext}

/**
 * Implementation for "describe [extended] table".
 */
private[hive]
case class DescribeHiveTableCommand(
    table: MetastoreRelation,
    override val output: Seq[Attribute],
    isExtended: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    // Trying to mimic the format of Hive's output. But not exactly the same.
    var results: Seq[(String, String, String)] = Nil

    val columns: Seq[FieldSchema] = table.hiveQlTable.getCols.asScala
    val partitionColumns: Seq[FieldSchema] = table.hiveQlTable.getPartCols.asScala
    results ++= columns.map(field => (field.getName, field.getType, field.getComment))
    if (partitionColumns.nonEmpty) {
      val partColumnInfo =
        partitionColumns.map(field => (field.getName, field.getType, field.getComment))
      results ++=
        partColumnInfo ++
          Seq(("# Partition Information", "", "")) ++
          Seq((s"# ${output(0).name}", output(1).name, output(2).name)) ++
          partColumnInfo
    }

    if (isExtended) {
      results ++= Seq(("Detailed Table Information", table.hiveQlTable.getTTable.toString, ""))
    }

    results.map { case (name, dataType, comment) =>
      Row(name, dataType, comment)
    }
  }
}
