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

import org.apache.hadoop.hive.ql.metadata.HiveUtils.{unparseIdentifier => verbose}

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.hive.{HiveMetastoreTypes, HiveContext}
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.hive.client.{HiveColumn, HiveTable}

private[hive]
case class CreateViewAsSelect(
    tableDesc: HiveTable,
    childSchema: Seq[Attribute],
    allowExisting: Boolean) extends RunnableCommand {

  assert(tableDesc.schema == Nil || tableDesc.schema.length == childSchema.length)
  assert(tableDesc.viewText.isDefined)

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    val database = tableDesc.database
    val viewName = tableDesc.name
    val viewText = tableDesc.viewText.get

    if (hiveContext.catalog.tableExists(Seq(database, viewName))) {
      if (allowExisting) {
        // view already exists, will do nothing, to keep consistent with Hive
      } else {
        throw new AnalysisException(s"$database.$viewName already exists.")
      }
    } else {
      val schema = if (tableDesc.schema == Nil) {
        childSchema.map { attr =>
          HiveColumn(attr.name, HiveMetastoreTypes.toMetastoreType(attr.dataType), null)
        }
      } else {
        childSchema.zip(tableDesc.schema).map { case (attr, col) =>
          HiveColumn(col.name, HiveMetastoreTypes.toMetastoreType(attr.dataType), col.comment)
        }
      }

      val columnNames = childSchema.map(f => verbose(f.name))
      val projectList = if (tableDesc.schema == Nil) {
        columnNames.mkString(", ")
      } else {
        columnNames.zip(tableDesc.schema.map(f => verbose(f.name))).map {
          case (name, alias) => s"$name AS $alias"
        }.mkString(", ")
      }

      val expandedText = s"SELECT $projectList FROM ($viewText) ${verbose(viewName)}"

      hiveContext.catalog.client.createView(
        tableDesc.copy(schema = schema, viewText = Some(expandedText)))
    }

    Seq.empty[Row]
  }
}
