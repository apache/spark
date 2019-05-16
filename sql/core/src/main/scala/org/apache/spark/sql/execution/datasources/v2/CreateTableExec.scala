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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalog.v2.{Identifier, TableCatalog}
import org.apache.spark.sql.catalog.v2.expressions.Transform
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.types.StructType

case class CreateTableExec(
    catalog: TableCatalog,
    identifier: Identifier,
    tableSchema: StructType,
    partitioning: Seq[Transform],
    tableProperties: Map[String, String],
    ignoreIfExists: Boolean) extends LeafExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    def create(): Unit = {
      catalog.createTable(identifier, tableSchema, partitioning.toArray, tableProperties.asJava)
    }

    if (!catalog.tableExists(identifier)) {
      if (ignoreIfExists) {
        try {
          create()
        } catch {
          case _: TableAlreadyExistsException =>
            // ignore the table that was created after checking existence
        }
      } else {
        create()
      }
    } else if (!ignoreIfExists) {
      throw new TableAlreadyExistsException(identifier)
    }

    sqlContext.sparkContext.parallelize(Seq.empty, 1)
  }

  override def output: Seq[Attribute] = Seq.empty
}
