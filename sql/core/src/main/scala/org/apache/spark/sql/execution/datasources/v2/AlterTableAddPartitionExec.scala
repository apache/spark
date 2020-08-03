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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{CatalogV2Implicits, Identifier, TableCatalog}

/**
 * Physical plan node for adding partitions of table.
 */
case class AlterTableAddPartitionExec(
    catalog: TableCatalog,
    ident: Identifier,
    partitionSpecsAndLocs: Seq[(TablePartitionSpec, Option[String])],
    ignoreIfExists: Boolean) extends V2CommandExec {
  import DataSourceV2Implicits._
  import CatalogV2Implicits._

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    val table = catalog.loadTable(ident).asPartitionable
    val partNames = table.partitionSchema().map(_.name)

    partitionSpecsAndLocs.foreach { case (spec, location) =>
      val partParams = new java.util.HashMap[String, String](table.properties())
      location.foreach(locationUri =>
        partParams.put("location", locationUri))
      partParams.put("ignoreIfExists", ignoreIfExists.toString)

      val conflictKeys = spec.keys.filterNot(partNames.contains)
      if (conflictKeys.nonEmpty) {
        throw new AnalysisException(
          s"Partition key ${conflictKeys.mkString(",")} " +
            s"not exists in ${ident.namespace().quoted}.${ident.name()}")
      }

      val partIdent: InternalRow = convertPartitionIndentifers(spec, table.partitionSchema())
      table.createPartition(partIdent, partParams)
    }
    Seq.empty
  }
}
