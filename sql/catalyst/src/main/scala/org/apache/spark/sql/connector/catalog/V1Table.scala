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

package org.apache.spark.sql.connector.catalog

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.expressions.{LogicalExpressions, Transform}
import org.apache.spark.sql.types.StructType

/**
 * An implementation of catalog v2 `Table` to expose v1 table metadata.
 */
private[sql] case class V1Table(v1Table: CatalogTable) extends Table {
  assert(v1Table.provider.isDefined)

  implicit class IdentifierHelper(identifier: TableIdentifier) {
    def quoted: String = {
      identifier.database match {
        case Some(db) =>
          Seq(db, identifier.table).map(quote).mkString(".")
        case _ =>
          quote(identifier.table)
      }
    }

    private def quote(part: String): String = {
      if (part.contains(".") || part.contains("`")) {
        s"`${part.replace("`", "``")}`"
      } else {
        part
      }
    }
  }

  override lazy val properties: util.Map[String, String] = {
    val pathOption = v1Table.storage.locationUri match {
      case Some(uri) =>
        Some("path" -> uri.toString)
      case _ =>
        None
    }
    val providerOption = "provider" -> v1Table.provider.get
    (v1Table.storage.properties ++ v1Table.properties ++ pathOption + providerOption).asJava
  }

  override def schema: StructType = v1Table.schema

  override lazy val partitioning: Array[Transform] = {
    val partitions = new mutable.ArrayBuffer[Transform]()

    v1Table.partitionColumnNames.foreach { col =>
      partitions += LogicalExpressions.identity(col)
    }

    v1Table.bucketSpec.foreach { spec =>
      partitions += LogicalExpressions.bucket(spec.numBuckets, spec.bucketColumnNames: _*)
    }

    partitions.toArray
  }

  override def name: String = v1Table.identifier.quoted

  override def capabilities: util.Set[TableCapability] = new util.HashSet[TableCapability]()

  override def toString: String = s"V1Table($name)"
}
