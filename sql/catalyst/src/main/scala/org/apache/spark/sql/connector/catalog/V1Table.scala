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
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.quoteIfNeeded
import org.apache.spark.sql.connector.expressions.{LogicalExpressions, Transform}
import org.apache.spark.sql.types.StructType

/**
 * An implementation of catalog v2 `Table` to expose v1 table metadata.
 */
private[sql] case class V1Table(v1Table: CatalogTable) extends Table {
  implicit class IdentifierHelper(identifier: TableIdentifier) {
    def quoted: String = {
      identifier.database match {
        case Some(db) =>
          Seq(db, identifier.table).map(quoteIfNeeded).mkString(".")
        case _ =>
          quoteIfNeeded(identifier.table)

      }
    }
  }

  def catalogTable: CatalogTable = v1Table

  lazy val options: Map[String, String] = {
    v1Table.storage.locationUri match {
      case Some(uri) =>
        v1Table.storage.properties + ("path" -> uri.toString)
      case _ =>
        v1Table.storage.properties
    }
  }

  override lazy val properties: util.Map[String, String] = v1Table.properties.asJava

  override lazy val schema: StructType = v1Table.schema

  override lazy val partitioning: Array[Transform] = {
    import CatalogV2Implicits._
    val partitions = new mutable.ArrayBuffer[Transform]()

    v1Table.partitionColumnNames.foreach { col =>
      partitions += LogicalExpressions.identity(LogicalExpressions.reference(Seq(col)))
    }

    v1Table.bucketSpec.foreach { spec =>
      partitions += spec.asTransform
    }

    partitions.toArray
  }

  override def name: String = v1Table.identifier.quoted

  override def capabilities: util.Set[TableCapability] = new util.HashSet[TableCapability]()

  override def toString: String = s"V1Table($name)"
}

/**
 * A V2 table with V1 fallback support. This is used to fallback to V1 table when the V2 one
 * doesn't implement specific capabilities but V1 already has.
 */
private[sql] trait V2TableWithV1Fallback extends Table {
  def v1Table: CatalogTable
}
