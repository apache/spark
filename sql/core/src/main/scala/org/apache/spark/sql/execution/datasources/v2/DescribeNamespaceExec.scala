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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericRowWithSchema}
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog.COMMENT_TABLE_PROP
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog.LOCATION_TABLE_PROP
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog.RESERVED_PROPERTIES
import org.apache.spark.sql.types.StructType

/**
 * Physical plan node for describing a namespace.
 */
case class DescribeNamespaceExec(
    output: Seq[Attribute],
    catalog: CatalogPlugin,
    namespace: Seq[String],
    isExtended: Boolean) extends V2CommandExec {

  private val encoder = RowEncoder(StructType.fromAttributes(output)).resolveAndBind()

  override protected def run(): Seq[InternalRow] = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

    val rows = new ArrayBuffer[InternalRow]()
    val nsCatalog = catalog.asNamespaceCatalog
    val ns = namespace.toArray
    val metadata = nsCatalog.loadNamespaceMetadata(ns)

    rows += toCatalystRow("Namespace Name", ns.last)
    rows += toCatalystRow("Description", metadata.get(COMMENT_TABLE_PROP))
    rows += toCatalystRow("Location", metadata.get(LOCATION_TABLE_PROP))
    if (isExtended) {
      val properties = metadata.asScala.toSeq.filter(p => !RESERVED_PROPERTIES.contains(p._1))
      if (properties.nonEmpty) {
        rows += toCatalystRow("Properties", properties.mkString("(", ",", ")"))
      }
    }
    rows
  }

  private def toCatalystRow(strs: String*): InternalRow = {
    encoder.toRow(new GenericRowWithSchema(strs.toArray, schema)).copy()
  }
}
