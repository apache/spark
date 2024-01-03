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

package org.apache.spark.sql.catalyst.analysis

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{Identifier, View, ViewCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ArrayImplicits.SparkArrayOps

/**
 * A trait for view description.
 */
trait ViewDescription {

  val ident: Identifier

  // For backwards compatibility, we need to keep the `identifier` as a `TableIdentifier`.
  val identifier: TableIdentifier

  val viewText: Option[String]

  val viewCatalogAndNamespace: Seq[String]

  val viewQueryColumnNames: Seq[String]

  val viewSQLConfigs: Map[String, String]

  val schema: StructType

  val properties: Map[String, String]

  def query: String = viewText.getOrElse("")

  def comment: Option[String] = properties.get(ViewCatalog.PROP_COMMENT)

  def owner: Option[String] = properties.get(ViewCatalog.PROP_OWNER)

  def createEngineVersion: Option[String] = properties.get(ViewCatalog.PROP_CREATE_ENGINE_VERSION)
}

/**
 * View description backed by a [[CatalogTable]].
 *
 * @param metadata a CatalogTable
 */
case class V1ViewDescription(metadata: CatalogTable) extends ViewDescription {

  override val ident: Identifier = metadata.identifier.nameParts.asIdentifier

  override val identifier: TableIdentifier = metadata.identifier

  override val viewText: Option[String] = metadata.viewText

  override val viewCatalogAndNamespace: Seq[String] = metadata.viewCatalogAndNamespace

  override val viewQueryColumnNames: Seq[String] = metadata.viewQueryColumnNames

  override val viewSQLConfigs: Map[String, String] = metadata.viewSQLConfigs

  override val schema: StructType = metadata.schema

  override val properties: Map[String, String] = metadata.properties
}

/**
 * View description backed by a V2 [[View]].
 *
 * @param view a view in V2 catalog
 */
case class V2ViewDescription(
    override val ident: Identifier,
    view: View) extends ViewDescription {

  override val identifier: TableIdentifier = ident.asTableIdentifier

  override val viewText: Option[String] = Option(view.query)

  override val viewCatalogAndNamespace: Seq[String] =
    view.currentCatalog +: view.currentNamespace.toSeq

  override val viewQueryColumnNames: Seq[String] = view.schema.fieldNames.toImmutableArraySeq

  override val viewSQLConfigs: Map[String, String] = Map.empty

  override val schema: StructType = view.schema

  override val properties: Map[String, String] =
    view.properties.asScala.toMap -- ViewCatalog.RESERVED_PROPERTIES.asScala
}
