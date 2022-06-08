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

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.plans.logical.ViewDescription
import org.apache.spark.sql.connector.catalog.{View, ViewCatalog}
import org.apache.spark.sql.types.StructType

/**
 * View description backed by a View in V2 catalog.
 *
 * @param view a view in V2 catalog
 */
case class V2ViewDescription(
    override val identifier: String,
    view: View) extends ViewDescription {

  override val schema: StructType = view.schema

  override val viewText: Option[String] = None

  override val viewCatalogAndNamespace: Seq[String] =
    view.currentCatalog +: view.currentNamespace.toSeq

  override val viewQueryColumnNames: Seq[String] = view.schema.fieldNames

  val sql: String = view.sql

  val comment: Option[String] = Option(view.properties.get(ViewCatalog.PROP_COMMENT))

  val owner: Option[String] = Option(view.properties.get(ViewCatalog.PROP_OWNER))

  val createEngineVersion: Option[String] =
    Option(view.properties.get(ViewCatalog.PROP_CREATE_ENGINE_VERSION))

  val properties: Map[String, String] =
    view.properties.asScala.toMap -- ViewCatalog.RESERVED_PROPERTIES.asScala
}
