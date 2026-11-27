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

import org.apache.spark.sql.catalyst.plans.logical.CreateNamespace
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, SupportsNamespaces}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.command.{AlterDatabaseSetLocationCommand, CreateDatabaseCommand}
import org.apache.spark.util.SparkStringUtils

/** Lowering of namespace creation and location changes to V1 database commands. */
private[sql] object NamespaceCommandHelper {

  /** Removes reserved namespace properties and validates an explicitly supplied location. */
  def lowerCreateNamespace(c: CreateNamespace, name: String): CreateDatabaseCommand = {
    val comment = c.properties.get(SupportsNamespaces.PROP_COMMENT)
    val location = c.properties.get(SupportsNamespaces.PROP_LOCATION)
    val newProperties = c.properties -- CatalogV2Util.NAMESPACE_RESERVED_PROPERTIES
    if (location.exists(SparkStringUtils.isBlank)) {
      throw QueryExecutionErrors.invalidEmptyLocationError(location.get)
    }
    CreateDatabaseCommand(name, c.ifNotExists, location, comment, newProperties)
  }

  /** Rejects an empty database location before constructing the V1 command. */
  def lowerSetLocation(db: String, location: String): AlterDatabaseSetLocationCommand = {
    if (SparkStringUtils.isBlank(location)) {
      throw QueryExecutionErrors.invalidEmptyLocationError(location)
    }
    AlterDatabaseSetLocationCommand(db, location)
  }
}
