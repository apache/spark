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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.connector.catalog.{Identifier, IdentityColumnSpec, TableCatalog, TableCatalogCapability}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * This object contains utility methods and values for Identity Columns
 */
object IdentityColumn {
  val IDENTITY_INFO_START = "identity.start"
  val IDENTITY_INFO_STEP = "identity.step"
  val IDENTITY_INFO_ALLOW_EXPLICIT_INSERT = "identity.allowExplicitInsert"

  /**
   * If `schema` contains any generated columns, check whether the table catalog supports identity
   * columns. Otherwise throw an error.
   */
  def validateIdentityColumn(
      schema: StructType,
      catalog: TableCatalog,
      ident: Identifier): Unit = {
    if (hasIdentityColumns(schema)) {
      if (!catalog
          .capabilities()
          .contains(TableCatalogCapability.SUPPORTS_CREATE_TABLE_WITH_IDENTITY_COLUMNS)) {
        throw QueryCompilationErrors.unsupportedTableOperationError(
          catalog, ident, operation = "identity column"
        )
      }
    }
  }

  /**
   * Whether the given `field` is an identity column
   */
  def isIdentityColumn(field: StructField): Boolean = {
    field.metadata.contains(IDENTITY_INFO_START)
  }

  /**
   * Returns the identity information stored in the column metadata if it exists
   */
  def getIdentityInfo(field: StructField): Option[IdentityColumnSpec] = {
    if (isIdentityColumn(field)) {
      Some(new IdentityColumnSpec(
        field.metadata.getString(IDENTITY_INFO_START).toLong,
        field.metadata.getString(IDENTITY_INFO_STEP).toLong,
        field.metadata.getString(IDENTITY_INFO_ALLOW_EXPLICIT_INSERT).toBoolean))
    } else {
      None
    }
  }

  /**
   * Whether the `schema` has one or more identity columns
   */
  def hasIdentityColumns(schema: StructType): Boolean = {
    schema.exists(isIdentityColumn)
  }
}
