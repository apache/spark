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

package org.apache.spark.sql.internal.connector

import java.util.Objects

import org.apache.spark.sql.connector.catalog.{Column, ColumnDefaultValue, IdentityColumnSpec}
import org.apache.spark.sql.types.DataType

// The standard concrete implementation of data source V2 column.
case class ColumnImpl(
    name: String,
    dataType: DataType,
    nullable: Boolean,
    comment: String,
    defaultValue: ColumnDefaultValue,
    generationExpression: String,
    identityColumnSpec: IdentityColumnSpec,
    metadataInJSON: String,
    override val id: String = null) extends Column {

  // [[id]] is excluded from [[equals]] and [[hashCode]] because IDs only live on [[Column]],
  // not on [[StructField]] metadata. Any code path that round-trips through [[StructType]]
  // (e.g. [[CatalogV2Util.v2ColumnsToStructType]] followed by [[structTypeToV2Columns]])
  // drops the ID, producing a [[Column]] with id=null for the same logical column. Including
  // [[id]] in equality would cause spurious mismatches across these round-trips.
  // Column ID validation is performed separately by [[V2TableUtil.validateColumnIds]].
  override def equals(other: Any): Boolean = other match {
    case that: ColumnImpl =>
      name == that.name &&
        dataType == that.dataType &&
        nullable == that.nullable &&
        comment == that.comment &&
        defaultValue == that.defaultValue &&
        generationExpression == that.generationExpression &&
        identityColumnSpec == that.identityColumnSpec &&
        metadataInJSON == that.metadataInJSON
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hash(
      name,
      dataType,
      Boolean.box(nullable),
      comment,
      defaultValue,
      generationExpression,
      identityColumnSpec,
      metadataInJSON)
  }
}
