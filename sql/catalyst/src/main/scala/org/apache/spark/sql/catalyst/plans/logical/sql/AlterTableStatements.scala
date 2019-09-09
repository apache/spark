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

package org.apache.spark.sql.catalyst.plans.logical.sql

import org.apache.spark.sql.types.DataType

/**
 * Column data as parsed by ALTER TABLE ... ADD COLUMNS.
 */
case class QualifiedColType(name: Seq[String], dataType: DataType, comment: Option[String])

trait AlterTableStatement extends ParsedStatement {
  val tableName: Seq[String]
}

/**
 * ALTER TABLE ... ADD COLUMNS command, as parsed from SQL.
 */
case class AlterTableAddColumnsStatement(
    tableName: Seq[String],
    columnsToAdd: Seq[QualifiedColType]) extends AlterTableStatement

/**
 * ALTER TABLE ... CHANGE COLUMN command, as parsed from SQL.
 */
case class AlterTableAlterColumnStatement(
    tableName: Seq[String],
    column: Seq[String],
    dataType: Option[DataType],
    comment: Option[String]) extends AlterTableStatement

/**
 * ALTER TABLE ... RENAME COLUMN command, as parsed from SQL.
 */
case class AlterTableRenameColumnStatement(
    tableName: Seq[String],
    column: Seq[String],
    newName: String) extends AlterTableStatement

/**
 * ALTER TABLE ... DROP COLUMNS command, as parsed from SQL.
 */
case class AlterTableDropColumnsStatement(
    tableName: Seq[String],
    columnsToDrop: Seq[Seq[String]]) extends AlterTableStatement

/**
 * ALTER TABLE ... SET TBLPROPERTIES command, as parsed from SQL.
 */
case class AlterTableSetPropertiesStatement(
    tableName: Seq[String],
    properties: Map[String, String]) extends AlterTableStatement

/**
 * ALTER TABLE ... UNSET TBLPROPERTIES command, as parsed from SQL.
 */
case class AlterTableUnsetPropertiesStatement(
    tableName: Seq[String],
    propertyKeys: Seq[String],
    ifExists: Boolean) extends AlterTableStatement

/**
 * ALTER TABLE ... SET LOCATION command, as parsed from SQL.
 */
case class AlterTableSetLocationStatement(
    tableName: Seq[String],
    location: String) extends AlterTableStatement
