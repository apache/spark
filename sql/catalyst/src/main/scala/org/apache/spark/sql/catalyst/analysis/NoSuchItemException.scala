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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.types.StructType


/**
 * Thrown by a catalog when an item cannot be found. The analyzer will rethrow the exception
 * as an [[org.apache.spark.sql.AnalysisException]] with the correct position information.
 */
case class NoSuchDatabaseException(db: String)
  extends AnalysisException(s"Database '$db' not found")

case class NoSuchNamespaceException(
    override val message: String,
    override val cause: Option[Throwable] = None)
  extends AnalysisException(message, cause = cause) {

  def this(namespace: Array[String]) = {
    this(s"Namespace '${namespace.quoted}' not found")
  }
}

case class NoSuchTableException(
    override val message: String,
    override val cause: Option[Throwable] = None)
  extends AnalysisException(
    message,
    errorClass = Some("_LEGACY_ERROR_TEMP_1115"),
    messageParameters = Map("msg" -> message),
    cause = cause) {

  def this(db: String, table: String) = {
    this(s"Table or view '$table' not found in database '$db'")
  }

  def this(tableIdent: Identifier) = {
    this(s"Table ${tableIdent.quoted} not found")
  }

  def this(nameParts: Seq[String]) = {
    this(s"Table ${nameParts.quoted} not found")
  }
}

case class NoSuchPartitionException(
    override val message: String)
  extends AnalysisException(message) {

  def this(db: String, table: String, spec: TablePartitionSpec) = {
    this(s"Partition not found in table '$table' database '$db':\n" + spec.mkString("\n"))
  }

  def this(tableName: String, partitionIdent: InternalRow, partitionSchema: StructType) = {
    this(s"Partition not found in table $tableName: "
      + partitionIdent.toSeq(partitionSchema).zip(partitionSchema.map(_.name))
        .map( kv => s"${kv._1} -> ${kv._2}").mkString(","))
  }
}

case class NoSuchPermanentFunctionException(db: String, func: String)
  extends AnalysisException(s"Function '$func' not found in database '$db'")

case class NoSuchFunctionException(override val message: String)
  extends AnalysisException(message) {

  def this(db: String, func: String) = {
    this(s"Undefined function: '$func'. " +
        "This function is neither a registered temporary function nor " +
        s"a permanent function registered in the database '$db'.")
  }

  def this(identifier: Identifier) = {
    this(s"Undefined function: ${identifier.quoted}")
  }
}

case class NoSuchPartitionsException(override val message: String)
  extends AnalysisException(message) {

  def this(db: String, table: String, specs: Seq[TablePartitionSpec]) = {
    this(s"The following partitions not found in table '$table' database '$db':\n"
      + specs.mkString("\n===\n"))
  }

  def this(tableName: String, partitionIdents: Seq[InternalRow], partitionSchema: StructType) = {
    this(s"The following partitions not found in table $tableName: "
      + partitionIdents.map(_.toSeq(partitionSchema).zip(partitionSchema.map(_.name))
        .map( kv => s"${kv._1} -> ${kv._2}").mkString(",")).mkString("\n===\n"))
  }
}

case class NoSuchTempFunctionException(func: String)
  extends AnalysisException(s"Temporary function '$func' not found")

class NoSuchIndexException(message: String, cause: Option[Throwable] = None)
  extends AnalysisException(message, cause = cause)
