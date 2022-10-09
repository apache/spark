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
 * Thrown by a catalog when an item already exists. The analyzer will rethrow the exception
 * as an [[org.apache.spark.sql.AnalysisException]] with the correct position information.
 */
class DatabaseAlreadyExistsException(db: String)
  extends NamespaceAlreadyExistsException(s"Database '$db' already exists")

class NamespaceAlreadyExistsException(message: String)
  extends AnalysisException(
    message,
    errorClass = Some("_LEGACY_ERROR_TEMP_1118"),
    messageParameters = Map("msg" -> message)) {
  def this(namespace: Array[String]) = {
    this(s"Namespace '${namespace.quoted}' already exists")
  }
}

class TableAlreadyExistsException(message: String, cause: Option[Throwable] = None)
  extends AnalysisException(
    message,
    errorClass = Some("_LEGACY_ERROR_TEMP_1116"),
    messageParameters = Map("msg" -> message),
    cause = cause) {
  def this(db: String, table: String) = {
    this(s"Table or view '$table' already exists in database '$db'")
  }

  def this(tableIdent: Identifier) = {
    this(s"Table ${tableIdent.quoted} already exists")
  }
}

class TempTableAlreadyExistsException(table: String)
  extends TableAlreadyExistsException(s"Temporary view '$table' already exists")

class PartitionAlreadyExistsException(message: String) extends AnalysisException(message) {
  def this(db: String, table: String, spec: TablePartitionSpec) = {
    this(s"Partition already exists in table '$table' database '$db':\n" + spec.mkString("\n"))
  }

  def this(tableName: String, partitionIdent: InternalRow, partitionSchema: StructType) = {
    this(s"Partition already exists in table $tableName:" +
      partitionIdent.toSeq(partitionSchema).zip(partitionSchema.map(_.name))
        .map( kv => s"${kv._1} -> ${kv._2}").mkString(","))
  }
}

class PartitionsAlreadyExistException(message: String) extends AnalysisException(message) {
  def this(db: String, table: String, specs: Seq[TablePartitionSpec]) = {
    this(s"The following partitions already exists in table '$table' database '$db':\n"
      + specs.mkString("\n===\n"))
  }

  def this(tableName: String, partitionIdents: Seq[InternalRow], partitionSchema: StructType) = {
    this(s"The following partitions already exists in table $tableName:" +
      partitionIdents.map(id => partitionSchema.map(_.name).zip(id.toSeq(partitionSchema))
        .map( kv => s"${kv._1} -> ${kv._2}").mkString(",")).mkString("\n===\n"))
  }
}

class FunctionAlreadyExistsException(db: String, func: String)
  extends AnalysisException(s"Function '$func' already exists in database '$db'")

class IndexAlreadyExistsException(message: String, cause: Option[Throwable] = None)
  extends AnalysisException(message, cause = cause)
