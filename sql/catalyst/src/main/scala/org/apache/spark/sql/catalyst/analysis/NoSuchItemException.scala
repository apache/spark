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
import org.apache.spark.sql.catalog.v2.CatalogV2Implicits._
import org.apache.spark.sql.catalog.v2.Identifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec


/**
 * Thrown by a catalog when an item cannot be found. The analyzer will rethrow the exception
 * as an [[org.apache.spark.sql.AnalysisException]] with the correct position information.
 */
class NoSuchDatabaseException(
    val db: String) extends NoSuchNamespaceException(s"Database '$db' not found")

class NoSuchNamespaceException(message: String) extends AnalysisException(message) {
  def this(namespace: Array[String]) = {
    this(s"Namespace '${namespace.quoted}' not found")
  }
}

class NoSuchTableException(message: String) extends AnalysisException(message) {
  def this(db: String, table: String) = {
    this(s"Table or view '$table' not found in database '$db'")
  }

  def this(tableIdent: Identifier) = {
    this(s"Table ${tableIdent.quoted} not found")
  }
}

class NoSuchPartitionException(
    db: String,
    table: String,
    spec: TablePartitionSpec)
  extends AnalysisException(
    s"Partition not found in table '$table' database '$db':\n" + spec.mkString("\n"))

class NoSuchPermanentFunctionException(db: String, func: String)
  extends AnalysisException(s"Function '$func' not found in database '$db'")

class NoSuchFunctionException(db: String, func: String, cause: Option[Throwable] = None)
  extends AnalysisException(
    s"Undefined function: '$func'. This function is neither a registered temporary function nor " +
    s"a permanent function registered in the database '$db'.", cause = cause)

class NoSuchPartitionsException(db: String, table: String, specs: Seq[TablePartitionSpec])
  extends AnalysisException(
    s"The following partitions not found in table '$table' database '$db':\n"
      + specs.mkString("\n===\n"))

class NoSuchTempFunctionException(func: String)
  extends AnalysisException(s"Temporary function '$func' not found")
