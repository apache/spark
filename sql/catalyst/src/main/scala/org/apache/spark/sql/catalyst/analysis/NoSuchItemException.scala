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

import org.apache.spark.SparkThrowableHelper
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.util.{quoteIdentifier, quoteNameParts}
import org.apache.spark.sql.types.StructType

// any changes to this class should be backward compatible as it may be used by external connectors
class NoSuchPartitionException private(
    message: String,
    errorClass: Option[String],
    messageParameters: Map[String, String])
  extends AnalysisException(
    message,
    errorClass = errorClass,
    messageParameters = messageParameters) {

  def this(errorClass: String, messageParameters: Map[String, String]) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      Some(errorClass),
      messageParameters)
  }

  def this(db: String, table: String, spec: TablePartitionSpec) = {
    this(errorClass = "PARTITIONS_NOT_FOUND",
      Map("partitionList" ->
        ("PARTITION (" +
          spec.map( kv => quoteIdentifier(kv._1) + s" = ${kv._2}").mkString(", ") + ")"),
        "tableName" -> (quoteIdentifier(db) + "." + quoteIdentifier(table))))
  }

  def this(tableName: String, partitionIdent: InternalRow, partitionSchema: StructType) = {
    this(errorClass = "PARTITIONS_NOT_FOUND",
      Map("partitionList" ->
        ("PARTITION (" + partitionIdent.toSeq(partitionSchema).zip(partitionSchema.map(_.name))
        .map( kv => quoteIdentifier(s"${kv._2}") + s" = ${kv._1}").mkString(", ") + ")"),
        "tableName" -> quoteNameParts(UnresolvedAttribute.parseAttributeName(tableName))))
  }
}

// any changes to this class should be backward compatible as it may be used by external connectors
class NoSuchPartitionsException private(
    message: String,
    errorClass: Option[String],
    messageParameters: Map[String, String])
  extends AnalysisException(
    message,
    errorClass = errorClass,
    messageParameters = messageParameters) {

  def this(errorClass: String, messageParameters: Map[String, String]) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      Some(errorClass),
      messageParameters)
  }

  def this(db: String, table: String, specs: Seq[TablePartitionSpec]) = {
    this(errorClass = "PARTITIONS_NOT_FOUND",
      Map("partitionList" -> ("PARTITION (" +
        specs.map(spec => spec.map(kv => quoteIdentifier(kv._1) + s" = ${kv._2}").mkString(", "))
        .mkString("), PARTITION (") + ")"),
        "tableName" -> (quoteIdentifier(db) + "." + quoteIdentifier(table))))
  }

  def this(tableName: String, partitionIdents: Seq[InternalRow], partitionSchema: StructType) = {
    this(errorClass = "PARTITIONS_NOT_FOUND",
      Map("partitionList" -> ("PARTITION (" +
        partitionIdents.map(_.toSeq(partitionSchema).zip(partitionSchema.map(_.name))
          .map( kv => quoteIdentifier(s"${kv._2}") + s" = ${kv._1}")
          .mkString(", ")).mkString("), PARTITION (") + ")"),
        "tableName" -> quoteNameParts(UnresolvedAttribute.parseAttributeName(tableName))))
  }
}
