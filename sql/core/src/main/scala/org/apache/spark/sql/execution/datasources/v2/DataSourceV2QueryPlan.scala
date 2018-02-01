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

package org.apache.spark.sql.execution.datasources.v2

import java.util.Objects

import org.apache.commons.lang3.StringUtils

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.util.Utils

/**
 * A base class for data source v2 related query plan(both logical and physical). It defines the
 * equals/hashCode methods according to some common information.
 */
trait DataSourceV2QueryPlan {

  def output: Seq[Attribute]
  def sourceClass: Class[_ <: DataSourceV2]
  def filters: Set[Expression]

  // The metadata of this data source relation that can be used for equality test.
  private def metadata: Seq[Any] = Seq(output, sourceClass, filters)

  def canEqual(other: Any): Boolean

  override def equals(other: Any): Boolean = other match {
    case other: DataSourceV2QueryPlan =>
      canEqual(other) && metadata == other.metadata
    case _ => false
  }

  override def hashCode(): Int = {
    metadata.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
  }

  def metadataString: String = {
    val entries = scala.collection.mutable.ArrayBuffer.empty[(String, String)]
    if (filters.nonEmpty) entries += "PushedFilter" -> filters.mkString("[", ", ", "]")

    val outputStr = Utils.truncatedString(output, "[", ", ", "]")
    val entriesStr = Utils.truncatedString(entries.map {
      case (key, value) => key + ": " + StringUtils.abbreviate(redact(value), 100)
    }, " (", ", ", ")")

    s"${sourceClass.getSimpleName}$outputStr$entriesStr"
  }

  private def redact(text: String): String = {
    Utils.redact(SQLConf.get.stringRedationPattern, text)
  }
}
