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

import org.apache.commons.lang3.StringUtils

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.util.Utils

/**
 * A trait that can be used by data source v2 related query plans(both logical and physical), to
 * provide a string format of the data source information for explain.
 */
trait DataSourceV2StringFormat {

  /**
   * The instance of this data source implementation. Note that we only consider its class in
   * equals/hashCode, not the instance itself.
   */
  def source: DataSourceV2

  /**
   * The output of the data source reader, w.r.t. column pruning.
   */
  def output: Seq[Attribute]

  /**
   * The options for this data source reader.
   */
  def options: Map[String, String]

  /**
   * The filters which have been pushed to the data source.
   */
  def pushedFilters: Seq[Expression]

  private def sourceName: String = source match {
    case registered: DataSourceRegister => registered.shortName()
    // source.getClass.getSimpleName can cause Malformed class name error,
    // call safer `Utils.getSimpleName` instead
    case _ => Utils.getSimpleName(source.getClass)
  }

  def metadataString: String = {
    val entries = scala.collection.mutable.ArrayBuffer.empty[(String, String)]

    if (pushedFilters.nonEmpty) {
      entries += "Filters" -> pushedFilters.mkString("[", ", ", "]")
    }

    // TODO: we should only display some standard options like path, table, etc.
    if (options.nonEmpty) {
      entries += "Options" -> Utils.redact(options).map {
        case (k, v) => s"$k=$v"
      }.mkString("[", ",", "]")
    }

    val outputStr = Utils.truncatedString(output, "[", ", ", "]")

    val entriesStr = if (entries.nonEmpty) {
      Utils.truncatedString(entries.map {
        case (key, value) => key + ": " + StringUtils.abbreviate(value, 100)
      }, " (", ", ", ")")
    } else {
      ""
    }

    s"$sourceName$outputStr$entriesStr"
  }
}
