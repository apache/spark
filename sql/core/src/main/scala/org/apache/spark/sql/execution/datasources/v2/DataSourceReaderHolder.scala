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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.sources.v2.reader._

/**
 * A base class for data source reader holder with customized equals/hashCode methods.
 */
trait DataSourceReaderHolder {

  /**
   * The full output of the data source reader, without column pruning.
   */
  def fullOutput: Seq[AttributeReference]

  /**
   * The held data source reader.
   */
  def reader: DataSourceV2Reader

  /**
   * The metadata of this data source reader that can be used for equality test.
   */
  private def metadata: Seq[Any] = {
    val filters: Any = reader match {
      case s: SupportsPushDownCatalystFilters => s.pushedCatalystFilters().toSet
      case s: SupportsPushDownFilters => s.pushedFilters().toSet
      case _ => Nil
    }
    Seq(fullOutput, reader.getClass, reader.readSchema(), filters)
  }

  def canEqual(other: Any): Boolean

  override def equals(other: Any): Boolean = other match {
    case other: DataSourceReaderHolder =>
      canEqual(other) && metadata.length == other.metadata.length &&
        metadata.zip(other.metadata).forall { case (l, r) => l == r }
    case _ => false
  }

  override def hashCode(): Int = {
    metadata.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
  }

  lazy val output: Seq[Attribute] = reader.readSchema().map(_.name).map { name =>
    fullOutput.find(_.name == name).get
  }
}
