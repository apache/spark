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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * A logical plan node that contains exactly what was parsed from SQL.
 *
 * This is used to hold information parsed from SQL when there are multiple implementations of a
 * query or command. For example, CREATE TABLE may be implemented by different nodes for v1 and v2.
 * Instead of parsing directly to a v1 CreateTable that keeps metadata in CatalogTable, and then
 * converting that v1 metadata to the v2 equivalent, the sql [[CreateTableStatement]] plan is
 * produced by the parser and converted once into both implementations.
 *
 * Parsed logical plans are not resolved because they must be converted to concrete logical plans.
 *
 * Parsed logical plans are located in Catalyst so that as much SQL parsing logic as possible is be
 * kept in a [[org.apache.spark.sql.catalyst.parser.AbstractSqlParser]].
 */
private[sql] abstract class ParsedStatement extends LogicalPlan {
  // Redact properties and options when parsed nodes are used by generic methods like toString
  override def productIterator: Iterator[Any] = super.productIterator.map {
    case mapArg: Map[_, _] => conf.redactOptions(mapArg.asInstanceOf[Map[String, String]])
    case other => other
  }

  final override lazy val resolved = false
}
