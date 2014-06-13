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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference}
import org.apache.spark.sql.catalyst.types.StringType

/**
 * A logical node that represents a non-query command to be executed by the system.  For example,
 * commands can be used by parsers to represent DDL operations.
 */
abstract class Command extends LeafNode {
  self: Product =>
  def output: Seq[Attribute] = Seq.empty
}

/**
 * Returned for commands supported by a given parser, but not catalyst.  In general these are DDL
 * commands that are passed directly to another system.
 */
case class NativeCommand(cmd: String) extends Command {
  override def output =
    Seq(BoundReference(0, AttributeReference("result", StringType, nullable = false)()))
}

/**
 * Commands of the form "SET (key) (= value)".
 */
case class SetCommand(key: Option[String], value: Option[String]) extends Command {
  override def output = Seq(
    BoundReference(0, AttributeReference("key", StringType, nullable = false)()),
    BoundReference(1, AttributeReference("value", StringType, nullable = false)()))
}

/**
 * Returned by a parser when the users only wants to see what query plan would be executed, without
 * actually performing the execution.
 */
case class ExplainCommand(plan: LogicalPlan) extends Command {
  override def output =
    Seq(BoundReference(0, AttributeReference("plan", StringType, nullable = false)()))
}

/**
 * Returned for the "CACHE TABLE tableName" and "UNCACHE TABLE tableName" command.
 */
case class CacheCommand(tableName: String, doCache: Boolean) extends Command
