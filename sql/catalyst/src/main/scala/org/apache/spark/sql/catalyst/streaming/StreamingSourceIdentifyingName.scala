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

package org.apache.spark.sql.catalyst.streaming

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * A trait for logical plans that have a streaming source identifying name.
 *
 * This trait provides a common interface for both V1 (StreamingRelation) and V2
 * (StreamingRelationV2) streaming sources, allowing analyzer rules in sql/catalyst
 * to uniformly handle source naming without module boundary issues.
 *
 * The self-type constraint ensures this trait can only be mixed into LogicalPlan subclasses.
 */
trait HasStreamingSourceIdentifyingName { self: LogicalPlan =>
  def sourceIdentifyingName: StreamingSourceIdentifyingName
  def withSourceIdentifyingName(name: StreamingSourceIdentifyingName): LogicalPlan
}

/**
 * Represents the identifying name state for a streaming source during query analysis.
 *
 * Source names can be:
 * - User-provided via the `.name()` API
 * - Flow-assigned by external systems (e.g., SDP)
 * - Unassigned, to be auto-generated during analysis
 */
sealed trait StreamingSourceIdentifyingName {
  override def toString: String = this match {
    case UserProvided(name) => s"""name="$name""""
    case FlowAssigned(name) => s"""name="$name""""
    case Unassigned => "name=<Unassigned>"
  }
}

/**
 * A source name explicitly provided by the user via the `.name()` API.
 * Takes highest precedence.
 */
case class UserProvided(name: String) extends StreamingSourceIdentifyingName

/**
 * A source name assigned by an external flow system (e.g., SDP).
 * Used when the source is part of a managed pipeline.
 */
case class FlowAssigned(name: String) extends StreamingSourceIdentifyingName

/**
 * No name has been assigned yet. The analyzer will auto-generate one if needed.
 */
case object Unassigned extends StreamingSourceIdentifyingName
