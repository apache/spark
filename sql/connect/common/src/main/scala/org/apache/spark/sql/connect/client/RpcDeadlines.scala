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

package org.apache.spark.sql.connect.client

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
 * Per-RPC deadline configuration. Each field controls the deadline for one gRPC call. Set a field
 * to None to disable the deadline for that call. Use RpcDeadlines.disabled to create an instance
 * with all deadlines disabled.
 *
 * Note on reattachableExecutePlan and reattachExecute: these deadlines apply to each individual
 * gRPC stream segment, not to the overall query execution lifetime. When a deadline fires, the
 * server-side operation continues running; the client simply opens a new ReattachExecute stream
 * to resume receiving results. This avoids hanging connections while preserving execution.
 *
 * Non-reattachable ExecutePlan has no deadline because a timeout there would kill the execution
 * with no recovery path. ReleaseExecute has no deadline (fire-and-forget cleanup).
 */
private[sql] case class RpcDeadlines(
    reattachableExecutePlan: Option[FiniteDuration] = Some(10.minutes),
    reattachExecute: Option[FiniteDuration] = Some(10.minutes),
    analyzePlan: Option[FiniteDuration] = Some(1.hour),
    addArtifacts: Option[FiniteDuration] = Some(1.hour),
    config: Option[FiniteDuration] = Some(10.minutes),
    interrupt: Option[FiniteDuration] = Some(10.minutes),
    releaseSession: Option[FiniteDuration] = Some(10.minutes),
    artifactStatus: Option[FiniteDuration] = Some(10.minutes),
    cloneSession: Option[FiniteDuration] = Some(10.minutes),
    getStatus: Option[FiniteDuration] = Some(10.minutes),
    fetchErrorDetails: Option[FiniteDuration] = Some(10.minutes)) {

  // Validate all fields: each must be a positive duration or None.
  private val namedFields: Seq[(String, Option[FiniteDuration])] =
    productElementNames.toSeq.zip(
      productIterator.map(_.asInstanceOf[Option[FiniteDuration]]).toSeq)

  namedFields.foreach { case (name, opt) =>
    opt.foreach(d =>
      require(d.toMillis > 0, s"RpcDeadlines.$name must be a positive duration, got $d"))
  }

  override def toString: String = {
    val configured = namedFields.collect { case (name, Some(d)) => s"$name=$d" }
    if (configured.isEmpty) "RpcDeadlines(all disabled)"
    else s"RpcDeadlines(${configured.mkString(", ")})"
  }
}

private[sql] object RpcDeadlines {

  /**
   * Creates an RpcDeadlines with all deadlines disabled (no per-RPC timeout on any call). Use
   * this when you want to rely solely on server-side or network-layer timeouts.
   */
  val disabled: RpcDeadlines = RpcDeadlines(
    reattachableExecutePlan = None,
    reattachExecute = None,
    analyzePlan = None,
    addArtifacts = None,
    config = None,
    interrupt = None,
    releaseSession = None,
    artifactStatus = None,
    cloneSession = None,
    getStatus = None,
    fetchErrorDetails = None)
}
