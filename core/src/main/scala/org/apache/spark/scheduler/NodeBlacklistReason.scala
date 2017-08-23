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

package org.apache.spark.scheduler

import org.apache.spark.annotation.DeveloperApi

/**
 * Represents an explanation for a Node being blacklisted for task scheduling
 */
@DeveloperApi
private[spark] sealed trait NodeBlacklistReason extends Serializable {
  def message: String
}

@DeveloperApi
private[spark] case class ExecutorFailures(blacklistedExecutors: Set[String])
  extends NodeBlacklistReason {
  override def message: String = "Maximum number of executor failures allowed on Node exceeded."
}

@DeveloperApi
private[spark] case object NodeDecommissioning extends NodeBlacklistReason {
  override def message: String = "Node is being decommissioned by Cluster Manager."
}

@DeveloperApi
private[spark] case class FetchFailure(host: String) extends NodeBlacklistReason {
  override def message: String = s"Fetch failure for host $host"
}

