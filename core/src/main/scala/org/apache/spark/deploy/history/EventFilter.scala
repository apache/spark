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

package org.apache.spark.deploy.history

import org.apache.spark.scheduler._

private[spark] trait EventFilterBuilder extends SparkListenerInterface {
  def createFilter(): EventFilter
}

private[spark] trait EventFilter {
  def filterStageCompleted(event: SparkListenerStageCompleted): Option[Boolean] = None

  def filterStageSubmitted(event: SparkListenerStageSubmitted): Option[Boolean] = None

  def filterTaskStart(event: SparkListenerTaskStart): Option[Boolean] = None

  def filterTaskGettingResult(event: SparkListenerTaskGettingResult): Option[Boolean] = None

  def filterTaskEnd(event: SparkListenerTaskEnd): Option[Boolean] = None

  def filterJobStart(event: SparkListenerJobStart): Option[Boolean] = None

  def filterJobEnd(event: SparkListenerJobEnd): Option[Boolean] = None

  def filterEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Option[Boolean] = None

  def filterBlockManagerAdded(event: SparkListenerBlockManagerAdded): Option[Boolean] = None

  def filterBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Option[Boolean] = None

  def filterUnpersistRDD(event: SparkListenerUnpersistRDD): Option[Boolean] = None

  def filterApplicationStart(event: SparkListenerApplicationStart): Option[Boolean] = None

  def filterApplicationEnd(event: SparkListenerApplicationEnd): Option[Boolean] = None

  def filterExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Option[Boolean] = None

  def filterStageExecutorMetrics(event: SparkListenerStageExecutorMetrics): Option[Boolean] = None

  def filterExecutorAdded(event: SparkListenerExecutorAdded): Option[Boolean] = None

  def filterExecutorRemoved(event: SparkListenerExecutorRemoved): Option[Boolean] = None

  def filterExecutorBlacklisted(event: SparkListenerExecutorBlacklisted): Option[Boolean] = None

  def filterExecutorBlacklistedForStage(
      event: SparkListenerExecutorBlacklistedForStage): Option[Boolean] = None

  def filterNodeBlacklistedForStage(
      event: SparkListenerNodeBlacklistedForStage): Option[Boolean] = None

  def filterExecutorUnblacklisted(event: SparkListenerExecutorUnblacklisted): Option[Boolean] = None

  def filterNodeBlacklisted(event: SparkListenerNodeBlacklisted): Option[Boolean] = None

  def filterNodeUnblacklisted(event: SparkListenerNodeUnblacklisted): Option[Boolean] = None

  def filterBlockUpdated(event: SparkListenerBlockUpdated): Option[Boolean] = None

  def filterSpeculativeTaskSubmitted(
      event: SparkListenerSpeculativeTaskSubmitted): Option[Boolean] = None

  def filterOtherEvent(event: SparkListenerEvent): Option[Boolean] = None
}

