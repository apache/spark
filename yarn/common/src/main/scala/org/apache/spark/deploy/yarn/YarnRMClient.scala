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

package org.apache.spark.deploy.yarn

import scala.collection.{Map, Set}

import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.api.records._

import org.apache.spark.{SecurityManager, SparkConf, SparkContext}
import org.apache.spark.scheduler.SplitInfo

/**
 * Interface that defines a Yarn RM client. Abstracts away Yarn version-specific functionality that
 * is used by Spark's AM.
 */
trait YarnRMClient {

  /**
   * Registers the application master with the RM.
   *
   * @param conf The Yarn configuration.
   * @param sparkConf The Spark configuration.
   * @param preferredNodeLocations Map with hints about where to allocate containers.
   * @param uiAddress Address of the SparkUI.
   * @param uiHistoryAddress Address of the application on the History Server.
   */
  def register(
      conf: YarnConfiguration,
      sparkConf: SparkConf,
      preferredNodeLocations: Map[String, Set[SplitInfo]],
      uiAddress: String,
      uiHistoryAddress: String,
      securityMgr: SecurityManager): YarnAllocator

  /**
   * Shuts down the AM. Guaranteed to only be called once.
   *
   * @param status The final status of the AM.
   * @param diagnostics Diagnostics message to include in the final status.
   */
  def shutdown(status: FinalApplicationStatus, diagnostics: String = ""): Unit

  /** Returns the attempt ID. */
  def getAttemptId(): ApplicationAttemptId

  /** Returns the RM's proxy host and port. */
  def getProxyHostAndPort(conf: YarnConfiguration): String

  /** Returns the maximum number of attempts to register the AM. */
  def getMaxRegAttempts(conf: YarnConfiguration): Int

}
