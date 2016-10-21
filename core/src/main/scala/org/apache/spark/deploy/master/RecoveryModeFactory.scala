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

package org.apache.spark.deploy.master

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.Serializer

/**
 * ::DeveloperApi::
 *
 * Implementation of this class can be plugged in as recovery mode alternative for Spark's
 * Standalone mode.
 *
 */
@DeveloperApi
abstract class StandaloneRecoveryModeFactory(conf: SparkConf, serializer: Serializer) {

  /**
   * PersistenceEngine defines how the persistent data(Information about worker, driver etc..)
   * is handled for recovery.
   *
   */
  def createPersistenceEngine(): PersistenceEngine

  /**
   * Create an instance of LeaderAgent that decides who gets elected as master.
   */
  def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent
}

/**
 * LeaderAgent in this case is a no-op. Since leader is forever leader as the actual
 * recovery is made by restoring from filesystem.
 */
private[master] class FileSystemRecoveryModeFactory(conf: SparkConf, serializer: Serializer)
  extends StandaloneRecoveryModeFactory(conf, serializer) with Logging {

  val RECOVERY_DIR = conf.get("spark.deploy.recoveryDirectory", "")

  def createPersistenceEngine(): PersistenceEngine = {
    logInfo("Persisting recovery state to directory: " + RECOVERY_DIR)
    new FileSystemPersistenceEngine(RECOVERY_DIR, serializer)
  }

  def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent = {
    new MonarchyLeaderAgent(master)
  }
}

private[master] class ZooKeeperRecoveryModeFactory(conf: SparkConf, serializer: Serializer)
  extends StandaloneRecoveryModeFactory(conf, serializer) {

  def createPersistenceEngine(): PersistenceEngine = {
    new ZooKeeperPersistenceEngine(conf, serializer)
  }

  def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent = {
    new ZooKeeperLeaderElectionAgent(master, conf)
  }
}
