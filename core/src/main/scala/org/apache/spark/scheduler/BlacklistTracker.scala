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

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[scheduler] object BlacklistTracker extends Logging {

  private val DEFAULT_TIMEOUT = "1h"

  /**
   * Returns true if the blacklist is enabled, based on checking the configuration in the following
   * order:
   * 1. Is it specifically enabled or disabled?
   * 2. Is it enabled via the legacy timeout conf?
   * 3. Use the default for the spark-master:
   *   - off for local mode
   *   - on for distributed modes (including local-cluster)
   */
  def isBlacklistEnabled(conf: SparkConf): Boolean = {
    conf.get(config.BLACKLIST_ENABLED) match {
      case Some(isEnabled) =>
        isEnabled
      case None =>
        // if they've got a non-zero setting for the legacy conf, always enable the blacklist,
        // otherwise, use the default based on the cluster-mode (off for local-mode, on otherwise).
        val legacyKey = config.BLACKLIST_LEGACY_TIMEOUT_CONF.key
        conf.get(config.BLACKLIST_LEGACY_TIMEOUT_CONF) match {
          case Some(legacyTimeout) =>
            if (legacyTimeout == 0) {
              logWarning(s"Turning off blacklisting due to legacy configuaration:" +
                s" $legacyKey == 0")
              false
            } else {
              // mostly this is necessary just for tests, since real users that want the blacklist
              // will get it anyway by default
              logWarning(s"Turning on blacklisting due to legacy configuration:" +
                s" $legacyKey > 0")
              true
            }
          case None =>
            // local-cluster is *not* considered local for these purposes, we still want the
            // blacklist enabled by default
            !Utils.isLocalMaster(conf)
        }
    }
  }

  def getBlacklistTimeout(conf: SparkConf): Long = {
    conf.get(config.BLACKLIST_TIMEOUT_CONF).getOrElse {
      conf.get(config.BLACKLIST_LEGACY_TIMEOUT_CONF).getOrElse {
        Utils.timeStringAsMs(DEFAULT_TIMEOUT)
      }
    }
  }

  /**
   * Verify that blacklist configurations are consistent; if not, throw an exception.  Should only
   * be called if blacklisting is enabled.
   *
   * The configuration for the blacklist is expected to adhere to a few invariants.  Default
   * values follow these rules of course, but users may unwittingly change one configuration
   * without making the corresponding adjustment elsewhere.  This ensures we fail-fast when
   * there are such misconfigurations.
   */
  def validateBlacklistConfs(conf: SparkConf): Unit = {

    def mustBePos(k: String, v: String): Unit = {
      throw new IllegalArgumentException(s"$k was $v, but must be > 0.")
    }

    // undocumented escape hatch for validation -- just for tests that want to run in an "unsafe"
    // configuration.
    if (!conf.get("spark.blacklist.testing.skipValidation", "false").toBoolean) {

      Seq(
        config.MAX_TASK_ATTEMPTS_PER_EXECUTOR,
        config.MAX_TASK_ATTEMPTS_PER_NODE,
        config.MAX_FAILURES_PER_EXEC_STAGE,
        config.MAX_FAILED_EXEC_PER_NODE_STAGE
      ).foreach { config =>
        val v = conf.get(config)
        if (v <= 0) {
          mustBePos(config.key, v.toString)
        }
      }

      val timeout = getBlacklistTimeout(conf)
      if (timeout <= 0) {
        // first, figure out where the timeout came from, to include the right conf in the message.
        conf.get(config.BLACKLIST_TIMEOUT_CONF) match {
          case Some(t) =>
            mustBePos(config.BLACKLIST_TIMEOUT_CONF.key, timeout.toString)
          case None =>
            mustBePos(config.BLACKLIST_LEGACY_TIMEOUT_CONF.key, timeout.toString)
        }
      }

      val maxTaskFailures = conf.getInt("spark.task.maxFailures", 4)
      val maxNodeAttempts = conf.get(config.MAX_TASK_ATTEMPTS_PER_NODE)

      if (maxNodeAttempts >= maxTaskFailures) {
        throw new IllegalArgumentException(s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key} " +
          s"( = ${maxNodeAttempts}) was >= spark.task.maxFailures " +
          s"( = ${maxTaskFailures} ).  Though blacklisting is enabled, with this configuration, " +
          s"Spark will not be robust to one bad node.  Decrease " +
          s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key}, increase spark.task.maxFailures, or " +
          s"disable blacklisting with ${config.BLACKLIST_ENABLED.key}")
      }
    }
  }
}
