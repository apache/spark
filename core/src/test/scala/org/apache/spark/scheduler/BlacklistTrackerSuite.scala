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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config

class BlacklistTrackerSuite extends SparkFunSuite {

  test("blacklist still respects legacy configs") {
    val conf = new SparkConf().setMaster("local")
    assert(!BlacklistTracker.isBlacklistEnabled(conf))
    conf.set(config.BLACKLIST_LEGACY_TIMEOUT_CONF, 5000L)
    assert(BlacklistTracker.isBlacklistEnabled(conf))
    assert(5000 === BlacklistTracker.getBlacklistTimeout(conf))
    // the new conf takes precedence, though
    conf.set(config.BLACKLIST_TIMEOUT_CONF, 1000L)
    assert(1000 === BlacklistTracker.getBlacklistTimeout(conf))

    // if you explicitly set the legacy conf to 0, that also would disable blacklisting
    conf.set(config.BLACKLIST_LEGACY_TIMEOUT_CONF, 0L)
    assert(!BlacklistTracker.isBlacklistEnabled(conf))
    // but again, the new conf takes precendence
    conf.set(config.BLACKLIST_ENABLED, true)
    assert(BlacklistTracker.isBlacklistEnabled(conf))
    assert(1000 === BlacklistTracker.getBlacklistTimeout(conf))
  }

  test("check blacklist configuration invariants") {
    val conf = new SparkConf().setMaster("yarn-cluster")
    Seq(
      (2, 2),
      (2, 3)
    ).foreach { case (maxTaskFailures, maxNodeAttempts) =>
      conf.set(config.MAX_TASK_FAILURES, maxTaskFailures)
      conf.set(config.MAX_TASK_ATTEMPTS_PER_NODE.key, maxNodeAttempts.toString)
      val excMsg = intercept[IllegalArgumentException] {
        BlacklistTracker.validateBlacklistConfs(conf)
      }.getMessage()
      assert(excMsg === s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key} " +
        s"( = ${maxNodeAttempts}) was >= ${config.MAX_TASK_FAILURES.key} " +
        s"( = ${maxTaskFailures} ).  Though blacklisting is enabled, with this configuration, " +
        s"Spark will not be robust to one bad node.  Decrease " +
        s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key}, increase ${config.MAX_TASK_FAILURES.key}, " +
        s"or disable blacklisting with ${config.BLACKLIST_ENABLED.key}")
    }

    conf.remove(config.MAX_TASK_FAILURES)
    conf.remove(config.MAX_TASK_ATTEMPTS_PER_NODE)

    Seq(
      config.MAX_TASK_ATTEMPTS_PER_EXECUTOR,
      config.MAX_TASK_ATTEMPTS_PER_NODE,
      config.MAX_FAILURES_PER_EXEC_STAGE,
      config.MAX_FAILED_EXEC_PER_NODE_STAGE,
      config.BLACKLIST_TIMEOUT_CONF
    ).foreach { config =>
      conf.set(config.key, "0")
      val excMsg = intercept[IllegalArgumentException] {
        BlacklistTracker.validateBlacklistConfs(conf)
      }.getMessage()
      assert(excMsg.contains(s"${config.key} was 0, but must be > 0."))
      conf.remove(config)
    }
  }
}
