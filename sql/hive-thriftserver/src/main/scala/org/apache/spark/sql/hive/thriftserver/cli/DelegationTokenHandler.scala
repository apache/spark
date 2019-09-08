/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.cli

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.{Hive, HiveException}

import org.apache.spark.sql.hive.thriftserver.server.cli.SparkThriftServerSQLException

/**
 * Fetch Hive DelegationToken in a new Thread, avoid conflict of Hive Object.
 *
 * Since SparkSQLEnv will start in main thread, avoid conflict, we start a Hive
 * Token Handler in another thread.
 *
 * @param conf
 */
class DelegationTokenHandler(conf: HiveConf) extends Runnable {
  private var _hive: Hive = null
  private var _isStarted: Boolean = false

  override def run(): Unit = {
    Hive.closeCurrent()
    _hive = Hive.get(conf)
    _isStarted = true
  }

  def getDelegationToken(owner: String): String = {
    assert(_isStarted)
    _hive.getDelegationToken(owner, owner)
  }

  def cancelDelegationToken(tokenStr: String): Unit = {
    assert(_isStarted)
    try {
      _hive.cancelDelegationToken(tokenStr)
    } catch {
      case e: HiveException =>
        throw new SparkThriftServerSQLException("Couldn't cancel delegation token", e)
    }

  }
}
