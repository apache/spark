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

package org.apache.spark.sql.hive.thriftserver.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

class ThriftServerHDFSDelegationTokenProvider(sparkConf: SparkConf,
                                              hadoopConf: Configuration) extends Logging {

  def obtainDelegationTokens(creds: Credentials, renewer: String): Unit = {
    val fsToGetTokens = ThriftServerHadoopUtils.hadoopFSsToAccess(sparkConf, hadoopConf)
    fetchDelegationTokens(renewer, fsToGetTokens, creds)
  }

  def delegationTokensRequired(sparkConf: SparkConf,
                               hadoopConf: Configuration): Boolean = {
    UserGroupInformation.isSecurityEnabled
  }

  private def fetchDelegationTokens(renewer: String,
                                    filesystems: Set[FileSystem],
                                    creds: Credentials): Credentials = {
    filesystems.foreach { fs =>
      logInfo("getting token for: " + fs)
      fs.addDelegationTokens(renewer, creds)
    }
    creds
  }
}
