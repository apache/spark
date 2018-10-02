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
package org.apache.spark.scheduler.cluster.mesos

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.security.AbstractCredentialRenewer
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.rpc.RpcEndpointRef

/**
 * Mesos-specific implementation of AbstractCredentialRenewer.
 */
private[spark] class MesosHadoopDelegationTokenManager(
    _sparkConf: SparkConf,
    _hadoopConf: Configuration)
  extends AbstractCredentialRenewer(_sparkConf, _hadoopConf) {

  private val tokenManager = new HadoopDelegationTokenManager(sparkConf, hadoopConf)

  def start(driverEndpoint: RpcEndpointRef): Unit = {
    require(driverEndpoint != null, "DriverEndpoint is not initialized")
    setDriverRef(driverEndpoint)
    if (renewalEnabled) {
      super.start()
    } else {
      logInfo("Using ticket cache for Kerberos authentication, no token renewal.")
      createAndUpdateTokens()
    }
  }

  override protected def obtainDelegationTokens(creds: Credentials): Long = {
    tokenManager.obtainDelegationTokens(hadoopConf, creds)
  }

}
