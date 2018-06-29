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
package org.apache.spark.deploy.k8s.features.hadoopsteps

import org.apache.spark.deploy.k8s.security.KubernetesHadoopDelegationTokenManager
import org.apache.spark.internal.Logging

 /**
  * This step assumes that you have already done all the heavy lifting in retrieving a
  * delegation token and storing the following data in a secret before running this job.
  * This step requires that you just specify the secret name and data item-key corresponding
  * to the data where the delegation token is stored.
  */
private[spark] class HadoopKerberosSecretResolverStep(
  tokenSecretName: String,
  tokenItemKeyName: String,
  tokenManager: KubernetesHadoopDelegationTokenManager)
  extends HadoopConfigurationStep with Logging {

  override def configureHadoopSpec(hSpec: HadoopConfigSpec): HadoopConfigSpec = {
    hSpec.copy(
      dtSecret = None,
      dtSecretName = tokenSecretName,
      dtSecretItemKey = Some(tokenItemKeyName),
      jobUserName = Some(tokenManager.getCurrentUser.getShortUserName))
  }
}
