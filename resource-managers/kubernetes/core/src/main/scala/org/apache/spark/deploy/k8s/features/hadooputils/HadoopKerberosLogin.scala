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
package org.apache.spark.deploy.k8s.features.hadooputils

import io.fabric8.kubernetes.api.model.SecretBuilder
import org.apache.commons.codec.binary.Base64

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.security.KubernetesHadoopDelegationTokenManager

/**
 * This logic does all the heavy lifting for Delegation Token creation. This step
 * assumes that the job user has either specified a principal and keytab or ran
 * $kinit before running spark-submit. By running UGI.getCurrentUser we are able
 * to obtain the current user, either signed in via $kinit or keytab. With the
 * Job User principal you then retrieve the delegation token from the NameNode
 * and store values in DelegationToken. Lastly, the class puts the data into
 * a secret. All this is defined in a KerberosConfigSpec.
 */
private[spark] object HadoopKerberosLogin {
  def buildSpec(
      submissionSparkConf: SparkConf,
      kubernetesResourceNamePrefix: String,
      tokenManager: KubernetesHadoopDelegationTokenManager): KerberosConfigSpec = {
    // The JobUserUGI will be taken fom the Local Ticket Cache or via keytab+principal
    // The login happens in the SparkSubmit so login logic is not necessary to include
    val jobUserUGI = tokenManager.getCurrentUser
    val originalCredentials = jobUserUGI.getCredentials
    tokenManager.obtainDelegationTokens(originalCredentials)

    val tokenData = SparkHadoopUtil.get.serialize(originalCredentials)

    val initialTokenDataKeyName = KERBEROS_SECRET_KEY
    val newSecretName = s"$kubernetesResourceNamePrefix-$KERBEROS_DELEGEGATION_TOKEN_SECRET_NAME"
    val secretDT =
      new SecretBuilder()
        .withNewMetadata()
          .withName(newSecretName)
          .endMetadata()
        .addToData(initialTokenDataKeyName, Base64.encodeBase64String(tokenData))
        .build()
    KerberosConfigSpec(
      dtSecret = Some(secretDT),
      dtSecretName = newSecretName,
      dtSecretItemKey = initialTokenDataKeyName,
      jobUserName = jobUserUGI.getShortUserName)
  }
}
