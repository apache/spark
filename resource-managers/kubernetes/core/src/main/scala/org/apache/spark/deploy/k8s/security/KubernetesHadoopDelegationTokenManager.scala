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

package org.apache.spark.deploy.k8s.security

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.internal.Logging
import org.apache.spark.util.{Clock, SystemClock}

 /**
  * The KubernetesHadoopDelegationTokenManager fetches and updates Hadoop delegation tokens
  * on the behalf of the Kubernetes submission client. It is modeled after the YARN
  * AMCredentialRenewer, renewals in Kubernetes happen in a seperate microservice that will
  * automatically update the Tokens via Kubernetes Secrets. The principal difference is that
  * instead of writing the new credentials to HDFS and incrementing the timestamp of the file,
  * the new credentials (called Tokens when they are serialized) are stored in Secrets accessible
  * to the driver and executors, when new Tokens are received they overwrite the current Secrets.
  */
private[spark] class KubernetesHadoopDelegationTokenManager extends Logging {

   // HadoopUGI Util methods
   private val clock: Clock = new SystemClock()
   def getCurrentUser: UserGroupInformation = UserGroupInformation.getCurrentUser
   def getShortUserName : String = getCurrentUser.getShortUserName
   def getFileSystem(hadoopConf: Configuration) : FileSystem = FileSystem.get(hadoopConf)
   def isSecurityEnabled: Boolean = UserGroupInformation.isSecurityEnabled
   def loginUserFromKeytabAndReturnUGI(principal: String, keytab: String): UserGroupInformation =
     UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
   def getCurrentTime: Long = clock.getTimeMillis()
   def serializeCreds(creds: Credentials): Array[Byte] = SparkHadoopUtil.get.serialize(creds)
   def nextRT(rt: Long, conf: SparkConf): Long = SparkHadoopUtil.nextCredentialRenewalTime(rt, conf)

   // Grab files in the HADOOP_CONF directory
   def getHadoopConfFiles(path: String) : Seq[File] = {
     val dir = new File(path)
     if (dir.isDirectory) {
       dir.listFiles.flatMap { file => Some(file).filter(_.isFile) }.toSeq
     } else {
       Seq.empty[File]
     }
   }

   // Principle method in charge of retrieving new Delegation Tokens
   def getDelegationTokens(
    creds: Credentials,
    conf: SparkConf,
    hadoopConf: Configuration,
    tokenManager: HadoopDelegationTokenManager): (Array[Byte], Long) = {
    try {
      val rt = tokenManager.obtainDelegationTokens(hadoopConf, creds)
      logDebug(s"Initialized tokens: ${SparkHadoopUtil.get.dumpTokens(creds)}")
      (serializeCreds(creds), nextRT(rt, conf))
    } catch {
      case e: Exception =>
        logError(s"Failed to fetch Hadoop delegation tokens $e")
        throw e
    }
  }
}
