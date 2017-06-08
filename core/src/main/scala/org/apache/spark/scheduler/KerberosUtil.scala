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

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.internal.Logging

object KerberosUtil  extends Logging {
  var proxyUser : Option[UserGroupInformation] = None
  def securize (principal: String, keytab: String) : Unit = {
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(new SparkConf())
    hadoopConf.set("hadoop.security.authentication", "Kerberos")
    UserGroupInformation.setConfiguration(hadoopConf)
    UserGroupInformation.loginUserFromKeytab(principal, keytab)
  }


  def getHadoopDelegationTokens : Array[Byte] = {
    val ugi = proxyUser match {
      case Some(user) => user
      case None => UserGroupInformation.getLoginUser
    }
    val principal = ugi.getUserName
    val hadoopConf = SparkHadoopUtil.get.conf
    val namenodes = Set(FileSystem.get(hadoopConf).getHomeDirectory())
    logInfo(s"Found these HDFS namenodes: $namenodes")
    val ugiCreds = ugi.getCredentials
    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      override def run() = {
        // use the job principal itself to renew the tokens
        obtainTokensForNamenodes(namenodes, hadoopConf, ugiCreds, Some(principal))
      }
    })
    // write tokens into a memory file to transfer it to the executors
    val tokenBuf = new java.io.ByteArrayOutputStream(1024 * 1024)
    ugiCreds.writeTokenStorageToStream(new java.io.DataOutputStream(tokenBuf))
    logDebug(s"Wrote ${tokenBuf.size()} bytes of token data")

    hadoopConf.set("hadoop.security.authentication", "Kerberos")
    tokenBuf.toByteArray
  }
  def obtainTokensForNamenodes(
                                paths: Set[Path],
                                conf: Configuration,
                                creds: Credentials,
                                renewer: Option[String] = None
                              ): Unit = {
    if (UserGroupInformation.isSecurityEnabled()) {
      val delegTokenRenewer = renewer.getOrElse(getTokenRenewer(conf))
      paths.foreach { dst =>
        val dstFs = dst.getFileSystem(conf)
        logInfo("getting token for namenode: " + dst)
        dstFs.addDelegationTokens(delegTokenRenewer, creds)
      }
    }

  }
  def getTokenRenewer(conf: Configuration): String = {
    val delegTokenRenewer = Master.getMasterPrincipal(conf)
    logDebug("delegation token renewer is: " + delegTokenRenewer)
    if (delegTokenRenewer == null || delegTokenRenewer.length() == 0) {
      val errorMessage = "Can't get Master Kerberos principal for use as renewer"
      logError(errorMessage)
      throw new SparkException(errorMessage)
    }
    delegTokenRenewer
  }

   def useTokenAuth(tokens: Array[Byte]) {
     val sparkConf = SparkEnv.get.conf
     logInfo(s"Found delegation tokens of ${tokens.length} bytes")

     // configure to use tokens for HDFS login
     val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
     hadoopConf.set("hadoop.security.authentication", "Token")
     UserGroupInformation.setConfiguration(hadoopConf)

     // decode tokens and add them to the credentials
     val creds = UserGroupInformation.getCurrentUser.getCredentials
     val tokensBuf = new java.io.ByteArrayInputStream(tokens)
     creds.readTokenStorageStream(new java.io.DataInputStream(tokensBuf))
     UserGroupInformation.getCurrentUser.addCredentials(creds)
   }

}

