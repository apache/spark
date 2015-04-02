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

package org.apache.spark.deploy

import java.security.PrivilegedAction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkEnv, Logging}
import sun.security.jgss.krb5.Krb5InitCredential

import scala.sys.process.Process

private[spark] class StandaloneSparkHadoopUtil extends SparkHadoopUtil {

  val hadoopSecurityAuthenticationKey = "spark.hadoop.security.authentication"
  val principalKey = "spark.hadoop.dfs.namenode.kerberos.principal"
  val keytabKey = "spark.hadoop.dfs.namenode.keytab.file"
  val securityEnabledButKeyNotDefined = "Hadoop security was enabled, but %s" +
    "was not set in the Spark configuration."

  // Lazily evaluated upon invoking loginAsSparkUserAndReturnUGI()
  lazy val loggedInUser: UserGroupInformation = {
    val authenticationType = SparkEnv.get.conf.get(hadoopSecurityAuthenticationKey, "simple")
    if (authenticationType.equalsIgnoreCase("kerberos")) {
      logInfo("Setting up kerberos to authenticate")
      SparkEnv.get.conf.getOption(principalKey) match {
        case Some(principal) =>
          SparkEnv.get.conf.getOption(keytabKey) match {
            case Some(keytab) =>
              UserGroupInformation.setConfiguration(newConfiguration(SparkEnv.get.conf))
              loginUserFromKeytab(principal, keytab)
              UserGroupInformation.getLoginUser()
            case None =>
              val errorMsg = securityEnabledButKeyNotDefined.format(keytabKey)
              logError(errorMsg)
              throw new IllegalStateException(errorMsg)
          }
        case None =>
          val errorMsg = securityEnabledButKeyNotDefined.format(principalKey)
          logError(errorMsg)
          throw new IllegalStateException(errorMsg)
      }
    } else {
      logInfo("Not using Kerberos to authenticate to Hadoop.")
      UserGroupInformation.getCurrentUser()
    }
  }
  override def getAuthenticatedUgiForSparkUser(user: String): UserGroupInformation = {
    UserGroupInformation.createProxyUser(user, loginAsSparkUserAndReturnUGI())
  }

  override def loginAsSparkUser() {
    loginAsSparkUserAndReturnUGI()
  }

  private def loginAsSparkUserAndReturnUGI(): UserGroupInformation = loggedInUser

  override def newConfiguration(sparkConf: SparkConf): Configuration = {
    val originalConf = super.newConfiguration(sparkConf)
    originalConf.set("hadoop.security.authentication",
      sparkConf.get(hadoopSecurityAuthenticationKey, "simple"))
    originalConf
  }

}
