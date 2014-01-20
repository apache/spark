/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.deploy.SparkHadoopUtil

/** 
 * Spark class responsible for security.  
 */
private[spark] class SecurityManager extends Logging {

  private val isAuthOn = System.getProperty("spark.authenticate", "false").toBoolean
  private val isUIAuthOn = System.getProperty("spark.authenticate.ui", "false").toBoolean
  private val viewAcls = System.getProperty("spark.ui.view.acls", "").split(',').map(_.trim()).toSet
  private val secretKey = generateSecretKey()
  logDebug("is auth enabled = " + isAuthOn + " is uiAuth enabled = " + isUIAuthOn)
 
  /**
   * In Yarn mode it uses Hadoop UGI to pass the secret as that
   * will keep it protected.  For a standalone SPARK cluster
   * use a environment variable SPARK_SECRET to specify the secret.
   * This probably isn't ideal but only the user who starts the process
   * should have access to view the variable (at least on Linux).
   * Since we can't set the environment variable we set the 
   * java system property SPARK_SECRET so it will automatically
   * generate a secret is not specified.  This definitely is not
   * ideal since users can see it. We should switch to put it in 
   * a config.
   */
  private def generateSecretKey(): String = {

    if (!isAuthenticationEnabled) return null
    // first check to see if secret already set, else generate it
    if (SparkHadoopUtil.get.isYarnMode) {
      val credentials = SparkHadoopUtil.get.getCurrentUserCredentials()
      if (credentials != null) { 
        val secretKey = credentials.getSecretKey(new Text("akkaCookie"))
        if (secretKey != null) {
          logDebug("in yarn mode, getting secret from credentials")
          return new Text(secretKey).toString
        } else {
          logDebug("getSecretKey: yarn mode, secret key from credentials is null")
        }
      } else {
        logDebug("getSecretKey: yarn mode, credentials are null")
      }
    }
    val secret = System.getProperty("SPARK_SECRET", System.getenv("SPARK_SECRET")) 
    if (secret != null && !secret.isEmpty()) return secret 
    // generate one 
    val sCookie = akka.util.Crypt.generateSecureCookie

    // if we generate we must be the first so lets set it so its used by everyone else
    if (SparkHadoopUtil.get.isYarnMode) {
      val creds = new Credentials()
      creds.addSecretKey(new Text("akkaCookie"), sCookie.getBytes())
      SparkHadoopUtil.get.addCurrentUserCredentials(creds)
      logDebug("adding secret to credentials yarn mode")
    } else {
      System.setProperty("SPARK_SECRET", sCookie)
      logDebug("adding secret to java property")
    }
    return sCookie
  }

  def isUIAuthenticationEnabled(): Boolean = return isUIAuthOn 

  // allow anyone in the acl list and the application owner 
  def checkUIViewPermissions(user: String): Boolean = {
    if (isUIAuthenticationEnabled() && (user != null)) {
      if ((!viewAcls.contains(user)) && (user != System.getProperty("user.name"))) {
        return false
      }
    }
    return true
  }

  def isAuthenticationEnabled(): Boolean = return isAuthOn

  // user for HTTP connections
  def getHttpUser(): String = "sparkHttpUser"

  // user to use with SASL connections
  def getSaslUser(): String = "sparkSaslUser"

  /**
   * Gets the secret key if security is enabled, else returns null.
   */
  def getSecretKey(): String = {
    return secretKey
  }
}
