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

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark._

import scala.collection.JavaConversions._
import java.util.{Collection, TimerTask, Timer}
import java.io.{File, IOException}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import org.apache.hadoop.security.token.{TokenIdentifier, Token}
import org.apache.hadoop.fs.permission.FsPermission

/**
 * Contains util methods to interact with Hadoop from Spark.
 */
class SparkHadoopUtil extends Logging {
  val conf: Configuration = newConfiguration()
  UserGroupInformation.setConfiguration(conf)

  val sparkConf = new SparkConf()

  def runAsUser(user: String)(func: () => Unit) {
    if (user != SparkContext.SPARK_UNKNOWN_USER) {
      val ugi = UserGroupInformation.createRemoteUser(user)
      transferCredentials(UserGroupInformation.getCurrentUser(), ugi)
      ugi.doAs(new PrivilegedExceptionAction[Unit] {
        def run: Unit = func()
      })
    } else {
      func()
    }
  }

  def transferCredentials(source: UserGroupInformation, dest: UserGroupInformation) {
    for (token <- source.getTokens()) {
      dest.addToken(token)
    }
  }

  /**
   * Return an appropriate (subclass) of Configuration. Creating config can initializes some Hadoop
   * subsystems.
   */
  def newConfiguration(): Configuration = new Configuration()

  /**
   * Add any user credentials to the job conf which are necessary for running on a secure Hadoop
   * cluster.
   */
  def addCredentials(conf: JobConf) {}

  def isYarnMode(): Boolean = { false }

  def getCurrentUserCredentials(): Credentials = { null }

  def addCurrentUserCredentials(creds: Credentials) {}

  def addSecretKeyToUserCredentials(key: String, secret: String) {}

  def getSecretKeyFromUserCredentials(key: String): Array[Byte] = { null }

  /**
   * Return whether Hadoop security is enabled or not.
   *
   * @return Whether Hadoop security is enabled or not
   */
  def isSecurityEnabled(): Boolean = {
    UserGroupInformation.isSecurityEnabled
  }

  /**
   * Do user authentication when Hadoop security is turned on. Used by the driver.
   *
   * @param sc Spark context
   */
  def doUserAuthentication(sc: SparkContext) {
    getAuthenticationType match {
      case "keytab" => {
        // Authentication through a Kerberos keytab file. Necessary for
        // long-running services like Shark/Spark Streaming.
        scheduleKerberosRenewTask(sc)
      }
      case _ => {
        // No authentication needed. Assuming authentication is already done
        // before Spark is launched, e.g., the user has authenticated with
        // Kerberos through kinit already.
        // Renew a Hadoop delegation token and store the token into a file.
        // Add the token file so it gets downloaded by every slave nodes.
        sc.addFile(initDelegationToken().toString)
      }
    }
  }

  /**
   * Get the user whom the task belongs to.
   *
   * @param userName Name of the user whom the task belongs to
   * @return The user whom the task belongs to
   */
  def getTaskUser(userName: String): UserGroupInformation = {
    val ugi = UserGroupInformation.createRemoteUser(userName)
    // Change the authentication method to Kerberos
    ugi.setAuthenticationMethod(
      UserGroupInformation.AuthenticationMethod.KERBEROS)
    // Get and add Hadoop delegation tokens for the user
    val iter = getDelegationTokens().iterator()
    while (iter.hasNext) {
      ugi.addToken(iter.next())
    }

    ugi
  }

  /**
   * Get the type of Hadoop security authentication.
   *
   * @return Type of Hadoop security authentication
   */
  private def getAuthenticationType: String = {
    sparkConf.get("spark.hadoop.security.authentication")
  }

  /**
   * Schedule a timer task for automatically renewing Kerberos credential.
   *
   * @param sc Spark context
   */
  private def scheduleKerberosRenewTask(sc: SparkContext) {
    val kerberosRenewTimer = new Timer()
    val kerberosRenewTimerTask = new TimerTask {
      def run() {
        try {
          kerberosLoginFromKeytab
          // Renew a Hadoop delegation token and store the token into a file.
          // Add the token file so it gets downloaded by every slave nodes.
          sc.addFile(initDelegationToken().toString)
        } catch {
          case ioe: IOException => {
            logError("Failed to login from Kerberos keytab", ioe)
          }
        }
      }
    }

    val interval = sparkConf.getLong(
      "spark.hadoop.security.kerberos.renewInterval", 21600000)
    kerberosRenewTimer.schedule(kerberosRenewTimerTask, 0, interval)
    logInfo("Scheduled timer task for renewing Kerberos credential")
  }

  /**
   * Log a user in from a keytab file. Loads user credential from a keytab
   * file and logs the user in.
   */
  private def kerberosLoginFromKeytab() {
    val defaultKeytab = System.getProperty("user.home") + Path.SEPARATOR +
      System.getProperty("user.name") + ".keytab"
    val keytab = sparkConf.get(
      "spark.hadoop.security.kerberos.keytab", defaultKeytab)
    val principal = sparkConf.get(
      "spark.hadoop.security.kerberos.principal", System.getProperty("user.name"))

    // Keytab file not found
    if (!new File(keytab).exists()) {
      throw new IOException("Keytab file %s not found".format(keytab))
    }

    UserGroupInformation.loginUserFromKeytab(principal, keytab)
  }

  /**
   * Initialize a Hadoop delegation token, store the token into a file,
   * and add it to the SparkContext so executors can get it.
   *
   * @return URI of the token file
   */
  private def initDelegationToken(): URI = {
    val localFS = FileSystem.getLocal(conf)
    // Store the token file under user's home directory
    val tokenFile = new Path(localFS.getHomeDirectory, sparkConf.get(
      "spark.hadoop.security.token.name", "spark.token"))
    if (localFS.exists(tokenFile)) {
      localFS.delete(tokenFile, false)
    }

    // Get a new token and write it to the given token file
    val currentUser = UserGroupInformation.getCurrentUser
    val fs = FileSystem.get(conf)
    val token: Token[_ <: TokenIdentifier] =
      fs.getDelegationToken(currentUser.getShortUserName)
        .asInstanceOf[Token[_ <: TokenIdentifier]]
    val cred = new Credentials()
    cred.addToken(token.getService, token)
    cred.writeTokenStorageFile(tokenFile, conf)
    // Make sure the token file is read-only to the owner
    localFS.setPermission(tokenFile, FsPermission.createImmutable(0400))

    logInfo("Stored Hadoop delegation token for user %s to file %s".format(
      currentUser.getShortUserName, tokenFile.toUri.toString))
    tokenFile.toUri
  }

  /**
   * Get delegation tokens from the token file added through SparkContext.addFile().
   *
   * @return Collection of delegation tokens
   */
  private def getDelegationTokens(): Collection[Token[_ <: TokenIdentifier]] = {
    // Get the token file added through SparkContext.addFile()
    val source = new File(SparkFiles.get(sparkConf.get(
      "spark.hadoop.security.token.name", "spark.token")))
    if (source.exists()) {
      val sourcePath = new Path("file://" + source.getAbsolutePath)
      // Read credentials from the token file
      Credentials.readTokenStorageFile(sourcePath, conf).getAllTokens
    } else {
      throw new IOException(
        "Token file %s does not exist".format(source.getAbsolutePath))
    }
  }
}

object SparkHadoopUtil {

  private val hadoop = {
    val yarnMode = java.lang.Boolean.valueOf(
        System.getProperty("SPARK_YARN_MODE", System.getenv("SPARK_YARN_MODE")))
    if (yarnMode) {
      try {
        Class.forName("org.apache.spark.deploy.yarn.YarnSparkHadoopUtil")
          .newInstance()
          .asInstanceOf[SparkHadoopUtil]
      } catch {
       case th: Throwable => throw new SparkException("Unable to load YARN support", th)
      }
    } else {
      new SparkHadoopUtil
    }
  }

  def get: SparkHadoopUtil = {
    hadoop
  }
}
