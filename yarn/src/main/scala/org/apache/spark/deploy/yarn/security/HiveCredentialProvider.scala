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

package org.apache.spark.deploy.yarn.security

import java.lang.reflect.UndeclaredThrowableException
import java.security.PrivilegedExceptionAction

import scala.reflect.runtime.universe
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.Token

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[security] class HiveCredentialProvider extends ServiceCredentialProvider with Logging {

  override def serviceName: String = "hive"

  private def hiveConf(hadoopConf: Configuration): Configuration = {
    try {
      val mirror = universe.runtimeMirror(Utils.getSparkClassLoader)
      // the hive configuration class is a subclass of Hadoop Configuration, so can be cast down
      // to a Configuration and used without reflection
      val hiveConfClass = mirror.classLoader.loadClass("org.apache.hadoop.hive.conf.HiveConf")
      // using the (Configuration, Class) constructor allows the current configuration to be
      // included in the hive config.
      val ctor = hiveConfClass.getDeclaredConstructor(classOf[Configuration],
        classOf[Object].getClass)
      ctor.newInstance(hadoopConf, hiveConfClass).asInstanceOf[Configuration]
    } catch {
      case NonFatal(e) =>
        logDebug("Fail to create Hive Configuration", e)
        hadoopConf
    }
  }

  override def credentialsRequired(hadoopConf: Configuration): Boolean = {
    UserGroupInformation.isSecurityEnabled &&
      hiveConf(hadoopConf).getTrimmed("hive.metastore.uris", "").nonEmpty
  }

  override def obtainCredentials(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    val conf = hiveConf(hadoopConf)

    val principalKey = "hive.metastore.kerberos.principal"
    val principal = conf.getTrimmed(principalKey, "")
    require(principal.nonEmpty, s"Hive principal $principalKey undefined")
    val metastoreUri = conf.getTrimmed("hive.metastore.uris", "")
    require(metastoreUri.nonEmpty, "Hive metastore uri undefined")

    val currentUser = UserGroupInformation.getCurrentUser()
    logDebug(s"Getting Hive delegation token for ${currentUser.getUserName()} against " +
      s"$principal at $metastoreUri")

    val mirror = universe.runtimeMirror(Utils.getSparkClassLoader)
    val hiveClass = mirror.classLoader.loadClass("org.apache.hadoop.hive.ql.metadata.Hive")
    val hiveConfClass = mirror.classLoader.loadClass("org.apache.hadoop.hive.conf.HiveConf")
    val closeCurrent = hiveClass.getMethod("closeCurrent")

    try {
      // get all the instance methods before invoking any
      val getDelegationToken = hiveClass.getMethod("getDelegationToken",
        classOf[String], classOf[String])
      val getHive = hiveClass.getMethod("get", hiveConfClass)

      doAsRealUser {
        val hive = getHive.invoke(null, conf)
        val tokenStr = getDelegationToken.invoke(hive, currentUser.getUserName(), principal)
          .asInstanceOf[String]
        val hive2Token = new Token[DelegationTokenIdentifier]()
        hive2Token.decodeFromUrlString(tokenStr)
        logInfo(s"Get Token from hive metastore: ${hive2Token.toString}")
        creds.addToken(new Text("hive.server2.delegation.token"), hive2Token)
      }
    } catch {
      case NonFatal(e) =>
        logDebug(s"Fail to get token from service $serviceName", e)
    } finally {
      Utils.tryLogNonFatalError {
        closeCurrent.invoke(null)
      }
    }

    None
  }

  /**
   * Run some code as the real logged in user (which may differ from the current user, for
   * example, when using proxying).
   */
  private def doAsRealUser[T](fn: => T): T = {
    val currentUser = UserGroupInformation.getCurrentUser()
    val realUser = Option(currentUser.getRealUser()).getOrElse(currentUser)

   // For some reason the Scala-generated anonymous class ends up causing an
   // UndeclaredThrowableException, even if you annotate the method with @throws.
   try {
      realUser.doAs(new PrivilegedExceptionAction[T]() {
        override def run(): T = fn
      })
    } catch {
      case e: UndeclaredThrowableException => throw Option(e.getCause()).getOrElse(e)
    }
  }
}
