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

package org.apache.spark.deploy.yarn

import java.util.regex.Matcher
import java.util.regex.Pattern

import scala.collection.mutable.HashMap

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.StringInterner
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * Contains util methods to interact with Hadoop from spark.
 */
class YarnSparkHadoopUtil extends SparkHadoopUtil {

  override def transferCredentials(source: UserGroupInformation, dest: UserGroupInformation) {
    dest.addCredentials(source.getCredentials())
  }

  // Note that all params which start with SPARK are propagated all the way through, so if in yarn mode, this MUST be set to true.
  override def isYarnMode(): Boolean = { true }

  // Return an appropriate (subclass) of Configuration. Creating config can initializes some hadoop subsystems
  // Always create a new config, dont reuse yarnConf.
  override def newConfiguration(): Configuration = new YarnConfiguration(new Configuration())

  // add any user credentials to the job conf which are necessary for running on a secure Hadoop cluster
  override def addCredentials(conf: JobConf) {
    val jobCreds = conf.getCredentials()
    jobCreds.mergeAll(UserGroupInformation.getCurrentUser().getCredentials())
  }

  override def getCurrentUserCredentials(): Credentials = {
    UserGroupInformation.getCurrentUser().getCredentials()
  }

  override def addCurrentUserCredentials(creds: Credentials) {
    UserGroupInformation.getCurrentUser().addCredentials(creds)
  }

  override def addSecretKeyToUserCredentials(key: String, secret: String) {
    val creds = new Credentials()
    creds.addSecretKey(new Text(key), secret.getBytes("utf-8"))
    addCurrentUserCredentials(creds)
  }

  override def getSecretKeyFromUserCredentials(key: String): Array[Byte] = {
    val credentials = getCurrentUserCredentials()
    if (credentials != null) credentials.getSecretKey(new Text(key)) else null
  }

}

object YarnSparkHadoopUtil {
  def addToEnvironment(
      env: HashMap[String, String],
      variable: String,
      value: String,
      classPathSeparator: String) = {
    var envVariable = ""
    if (env.get(variable) == None) {
      envVariable = value
    } else {
      envVariable = env.get(variable).get + classPathSeparator + value
    }
    env put (StringInterner.weakIntern(variable), StringInterner.weakIntern(envVariable))
  }

  def setEnvFromInputString(
      env: HashMap[String, String],
      envString: String,
      classPathSeparator: String) = {
    if (envString != null && envString.length() > 0) {
      var childEnvs = envString.split(",")
      var p = Pattern.compile(getEnvironmentVariableRegex())
      for (cEnv <- childEnvs) {
        var parts = cEnv.split("=") // split on '='
        var m = p.matcher(parts(1))
        val sb = new StringBuffer
        while (m.find()) {
          val variable = m.group(1)
          var replace = ""
          if (env.get(variable) != None) {
            replace = env.get(variable).get
          } else {
            // if this key is not configured for the child .. get it
            // from the env
            replace = System.getenv(variable)
            if (replace == null) {
            // the env key is note present anywhere .. simply set it
              replace = ""
            }
          }
          m.appendReplacement(sb, Matcher.quoteReplacement(replace))
        }
        m.appendTail(sb)
        addToEnvironment(env, parts(0), sb.toString(), classPathSeparator)
      }
    }
  }

  private def getEnvironmentVariableRegex() : String = {
    val osName = System.getProperty("os.name")
    if (osName startsWith "Windows") {
      "%([A-Za-z_][A-Za-z0-9_]*?)%"
    } else {
      "\\$([A-Za-z_][A-Za-z0-9_]*)"
    }
  }

  def getUIHistoryAddress(sc: SparkContext, conf: SparkConf) : String = {
    val eventLogDir = sc.eventLogger match {
      case Some(logger) => logger.getApplicationLogDir()
      case None => ""
    }
    val historyServerAddress = conf.get("spark.yarn.historyServer.address", "")
    if (historyServerAddress != "" && eventLogDir != "") {
      historyServerAddress + HistoryServer.UI_PATH_PREFIX + s"/$eventLogDir"
    } else {
      ""
    }
  }

}
