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

import java.nio.charset.StandardCharsets.UTF_8
import java.util.regex.Matcher
import java.util.regex.Pattern

import scala.collection.mutable.{HashMap, ListBuffer}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{ApplicationAccessType, ContainerId, Priority}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.yarn.security.{ConfigurableCredentialManager, CredentialUpdater}
import org.apache.spark.internal.config._
import org.apache.spark.launcher.YarnCommandBuilderUtils
import org.apache.spark.util.Utils

/**
 * Contains util methods to interact with Hadoop from spark.
 */
class YarnSparkHadoopUtil extends SparkHadoopUtil {

  private var credentialUpdater: CredentialUpdater = _

  override def transferCredentials(source: UserGroupInformation, dest: UserGroupInformation) {
    dest.addCredentials(source.getCredentials())
  }

  // Note that all params which start with SPARK are propagated all the way through, so if in yarn
  // mode, this MUST be set to true.
  override def isYarnMode(): Boolean = { true }

  // Return an appropriate (subclass) of Configuration. Creating a config initializes some Hadoop
  // subsystems. Always create a new config, don't reuse yarnConf.
  override def newConfiguration(conf: SparkConf): Configuration =
    new YarnConfiguration(super.newConfiguration(conf))

  // Add any user credentials to the job conf which are necessary for running on a secure Hadoop
  // cluster
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
    creds.addSecretKey(new Text(key), secret.getBytes(UTF_8))
    addCurrentUserCredentials(creds)
  }

  override def getSecretKeyFromUserCredentials(key: String): Array[Byte] = {
    val credentials = getCurrentUserCredentials()
    if (credentials != null) credentials.getSecretKey(new Text(key)) else null
  }

  private[spark] override def startCredentialUpdater(sparkConf: SparkConf): Unit = {
    credentialUpdater =
      new ConfigurableCredentialManager(sparkConf, newConfiguration(sparkConf)).credentialUpdater()
    credentialUpdater.start()
  }

  private[spark] override def stopCredentialUpdater(): Unit = {
    if (credentialUpdater != null) {
      credentialUpdater.stop()
      credentialUpdater = null
    }
  }

  private[spark] def getContainerId: ContainerId = {
    val containerIdString = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name())
    ConverterUtils.toContainerId(containerIdString)
  }
}

object YarnSparkHadoopUtil {
  // Additional memory overhead
  // 10% was arrived at experimentally. In the interest of minimizing memory waste while covering
  // the common cases. Memory overhead tends to grow with container size.

  val MEMORY_OVERHEAD_FACTOR = 0.10
  val MEMORY_OVERHEAD_MIN = 384L

  val ANY_HOST = "*"

  val DEFAULT_NUMBER_EXECUTORS = 2

  // All RM requests are issued with same priority : we do not (yet) have any distinction between
  // request types (like map/reduce in hadoop for example)
  val RM_REQUEST_PRIORITY = Priority.newInstance(1)

  def get: YarnSparkHadoopUtil = {
    val yarnMode = java.lang.Boolean.parseBoolean(
      System.getProperty("SPARK_YARN_MODE", System.getenv("SPARK_YARN_MODE")))
    if (!yarnMode) {
      throw new SparkException("YarnSparkHadoopUtil is not available in non-YARN mode!")
    }
    SparkHadoopUtil.get.asInstanceOf[YarnSparkHadoopUtil]
  }
  /**
   * Add a path variable to the given environment map.
   * If the map already contains this key, append the value to the existing value instead.
   */
  def addPathToEnvironment(env: HashMap[String, String], key: String, value: String): Unit = {
    val newValue =
      if (env.contains(key)) {
        env(key) + ApplicationConstants.CLASS_PATH_SEPARATOR  + value
      } else {
        value
      }
    env.put(key, newValue)
  }

  /**
   * Set zero or more environment variables specified by the given input string.
   * The input string is expected to take the form "KEY1=VAL1,KEY2=VAL2,KEY3=VAL3".
   */
  def setEnvFromInputString(env: HashMap[String, String], inputString: String): Unit = {
    if (inputString != null && inputString.length() > 0) {
      val childEnvs = inputString.split(",")
      val p = Pattern.compile(environmentVariableRegex)
      for (cEnv <- childEnvs) {
        val parts = cEnv.split("=") // split on '='
        val m = p.matcher(parts(1))
        val sb = new StringBuffer
        while (m.find()) {
          val variable = m.group(1)
          var replace = ""
          if (env.contains(variable)) {
            replace = env(variable)
          } else {
            // if this key is not configured for the child .. get it from the env
            replace = System.getenv(variable)
            if (replace == null) {
            // the env key is note present anywhere .. simply set it
              replace = ""
            }
          }
          m.appendReplacement(sb, Matcher.quoteReplacement(replace))
        }
        m.appendTail(sb)
        // This treats the environment variable as path variable delimited by `File.pathSeparator`
        // This is kept for backward compatibility and consistency with Hadoop's behavior
        addPathToEnvironment(env, parts(0), sb.toString)
      }
    }
  }

  private val environmentVariableRegex: String = {
    if (Utils.isWindows) {
      "%([A-Za-z_][A-Za-z0-9_]*?)%"
    } else {
      "\\$([A-Za-z_][A-Za-z0-9_]*)"
    }
  }

  /**
   * Kill if OOM is raised - leverage yarn's failure handling to cause rescheduling.
   * Not killing the task leaves various aspects of the executor and (to some extent) the jvm in
   * an inconsistent state.
   * TODO: If the OOM is not recoverable by rescheduling it on different node, then do
   * 'something' to fail job ... akin to blacklisting trackers in mapred ?
   *
   * The handler if an OOM Exception is thrown by the JVM must be configured on Windows
   * differently: the 'taskkill' command should be used, whereas Unix-based systems use 'kill'.
   *
   * As the JVM interprets both %p and %%p as the same, we can use either of them. However,
   * some tests on Windows computers suggest, that the JVM only accepts '%%p'.
   *
   * Furthermore, the behavior of the character '%' on the Windows command line differs from
   * the behavior of '%' in a .cmd file: it gets interpreted as an incomplete environment
   * variable. Windows .cmd files escape a '%' by '%%'. Thus, the correct way of writing
   * '%%p' in an escaped way is '%%%%p'.
   */
  private[yarn] def addOutOfMemoryErrorArgument(javaOpts: ListBuffer[String]): Unit = {
    if (!javaOpts.exists(_.contains("-XX:OnOutOfMemoryError"))) {
      if (Utils.isWindows) {
        javaOpts += escapeForShell("-XX:OnOutOfMemoryError=taskkill /F /PID %%%%p")
      } else {
        javaOpts += "-XX:OnOutOfMemoryError='kill %p'"
      }
    }
  }

  /**
   * Escapes a string for inclusion in a command line executed by Yarn. Yarn executes commands
   * using either
   *
   * (Unix-based) `bash -c "command arg1 arg2"` and that means plain quoting doesn't really work.
   * The argument is enclosed in single quotes and some key characters are escaped.
   *
   * (Windows-based) part of a .cmd file in which case windows escaping for each argument must be
   * applied. Windows is quite lenient, however it is usually Java that causes trouble, needing to
   * distinguish between arguments starting with '-' and class names. If arguments are surrounded
   * by ' java takes the following string as is, hence an argument is mistakenly taken as a class
   * name which happens to start with a '-'. The way to avoid this, is to surround nothing with
   * a ', but instead with a ".
   *
   * @param arg A single argument.
   * @return Argument quoted for execution via Yarn's generated shell script.
   */
  def escapeForShell(arg: String): String = {
    if (arg != null) {
      if (Utils.isWindows) {
        YarnCommandBuilderUtils.quoteForBatchScript(arg)
      } else {
        val escaped = new StringBuilder("'")
        arg.foreach {
          case '$' => escaped.append("\\$")
          case '"' => escaped.append("\\\"")
          case '\'' => escaped.append("'\\''")
          case c => escaped.append(c)
        }
        escaped.append("'").toString()
      }
    } else {
      arg
    }
  }

  // YARN/Hadoop acls are specified as user1,user2 group1,group2
  // Users and groups are separated by a space and hence we need to pass the acls in same format
  def getApplicationAclsForYarn(securityMgr: SecurityManager)
      : Map[ApplicationAccessType, String] = {
    Map[ApplicationAccessType, String] (
      ApplicationAccessType.VIEW_APP -> (securityMgr.getViewAcls + " " +
        securityMgr.getViewAclsGroups),
      ApplicationAccessType.MODIFY_APP -> (securityMgr.getModifyAcls + " " +
        securityMgr.getModifyAclsGroups)
    )
  }

  /**
   * Getting the initial target number of executors depends on whether dynamic allocation is
   * enabled.
   * If not using dynamic allocation it gets the number of executors requested by the user.
   */
  def getInitialTargetExecutorNumber(
      conf: SparkConf,
      numExecutors: Int = DEFAULT_NUMBER_EXECUTORS): Int = {
    if (Utils.isDynamicAllocationEnabled(conf)) {
      val minNumExecutors = conf.get(DYN_ALLOCATION_MIN_EXECUTORS)
      val initialNumExecutors = Utils.getDynamicAllocationInitialExecutors(conf)
      val maxNumExecutors = conf.get(DYN_ALLOCATION_MAX_EXECUTORS)
      require(initialNumExecutors >= minNumExecutors && initialNumExecutors <= maxNumExecutors,
        s"initial executor number $initialNumExecutors must between min executor number " +
          s"$minNumExecutors and max executor number $maxNumExecutors")

      initialNumExecutors
    } else {
      conf.get(EXECUTOR_INSTANCES).getOrElse(numExecutors)
    }
  }
}

