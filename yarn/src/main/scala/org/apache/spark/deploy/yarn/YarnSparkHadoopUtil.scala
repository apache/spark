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

import java.io.File
import java.lang.reflect.UndeclaredThrowableException
import java.nio.charset.StandardCharsets.UTF_8
import java.security.PrivilegedExceptionAction
import java.util.regex.Matcher
import java.util.regex.Pattern

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.reflect.runtime._
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{JobConf, Master}
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.{ApplicationAccessType, ContainerId, Priority}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.config._
import org.apache.spark.launcher.YarnCommandBuilderUtils
import org.apache.spark.util.Utils

/**
 * Contains util methods to interact with Hadoop from spark.
 */
class YarnSparkHadoopUtil extends SparkHadoopUtil {

  private var tokenRenewer: Option[ExecutorDelegationTokenUpdater] = None

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

  /**
   * Get the list of namenodes the user may access.
   */
  def getNameNodesToAccess(sparkConf: SparkConf): Set[Path] = {
    sparkConf.get(NAMENODES_TO_ACCESS)
      .map(new Path(_))
      .toSet
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

  /**
   * Obtains tokens for the namenodes passed in and adds them to the credentials.
   */
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

  /**
   * Obtains token for the Hive metastore and adds them to the credentials.
   */
  def obtainTokenForHiveMetastore(
      sparkConf: SparkConf,
      conf: Configuration,
      credentials: Credentials) {
    if (shouldGetTokens(sparkConf, "hive") && UserGroupInformation.isSecurityEnabled) {
      YarnSparkHadoopUtil.get.obtainTokenForHiveMetastore(conf).foreach {
        credentials.addToken(new Text("hive.server2.delegation.token"), _)
      }
    }
  }

  /**
   * Obtain a security token for HBase.
   */
  def obtainTokenForHBase(
      sparkConf: SparkConf,
      conf: Configuration,
      credentials: Credentials): Unit = {
    if (shouldGetTokens(sparkConf, "hbase") && UserGroupInformation.isSecurityEnabled) {
      YarnSparkHadoopUtil.get.obtainTokenForHBase(conf).foreach { token =>
        credentials.addToken(token.getService, token)
        logInfo("Added HBase security token to credentials.")
      }
    }
  }

  /**
   * Return whether delegation tokens should be retrieved for the given service when security is
   * enabled. By default, tokens are retrieved, but that behavior can be changed by setting
   * a service-specific configuration.
   */
  private def shouldGetTokens(conf: SparkConf, service: String): Boolean = {
    conf.getBoolean(s"spark.yarn.security.tokens.${service}.enabled", true)
  }

  private[spark] override def startExecutorDelegationTokenRenewer(sparkConf: SparkConf): Unit = {
    tokenRenewer = Some(new ExecutorDelegationTokenUpdater(sparkConf, conf))
    tokenRenewer.get.updateCredentialsIfRequired()
  }

  private[spark] override def stopExecutorDelegationTokenRenewer(): Unit = {
    tokenRenewer.foreach(_.stop())
  }

  private[spark] def getContainerId: ContainerId = {
    val containerIdString = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name())
    ConverterUtils.toContainerId(containerIdString)
  }

  /**
   * Obtains token for the Hive metastore, using the current user as the principal.
   * Some exceptions are caught and downgraded to a log message.
   * @param conf hadoop configuration; the Hive configuration will be based on this
   * @return a token, or `None` if there's no need for a token (no metastore URI or principal
   *         in the config), or if a binding exception was caught and downgraded.
   */
  def obtainTokenForHiveMetastore(conf: Configuration): Option[Token[DelegationTokenIdentifier]] = {
    try {
      obtainTokenForHiveMetastoreInner(conf)
    } catch {
      case e: ClassNotFoundException =>
        logInfo(s"Hive class not found $e")
        logDebug("Hive class not found", e)
        None
    }
  }

  /**
   * Inner routine to obtains token for the Hive metastore; exceptions are raised on any problem.
   * @param conf hadoop configuration; the Hive configuration will be based on this.
   * @param username the username of the principal requesting the delegating token.
   * @return a delegation token
   */
  private[yarn] def obtainTokenForHiveMetastoreInner(conf: Configuration):
      Option[Token[DelegationTokenIdentifier]] = {
    val mirror = universe.runtimeMirror(Utils.getContextOrSparkClassLoader)

    // the hive configuration class is a subclass of Hadoop Configuration, so can be cast down
    // to a Configuration and used without reflection
    val hiveConfClass = mirror.classLoader.loadClass("org.apache.hadoop.hive.conf.HiveConf")
    // using the (Configuration, Class) constructor allows the current configuration to be included
    // in the hive config.
    val ctor = hiveConfClass.getDeclaredConstructor(classOf[Configuration],
      classOf[Object].getClass)
    val hiveConf = ctor.newInstance(conf, hiveConfClass).asInstanceOf[Configuration]
    val metastoreUri = hiveConf.getTrimmed("hive.metastore.uris", "")

    // Check for local metastore
    if (metastoreUri.nonEmpty) {
      val principalKey = "hive.metastore.kerberos.principal"
      val principal = hiveConf.getTrimmed(principalKey, "")
      require(principal.nonEmpty, "Hive principal $principalKey undefined")
      val currentUser = UserGroupInformation.getCurrentUser()
      logDebug(s"Getting Hive delegation token for ${currentUser.getUserName()} against " +
        s"$principal at $metastoreUri")
      val hiveClass = mirror.classLoader.loadClass("org.apache.hadoop.hive.ql.metadata.Hive")
      val closeCurrent = hiveClass.getMethod("closeCurrent")
      try {
        // get all the instance methods before invoking any
        val getDelegationToken = hiveClass.getMethod("getDelegationToken",
          classOf[String], classOf[String])
        val getHive = hiveClass.getMethod("get", hiveConfClass)

        doAsRealUser {
          val hive = getHive.invoke(null, hiveConf)
          val tokenStr = getDelegationToken.invoke(hive, currentUser.getUserName(), principal)
            .asInstanceOf[String]
          val hive2Token = new Token[DelegationTokenIdentifier]()
          hive2Token.decodeFromUrlString(tokenStr)
          Some(hive2Token)
        }
      } finally {
        Utils.tryLogNonFatalError {
          closeCurrent.invoke(null)
        }
      }
    } else {
      logDebug("HiveMetaStore configured in localmode")
      None
    }
  }

  /**
   * Obtain a security token for HBase.
   *
   * Requirements
   *
   * 1. `"hbase.security.authentication" == "kerberos"`
   * 2. The HBase classes `HBaseConfiguration` and `TokenUtil` could be loaded
   * and invoked.
   *
   * @param conf Hadoop configuration; an HBase configuration is created
   *             from this.
   * @return a token if the requirements were met, `None` if not.
   */
  def obtainTokenForHBase(conf: Configuration): Option[Token[TokenIdentifier]] = {
    try {
      obtainTokenForHBaseInner(conf)
    } catch {
      case e: ClassNotFoundException =>
        logInfo(s"HBase class not found $e")
        logDebug("HBase class not found", e)
        None
    }
  }

  /**
   * Obtain a security token for HBase if `"hbase.security.authentication" == "kerberos"`
   *
   * @param conf Hadoop configuration; an HBase configuration is created
   *             from this.
   * @return a token if one was needed
   */
  def obtainTokenForHBaseInner(conf: Configuration): Option[Token[TokenIdentifier]] = {
    val mirror = universe.runtimeMirror(getClass.getClassLoader)
    val confCreate = mirror.classLoader.
      loadClass("org.apache.hadoop.hbase.HBaseConfiguration").
      getMethod("create", classOf[Configuration])
    val obtainToken = mirror.classLoader.
      loadClass("org.apache.hadoop.hbase.security.token.TokenUtil").
      getMethod("obtainToken", classOf[Configuration])
    val hbaseConf = confCreate.invoke(null, conf).asInstanceOf[Configuration]
    if ("kerberos" == hbaseConf.get("hbase.security.authentication")) {
      logDebug("Attempting to fetch HBase security token.")
      Some(obtainToken.invoke(null, hbaseConf).asInstanceOf[Token[TokenIdentifier]])
    } else {
      None
    }
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
    val newValue = if (env.contains(key)) { env(key) + getClassPathSeparator  + value } else value
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
          if (env.get(variable) != None) {
            replace = env.get(variable).get
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
        for (i <- 0 to arg.length() - 1) {
          arg.charAt(i) match {
            case '$' => escaped.append("\\$")
            case '"' => escaped.append("\\\"")
            case '\'' => escaped.append("'\\''")
            case c => escaped.append(c)
          }
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
   * Expand environment variable using Yarn API.
   * If environment.$$() is implemented, return the result of it.
   * Otherwise, return the result of environment.$()
   * Note: $$() is added in Hadoop 2.4.
   */
  private lazy val expandMethod =
    Try(classOf[Environment].getMethod("$$"))
      .getOrElse(classOf[Environment].getMethod("$"))

  def expandEnvironment(environment: Environment): String =
    expandMethod.invoke(environment).asInstanceOf[String]

  /**
   * Get class path separator using Yarn API.
   * If ApplicationConstants.CLASS_PATH_SEPARATOR is implemented, return it.
   * Otherwise, return File.pathSeparator
   * Note: CLASS_PATH_SEPARATOR is added in Hadoop 2.4.
   */
  private lazy val classPathSeparatorField =
    Try(classOf[ApplicationConstants].getField("CLASS_PATH_SEPARATOR"))
      .getOrElse(classOf[File].getField("pathSeparator"))

  def getClassPathSeparator(): String = {
    classPathSeparatorField.get(null).asInstanceOf[String]
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
      val initialNumExecutors = conf.get(DYN_ALLOCATION_INITIAL_EXECUTORS)
      val maxNumExecutors = conf.get(DYN_ALLOCATION_MAX_EXECUTORS)
      require(initialNumExecutors >= minNumExecutors && initialNumExecutors <= maxNumExecutors,
        s"initial executor number $initialNumExecutors must between min executor number " +
          s"$minNumExecutors and max executor number $maxNumExecutors")

      initialNumExecutors
    } else {
      val targetNumExecutors =
        sys.env.get("SPARK_EXECUTOR_INSTANCES").map(_.toInt).getOrElse(numExecutors)
      // System property can override environment variable.
      conf.get(EXECUTOR_INSTANCES).getOrElse(targetNumExecutors)
    }
  }
}

