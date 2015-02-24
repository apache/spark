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

import java.io._
import java.net.URI
import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}
import java.util.concurrent.{TimeUnit, ThreadFactory, Executors}
import java.util.regex.Matcher
import java.util.regex.Pattern

import scala.collection.mutable.HashMap
import scala.util.Try

import org.apache.hadoop.fs.{FileUtil, Path, FileSystem}
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{Master, JobConf}
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.{Priority, ApplicationAccessType}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkException, SecurityManager, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.{SerializableBuffer, Utils}

/**
 * Contains util methods to interact with Hadoop from spark.
 */
class YarnSparkHadoopUtil extends SparkHadoopUtil {

  private var keytabFile: Option[String] = None
  private var loginPrincipal: Option[String] = None
  private val loggedInViaKeytab = new AtomicBoolean(false)
  private val loggedInUGI = new AtomicReference[UserGroupInformation](null)

  override def transferCredentials(source: UserGroupInformation, dest: UserGroupInformation) {
    dest.addCredentials(source.getCredentials())
  }

  // Note that all params which start with SPARK are propagated all the way through, so if in yarn
  // mode, this MUST be set to true.
  override def isYarnMode(): Boolean = { true }

  // Return an appropriate (subclass) of Configuration. Creating a config initializes some Hadoop
  // subsystems. Always create a new config, dont reuse yarnConf.
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
    creds.addSecretKey(new Text(key), secret.getBytes("utf-8"))
    addCurrentUserCredentials(creds)
  }

  override def getSecretKeyFromUserCredentials(key: String): Array[Byte] = {
    val credentials = getCurrentUserCredentials()
    if (credentials != null) credentials.getSecretKey(new Text(key)) else null
  }

  override def setPrincipalAndKeytabForLogin(principal: String, keytab: String): Unit = {
    loginPrincipal = Option(principal)
    keytabFile = Option(keytab)
  }

  private[spark] override def scheduleLoginFromKeytab(
    callback: (SerializableBuffer)  => Unit): Unit = {

    loginPrincipal match {
      case Some(principal) =>
        val keytab = keytabFile.get
        val remoteFs = FileSystem.get(conf)
        val remoteKeytabPath = new Path(
          remoteFs.getHomeDirectory, System.getenv("SPARK_STAGING_DIR") + Path.SEPARATOR + keytab)
        val localFS = FileSystem.getLocal(conf)
        // At this point, SparkEnv is likely no initialized, so create a dir, put the keytab there.
        val tempDir = Utils.createTempDir()
        val localURI = new URI(tempDir.getAbsolutePath + Path.SEPARATOR + keytab)
        val qualifiedURI = new  URI(localFS.makeQualified(new Path(localURI)).toString)
        FileUtil.copy(
          remoteFs, remoteKeytabPath, localFS, new Path(qualifiedURI), false, false, conf)
        // Get the current credentials, find out when they expire.
        val creds = UserGroupInformation.getCurrentUser.getCredentials
        val credStream = new ByteArrayOutputStream()
        creds.writeTokenStorageToStream(new DataOutputStream(credStream))
        val in = new DataInputStream(new ByteArrayInputStream(credStream.toByteArray))
        val tokenIdentifier = new DelegationTokenIdentifier()
        tokenIdentifier.readFields(in)
        val timeToRenewal = (0.6 * (tokenIdentifier.getMaxDate - System.currentTimeMillis())).toLong
        Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
          override def newThread(r: Runnable): Thread = {
            val t = new Thread(r)
            t.setName("Delegation Token Refresh Thread")
            t.setDaemon(true)
            t
          }
        }).scheduleWithFixedDelay(new Runnable {
          override def run(): Unit = {
            if (!loggedInViaKeytab.get()) {
              loggedInUGI.set(UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                principal, tempDir.getAbsolutePath + Path.SEPARATOR + keytab))
              loggedInViaKeytab.set(true)
            }
            val nns = getNameNodesToAccess(sparkConf) + remoteKeytabPath
            val newCredentials = loggedInUGI.get().getCredentials
            obtainTokensForNamenodes(nns, conf, newCredentials)
            // Now write this out via Akka to executors.
            val outputStream = new ByteArrayOutputStream()
            newCredentials.writeTokenStorageToStream(new DataOutputStream(outputStream))
            callback(new SerializableBuffer(ByteBuffer.wrap(outputStream.toByteArray)))
          }
        }, timeToRenewal, timeToRenewal, TimeUnit.MILLISECONDS)
      case None =>
    }
  }

  /**
   * Get the list of namenodes the user may access.
   */
  def getNameNodesToAccess(sparkConf: SparkConf): Set[Path] = {
    sparkConf.get("spark.yarn.access.namenodes", "")
      .split(",")
      .map(_.trim())
      .filter(!_.isEmpty)
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
    creds: Credentials): Unit = {
    if (UserGroupInformation.isSecurityEnabled()) {
      val delegTokenRenewer = getTokenRenewer(conf)
      paths.foreach { dst =>
        val dstFs = dst.getFileSystem(conf)
        logDebug("getting token for namenode: " + dst)
        dstFs.addDelegationTokens(delegTokenRenewer, creds)
      }
    }
  }

}

object YarnSparkHadoopUtil {
  // Additional memory overhead 
  // 7% was arrived at experimentally. In the interest of minimizing memory waste while covering
  // the common cases. Memory overhead tends to grow with container size. 

  val MEMORY_OVERHEAD_FACTOR = 0.07
  val MEMORY_OVERHEAD_MIN = 384

  val ANY_HOST = "*"

  val DEFAULT_NUMBER_EXECUTORS = 2

  // All RM requests are issued with same priority : we do not (yet) have any distinction between
  // request types (like map/reduce in hadoop for example)
  val RM_REQUEST_PRIORITY = Priority.newInstance(1)

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
   * Escapes a string for inclusion in a command line executed by Yarn. Yarn executes commands
   * using `bash -c "command arg1 arg2"` and that means plain quoting doesn't really work. The
   * argument is enclosed in single quotes and some key characters are escaped.
   *
   * @param arg A single argument.
   * @return Argument quoted for execution via Yarn's generated shell script.
   */
  def escapeForShell(arg: String): String = {
    if (arg != null) {
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
    } else {
      arg
    }
  }

  def getApplicationAclsForYarn(securityMgr: SecurityManager)
      : Map[ApplicationAccessType, String] = {
    Map[ApplicationAccessType, String] (
      ApplicationAccessType.VIEW_APP -> securityMgr.getViewAcls,
      ApplicationAccessType.MODIFY_APP -> securityMgr.getModifyAcls
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

}
