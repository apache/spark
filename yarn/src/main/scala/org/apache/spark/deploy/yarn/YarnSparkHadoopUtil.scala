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
import java.util.concurrent.{ TimeUnit, Executors}
import java.util.regex.Matcher
import java.util.regex.Pattern

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import scala.util.Try

import org.apache.hadoop.fs.Options.Rename
import org.apache.hadoop.fs.{FileUtil, Path, FileSystem}
import org.apache.hadoop.hdfs.DistributedFileSystem
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
import org.apache.spark.util.Utils

/**
 * Contains util methods to interact with Hadoop from spark.
 */
class YarnSparkHadoopUtil extends SparkHadoopUtil {

  private var keytab: String = null
  private var principal: String = null
  @volatile private var loggedInViaKeytab = false
  @volatile private var loggedInUGI: UserGroupInformation = null
  @volatile private var lastCredentialsRefresh = 0L
  private lazy val delegationTokenRenewer =
    Executors.newSingleThreadScheduledExecutor(
      Utils.namedThreadFactory("Delegation Token Refresh Thread"))
  private lazy val delegationTokenExecuterUpdaterThread = new Runnable {
    override def run(): Unit = updateCredentialsIfRequired()
  }

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

  private[spark] override def scheduleLoginFromKeytab(): Unit = {
    val principal = System.getenv("SPARK_PRINCIPAL")
    val keytab = System.getenv("SPARK_KEYTAB")
    if (principal != null) {
      val delegationTokenRenewerThread =
        new Runnable {
          override def run(): Unit = {
            if (!loggedInViaKeytab) {
              // Keytab is copied by YARN to the working directory of the AM, so full path is
              // not needed.
              loggedInUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                principal, keytab)
              loggedInViaKeytab = true
            }
            val nns = getNameNodesToAccess(sparkConf)
            val newCredentials = loggedInUGI.getCredentials
            obtainTokensForNamenodes(nns, conf, newCredentials)
            val remoteFs = FileSystem.get(conf)
            val stagingDirPath =
              new Path(remoteFs.getHomeDirectory, System.getenv("SPARK_YARN_STAGING_DIR"))
            val tokenPathStr = sparkConf.get("spark.yarn.credentials.file")
            val tokenPath = new Path(stagingDirPath.toString, tokenPathStr)
            val tempTokenPath = new Path(stagingDirPath.toString, tokenPathStr + ".tmp")
            val stream = remoteFs.create(tempTokenPath, true)
            // Now write this out to HDFS
            newCredentials.writeTokenStorageToStream(stream)
            stream.hflush()
            stream.close()
            // HDFS does reads by inodes now, so just doing a rename should be fine. But I could
            // not find a clear explanation of when the blocks on HDFS are deleted. Ideally, we
            // would not need this, but just be defensive to ensure we don't mess up the
            // credentials. So create a file to show that we are currently updating - if the
            // reader sees this file, they go away and come back later. Then delete old token and
            // rename the old to new.
            val updatingPath = new Path(stagingDirPath, "_UPDATING")
            if (remoteFs.exists(updatingPath)) {
              remoteFs.delete(updatingPath, true)
            }
            remoteFs.create(updatingPath).close()
            if (remoteFs.exists(tokenPath)) {
              remoteFs.delete(tokenPath, true)
            }
            remoteFs.rename(tempTokenPath, tokenPath)
            remoteFs.delete(updatingPath, true)
            delegationTokenRenewer.schedule(
              this, (0.75 * (getLatestValidity - System.currentTimeMillis())).toLong,
              TimeUnit.MILLISECONDS)
          }
        }
      val timeToRenewal = (0.75 * (getLatestValidity - System.currentTimeMillis())).toLong
      delegationTokenRenewer.schedule(
        delegationTokenRenewerThread, timeToRenewal, TimeUnit.MILLISECONDS)
    }
  }

  override def updateCredentialsIfRequired(): Unit = {
    try {
      val credentialsFile = sparkConf.get("spark.yarn.credentials.file")
      if (credentialsFile != null && !credentialsFile.isEmpty) {
        val remoteFs = FileSystem.get(conf)
        val sparkStagingDir = System.getenv("SPARK_YARN_STAGING_DIR")
        val stagingDirPath = new Path(remoteFs.getHomeDirectory, sparkStagingDir)
        val credentialsFilePath = new Path(stagingDirPath, credentialsFile)
        // If an update is currently in progress, come back later!
        if (remoteFs.exists( new Path(stagingDirPath, "_UPDATING"))) {
          delegationTokenRenewer.schedule(delegationTokenExecuterUpdaterThread, 1, TimeUnit.HOURS)
        }
        // Now check if the file exists, if it does go get the credentials from there
        if (remoteFs.exists(credentialsFilePath)) {
          val status = remoteFs.getFileStatus(credentialsFilePath)
          val modTimeAtStart = status.getModificationTime
          if (modTimeAtStart > lastCredentialsRefresh) {
            val newCredentials = getCredentialsFromHDFSFile(remoteFs, credentialsFilePath)
            val newStatus = remoteFs.getFileStatus(credentialsFilePath)
            // File was updated after we started reading it, lets come back later and try to read
            // it.
            if (newStatus.getModificationTime != modTimeAtStart) {
              delegationTokenRenewer
                .schedule(delegationTokenExecuterUpdaterThread, 1, TimeUnit.HOURS)
            } else {
              UserGroupInformation.getCurrentUser.addCredentials(newCredentials)
              lastCredentialsRefresh = status.getModificationTime
              val totalValidity = getLatestValidity - lastCredentialsRefresh
              val timeToRunRenewal = lastCredentialsRefresh + (0.8 * totalValidity).toLong
              val timeFromNowToRenewal = timeToRunRenewal - System.currentTimeMillis()
              delegationTokenRenewer.schedule(delegationTokenExecuterUpdaterThread,
                timeFromNowToRenewal, TimeUnit.MILLISECONDS)
            }
          } else {
            // Check every hour to see if new credentials arrived.
            delegationTokenRenewer.schedule(delegationTokenExecuterUpdaterThread, 1, TimeUnit.HOURS)
          }
        }
      }
    } catch {
      // Since the file may get deleted while we are reading it, catch the Exception and come
      // back in an hour to try again
      case e: Exception =>
        logWarning(
          "Error encountered while trying to update credentials, will try again in 1 hour", e)
        delegationTokenRenewer.schedule(delegationTokenExecuterUpdaterThread, 1, TimeUnit.HOURS)
    }
  }

  private[spark] def getCredentialsFromHDFSFile(
    remoteFs: FileSystem,
    tokenPath: Path
  ): Credentials = {
    val stream = remoteFs.open(tokenPath)
    val newCredentials = new Credentials()
    newCredentials.readFields(stream)
    newCredentials
  }

  private[spark] def getLatestValidity: Long = {
    val creds = UserGroupInformation.getCurrentUser.getCredentials
    var latestValidity: Long = 0
    creds.getAllTokens
      .filter(_.getKind == DelegationTokenIdentifier.HDFS_DELEGATION_KIND)
      .foreach { t =>
        val identifier = new DelegationTokenIdentifier()
        identifier.readFields(new DataInputStream(new ByteArrayInputStream(t.getIdentifier)))
        latestValidity = {
          if (latestValidity < identifier.getMaxDate) {
            identifier.getMaxDate
          } else {
            latestValidity
          }
        }
      }
    latestValidity
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
    creds: Credentials
  ): Unit = {
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
