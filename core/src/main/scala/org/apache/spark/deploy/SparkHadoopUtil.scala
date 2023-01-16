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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, File, IOException}
import java.net.InetAddress
import java.security.PrivilegedExceptionAction
import java.text.DateFormat
import java.util.{Arrays, Date, Locale}

import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

import com.google.common.primitives.Longs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.BUFFER_SIZE
import org.apache.spark.util.Utils

/**
 * Contains util methods to interact with Hadoop from Spark.
 */
private[spark] class SparkHadoopUtil extends Logging {
  private val sparkConf = new SparkConf(false).loadFromSystemProperties(true)
  val conf: Configuration = newConfiguration(sparkConf)
  UserGroupInformation.setConfiguration(conf)

  /**
   * Runs the given function with a Hadoop UserGroupInformation as a thread local variable
   * (distributed to child threads), used for authenticating HDFS and YARN calls.
   *
   * IMPORTANT NOTE: If this function is going to be called repeated in the same process
   * you need to look https://issues.apache.org/jira/browse/HDFS-3545 and possibly
   * do a FileSystem.closeAllForUGI in order to avoid leaking Filesystems
   */
  def runAsSparkUser(func: () => Unit): Unit = {
    createSparkUser().doAs(new PrivilegedExceptionAction[Unit] {
      def run: Unit = func()
    })
  }

  def createSparkUser(): UserGroupInformation = {
    val user = Utils.getCurrentUserName()
    logDebug("creating UGI for user: " + user)
    val ugi = UserGroupInformation.createRemoteUser(user)
    transferCredentials(UserGroupInformation.getCurrentUser(), ugi)
    ugi
  }

  def transferCredentials(source: UserGroupInformation, dest: UserGroupInformation): Unit = {
    dest.addCredentials(source.getCredentials())
  }

  /**
   * Appends S3-specific, spark.hadoop.*, and spark.buffer.size configurations to a Hadoop
   * configuration.
   */
  def appendS3AndSparkHadoopHiveConfigurations(
      conf: SparkConf,
      hadoopConf: Configuration): Unit = {
    SparkHadoopUtil.appendS3AndSparkHadoopHiveConfigurations(conf, hadoopConf)
  }

  /**
   * Appends spark.hadoop.* configurations from a [[SparkConf]] to a Hadoop
   * configuration without the spark.hadoop. prefix.
   */
  def appendSparkHadoopConfigs(conf: SparkConf, hadoopConf: Configuration): Unit = {
    SparkHadoopUtil.appendSparkHadoopConfigs(conf, hadoopConf)
  }

  /**
   * Appends spark.hadoop.* configurations from a Map to another without the spark.hadoop. prefix.
   */
  def appendSparkHadoopConfigs(
      srcMap: Map[String, String],
      destMap: HashMap[String, String]): Unit = {
    // Copy any "spark.hadoop.foo=bar" system properties into destMap as "foo=bar"
    for ((key, value) <- srcMap if key.startsWith("spark.hadoop.")) {
      destMap.put(key.substring("spark.hadoop.".length), value)
    }
  }

  def appendSparkHiveConfigs(
      srcMap: Map[String, String],
      destMap: HashMap[String, String]): Unit = {
    // Copy any "spark.hive.foo=bar" system properties into destMap as "hive.foo=bar"
    for ((key, value) <- srcMap if key.startsWith("spark.hive.")) {
      destMap.put(key.substring("spark.".length), value)
    }
  }

  /**
   * Return an appropriate (subclass) of Configuration. Creating config can initialize some Hadoop
   * subsystems.
   */
  def newConfiguration(conf: SparkConf): Configuration = {
    val hadoopConf = SparkHadoopUtil.newConfiguration(conf)
    hadoopConf.addResource(SparkHadoopUtil.SPARK_HADOOP_CONF_FILE)
    hadoopConf
  }

  /**
   * Add any user credentials to the job conf which are necessary for running on a secure Hadoop
   * cluster.
   */
  def addCredentials(conf: JobConf): Unit = {
    val jobCreds = conf.getCredentials()
    jobCreds.mergeAll(UserGroupInformation.getCurrentUser().getCredentials())
  }

  def addCurrentUserCredentials(creds: Credentials): Unit = {
    UserGroupInformation.getCurrentUser.addCredentials(creds)
  }

  def loginUserFromKeytab(principalName: String, keytabFilename: String): Unit = {
    if (!new File(keytabFilename).exists()) {
      throw new SparkException(s"Keytab file: ${keytabFilename} does not exist")
    } else {
      logInfo("Attempting to login to Kerberos " +
        s"using principal: ${principalName} and keytab: ${keytabFilename}")
      UserGroupInformation.loginUserFromKeytab(principalName, keytabFilename)
    }
  }

  /**
   * Add or overwrite current user's credentials with serialized delegation tokens,
   * also confirms correct hadoop configuration is set.
   */
  private[spark] def addDelegationTokens(tokens: Array[Byte], sparkConf: SparkConf): Unit = {
    UserGroupInformation.setConfiguration(newConfiguration(sparkConf))
    val creds = deserialize(tokens)
    logInfo("Updating delegation tokens for current user.")
    logDebug(s"Adding/updating delegation tokens ${dumpTokens(creds)}")
    addCurrentUserCredentials(creds)
  }

  /**
   * Returns a function that can be called to find Hadoop FileSystem bytes read. If
   * getFSBytesReadOnThreadCallback is called from thread r at time t, the returned callback will
   * return the bytes read on r since t.
   */
  private[spark] def getFSBytesReadOnThreadCallback(): () => Long = {
    val f = () => FileSystem.getAllStatistics.asScala.map(_.getThreadStatistics.getBytesRead).sum
    val baseline = (Thread.currentThread().getId, f())

    /**
     * This function may be called in both spawned child threads and parent task thread (in
     * PythonRDD), and Hadoop FileSystem uses thread local variables to track the statistics.
     * So we need a map to track the bytes read from the child threads and parent thread,
     * summing them together to get the bytes read of this task.
     */
    new Function0[Long] {
      private val bytesReadMap = new mutable.HashMap[Long, Long]()

      override def apply(): Long = {
        bytesReadMap.synchronized {
          bytesReadMap.put(Thread.currentThread().getId, f())
          bytesReadMap.map { case (k, v) =>
            v - (if (k == baseline._1) baseline._2 else 0)
          }.sum
        }
      }
    }
  }

  /**
   * Returns a function that can be called to find Hadoop FileSystem bytes written. If
   * getFSBytesWrittenOnThreadCallback is called from thread r at time t, the returned callback will
   * return the bytes written on r since t.
   *
   * @return None if the required method can't be found.
   */
  private[spark] def getFSBytesWrittenOnThreadCallback(): () => Long = {
    val threadStats = FileSystem.getAllStatistics.asScala.map(_.getThreadStatistics)
    val f = () => threadStats.map(_.getBytesWritten).sum
    val baselineBytesWritten = f()
    () => f() - baselineBytesWritten
  }

  /**
   * Get [[FileStatus]] objects for all leaf children (files) under the given base path. If the
   * given path points to a file, return a single-element collection containing [[FileStatus]] of
   * that file.
   */
  def listLeafStatuses(fs: FileSystem, basePath: Path): Seq[FileStatus] = {
    listLeafStatuses(fs, fs.getFileStatus(basePath))
  }

  /**
   * Get [[FileStatus]] objects for all leaf children (files) under the given base path. If the
   * given path points to a file, return a single-element collection containing [[FileStatus]] of
   * that file.
   */
  def listLeafStatuses(fs: FileSystem, baseStatus: FileStatus): Seq[FileStatus] = {
    def recurse(status: FileStatus): Seq[FileStatus] = {
      val (directories, leaves) = fs.listStatus(status.getPath).partition(_.isDirectory)
      leaves ++ directories.flatMap(f => listLeafStatuses(fs, f))
    }

    if (baseStatus.isDirectory) recurse(baseStatus) else Seq(baseStatus)
  }

  def listLeafDirStatuses(fs: FileSystem, basePath: Path): Seq[FileStatus] = {
    listLeafDirStatuses(fs, fs.getFileStatus(basePath))
  }

  def listLeafDirStatuses(fs: FileSystem, baseStatus: FileStatus): Seq[FileStatus] = {
    def recurse(status: FileStatus): Seq[FileStatus] = {
      val (directories, files) = fs.listStatus(status.getPath).partition(_.isDirectory)
      val leaves = if (directories.isEmpty) Seq(status) else Seq.empty[FileStatus]
      leaves ++ directories.flatMap(dir => listLeafDirStatuses(fs, dir))
    }

    assert(baseStatus.isDirectory)
    recurse(baseStatus)
  }

  def isGlobPath(pattern: Path): Boolean = {
    pattern.toString.exists("{}[]*?\\".toSet.contains)
  }

  def globPath(pattern: Path): Seq[Path] = {
    val fs = pattern.getFileSystem(conf)
    globPath(fs, pattern)
  }

  def globPath(fs: FileSystem, pattern: Path): Seq[Path] = {
    Option(fs.globStatus(pattern)).map { statuses =>
      statuses.map(_.getPath.makeQualified(fs.getUri, fs.getWorkingDirectory)).toSeq
    }.getOrElse(Seq.empty[Path])
  }

  def globPathIfNecessary(pattern: Path): Seq[Path] = {
    if (isGlobPath(pattern)) globPath(pattern) else Seq(pattern)
  }

  def globPathIfNecessary(fs: FileSystem, pattern: Path): Seq[Path] = {
    if (isGlobPath(pattern)) globPath(fs, pattern) else Seq(pattern)
  }

  /**
   * Lists all the files in a directory with the specified prefix, and does not end with the
   * given suffix. The returned {{FileStatus}} instances are sorted by the modification times of
   * the respective files.
   */
  def listFilesSorted(
      remoteFs: FileSystem,
      dir: Path,
      prefix: String,
      exclusionSuffix: String): Array[FileStatus] = {
    try {
      val fileStatuses = remoteFs.listStatus(dir,
        new PathFilter {
          override def accept(path: Path): Boolean = {
            val name = path.getName
            name.startsWith(prefix) && !name.endsWith(exclusionSuffix)
          }
        })
      Arrays.sort(fileStatuses, (o1: FileStatus, o2: FileStatus) =>
        Longs.compare(o1.getModificationTime, o2.getModificationTime))
      fileStatuses
    } catch {
      case NonFatal(e) =>
        logWarning("Error while attempting to list files from application staging dir", e)
        Array.empty
    }
  }

  private[spark] def getSuffixForCredentialsPath(credentialsPath: Path): Int = {
    val fileName = credentialsPath.getName
    fileName.substring(
      fileName.lastIndexOf(SparkHadoopUtil.SPARK_YARN_CREDS_COUNTER_DELIM) + 1).toInt
  }


  private val HADOOP_CONF_PATTERN = "(\\$\\{hadoopconf-[^\\}\\$\\s]+\\})".r.unanchored

  /**
   * Substitute variables by looking them up in Hadoop configs. Only variables that match the
   * ${hadoopconf- .. } pattern are substituted.
   */
  def substituteHadoopVariables(text: String, hadoopConf: Configuration): String = {
    text match {
      case HADOOP_CONF_PATTERN(matched) =>
        logDebug(text + " matched " + HADOOP_CONF_PATTERN)
        val key = matched.substring(13, matched.length() - 1) // remove ${hadoopconf- .. }
        val eval = Option[String](hadoopConf.get(key))
          .map { value =>
            logDebug("Substituted " + matched + " with " + value)
            text.replace(matched, value)
          }
        if (eval.isEmpty) {
          // The variable was not found in Hadoop configs, so return text as is.
          text
        } else {
          // Continue to substitute more variables.
          substituteHadoopVariables(eval.get, hadoopConf)
        }
      case _ =>
        logDebug(text + " didn't match " + HADOOP_CONF_PATTERN)
        text
    }
  }

  /**
   * Dump the credentials' tokens to string values.
   *
   * @param credentials credentials
   * @return an iterator over the string values. If no credentials are passed in: an empty list
   */
  private[spark] def dumpTokens(credentials: Credentials): Iterable[String] = {
    if (credentials != null) {
      credentials.getAllTokens.asScala.map(tokenToString)
    } else {
      Seq.empty
    }
  }

  /**
   * Convert a token to a string for logging.
   * If its an abstract delegation token, attempt to unmarshall it and then
   * print more details, including timestamps in human-readable form.
   *
   * @param token token to convert to a string
   * @return a printable string value.
   */
  private[spark] def tokenToString(token: Token[_ <: TokenIdentifier]): String = {
    val df = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT, Locale.US)
    val buffer = new StringBuilder(128)
    buffer.append(token.toString)
    try {
      val ti = token.decodeIdentifier
      buffer.append("; ").append(ti)
      ti match {
        case dt: AbstractDelegationTokenIdentifier =>
          // include human times and the renewer, which the HDFS tokens toString omits
          buffer.append("; Renewer: ").append(dt.getRenewer)
          buffer.append("; Issued: ").append(df.format(new Date(dt.getIssueDate)))
          buffer.append("; Max Date: ").append(df.format(new Date(dt.getMaxDate)))
        case _ =>
      }
    } catch {
      case e: IOException =>
        logDebug(s"Failed to decode $token: $e", e)
    }
    buffer.toString
  }

  def serialize(creds: Credentials): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(byteStream)
    creds.writeTokenStorageToStream(dataStream)
    byteStream.toByteArray
  }

  def deserialize(tokenBytes: Array[Byte]): Credentials = {
    val tokensBuf = new ByteArrayInputStream(tokenBytes)

    val creds = new Credentials()
    creds.readTokenStorageStream(new DataInputStream(tokensBuf))
    creds
  }

  def isProxyUser(ugi: UserGroupInformation): Boolean = {
    ugi.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.PROXY
  }

}

private[spark] object SparkHadoopUtil extends Logging {

  private lazy val instance = new SparkHadoopUtil

  val SPARK_YARN_CREDS_TEMP_EXTENSION = ".tmp"

  val SPARK_YARN_CREDS_COUNTER_DELIM = "-"

  /**
   * Number of records to update input metrics when reading from HadoopRDDs.
   *
   * Each update is potentially expensive because we need to use reflection to access the
   * Hadoop FileSystem API of interest (only available in 2.5), so we should do this sparingly.
   */
  private[spark] val UPDATE_INPUT_METRICS_INTERVAL_RECORDS = 1000

  /**
   * Name of the file containing the gateway's Hadoop configuration, to be overlayed on top of the
   * cluster's Hadoop config. It is up to the Spark code launching the application to create
   * this file if it's desired. If the file doesn't exist, it will just be ignored.
   */
  private[spark] val SPARK_HADOOP_CONF_FILE = "__spark_hadoop_conf__.xml"

  /**
   * Source for hive-site.xml configuration options.
   */
  private[deploy] val SOURCE_HIVE_SITE = "Set by Spark from hive-site.xml"

  /**
   * Source for configuration options set by spark when another source is
   * not explicitly declared.
   */
  private[spark] val SOURCE_SPARK = "Set by Spark"

  /**
   * Source for configuration options with `spark.hadoop.` prefix copied
   * from spark-defaults.
   */
  private[deploy] val SOURCE_SPARK_HADOOP =
    "Set by Spark from keys starting with 'spark.hadoop'"

  /*
   * The AWS Authentication environment variables documented in
   * https://docs.aws.amazon.com/sdkref/latest/guide/environment-variables.html.
   * There are alternative names defined in `com.amazonaws.SDKGlobalConfiguration`
   * and which are picked up by the authentication provider
   * `EnvironmentVariableCredentialsProvider`; those are not propagated.
   */

  /**
   * AWS Access key.
   */
  private[deploy] val ENV_VAR_AWS_ACCESS_KEY = "AWS_ACCESS_KEY_ID"

  /**
   * AWS Secret Key.
   */
  private[deploy] val ENV_VAR_AWS_SECRET_KEY = "AWS_SECRET_ACCESS_KEY"

  /**
   * AWS Session token.
   */
  private[deploy] val ENV_VAR_AWS_SESSION_TOKEN = "AWS_SESSION_TOKEN"

  /**
   * Source for configuration options with `spark.hive.` prefix copied
   * from spark-defaults.
   */
  private[deploy] val SOURCE_SPARK_HIVE = "Set by Spark from keys starting with 'spark.hive'"

  /**
   * Hadoop configuration options set to their default values.
   */
  private[deploy] val SET_TO_DEFAULT_VALUES = "Set by Spark to default values"

  def get: SparkHadoopUtil = instance

  /**
   * Returns a Configuration object with Spark configuration applied on top. Unlike
   * the instance method, this will always return a Configuration instance, and not a
   * cluster manager-specific type.
   * The configuration will load all default values set in core-default.xml,
   * and if found on the classpath, those of core-site.xml.
   * This is done before the spark overrides are applied.
   */
  private[spark] def newConfiguration(conf: SparkConf): Configuration = {
    val hadoopConf = new Configuration()
    appendS3AndSparkHadoopHiveConfigurations(conf, hadoopConf)
    hadoopConf
  }

  private def appendS3AndSparkHadoopHiveConfigurations(
      conf: SparkConf,
      hadoopConf: Configuration): Unit = {
    // Note: this null check is around more than just access to the "conf" object to maintain
    // the behavior of the old implementation of this code, for backwards compatibility.
    if (conf != null) {
      appendS3CredentialsFromEnvironment(hadoopConf,
        System.getenv(ENV_VAR_AWS_ACCESS_KEY),
        System.getenv(ENV_VAR_AWS_SECRET_KEY),
        System.getenv(ENV_VAR_AWS_SESSION_TOKEN))
      appendHiveConfigs(hadoopConf)
      appendSparkHadoopConfigs(conf, hadoopConf)
      appendSparkHiveConfigs(conf, hadoopConf)
      val bufferSize = conf.get(BUFFER_SIZE).toString
      hadoopConf.set("io.file.buffer.size", bufferSize, BUFFER_SIZE.key)
    }
  }

  /**
   * Append any AWS secrets from the environment variables
   * if both `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are set.
   * If these two are set and `AWS_SESSION_TOKEN` is also set,
   * then `fs.s3a.session.token`.
   * The option is set with a source string which includes the hostname
   * on which it was set. This can help debug propagation issues.
   *
   * @param hadoopConf configuration to patch
   * @param keyId key ID or null
   * @param accessKey secret key
   * @param sessionToken session token.
   */
  // Exposed for testing
  private[deploy] def appendS3CredentialsFromEnvironment(
      hadoopConf: Configuration,
      keyId: String,
      accessKey: String,
      sessionToken: String): Unit = {
    if (keyId != null && accessKey != null) {
      // source prefix string; will have environment variable added
      val source = SOURCE_SPARK + " on " + InetAddress.getLocalHost.toString + " from "
      hadoopConf.set("fs.s3.awsAccessKeyId", keyId, source + ENV_VAR_AWS_ACCESS_KEY)
      hadoopConf.set("fs.s3n.awsAccessKeyId", keyId, source + ENV_VAR_AWS_ACCESS_KEY)
      hadoopConf.set("fs.s3a.access.key", keyId, source + ENV_VAR_AWS_ACCESS_KEY)
      hadoopConf.set("fs.s3.awsSecretAccessKey", accessKey, source + ENV_VAR_AWS_SECRET_KEY)
      hadoopConf.set("fs.s3n.awsSecretAccessKey", accessKey, source + ENV_VAR_AWS_SECRET_KEY)
      hadoopConf.set("fs.s3a.secret.key", accessKey, source + ENV_VAR_AWS_SECRET_KEY)

      // look for session token if the other variables were set
      if (sessionToken != null) {
        hadoopConf.set("fs.s3a.session.token", sessionToken,
          source + ENV_VAR_AWS_SESSION_TOKEN)
      }
    }
  }

  private lazy val hiveConfKeys = {
    val configFile = Utils.getContextOrSparkClassLoader.getResource("hive-site.xml")
    if (configFile != null) {
      val conf = new Configuration(false)
      conf.addResource(configFile)
      conf.iterator().asScala.toSeq
    } else {
      Nil
    }
  }

  private def appendHiveConfigs(hadoopConf: Configuration): Unit = {
    hiveConfKeys.foreach { kv =>
      hadoopConf.set(kv.getKey, kv.getValue, SOURCE_HIVE_SITE)
    }
  }

  private def appendSparkHadoopConfigs(conf: SparkConf, hadoopConf: Configuration): Unit = {
    // Copy any "spark.hadoop.foo=bar" spark properties into conf as "foo=bar"
    for ((key, value) <- conf.getAll if key.startsWith("spark.hadoop.")) {
      hadoopConf.set(key.substring("spark.hadoop.".length), value,
        SOURCE_SPARK_HADOOP)
    }
    val setBySpark = SET_TO_DEFAULT_VALUES
    if (conf.getOption("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version").isEmpty) {
      hadoopConf.set("mapreduce.fileoutputcommitter.algorithm.version", "1", setBySpark)
    }
    // In Hadoop 3.3.1, HADOOP-17597 starts to throw exceptions by default
    // this has been reverted in 3.3.2 (HADOOP-17928); setting it to
    // true here is harmless
    if (conf.getOption("spark.hadoop.fs.s3a.downgrade.syncable.exceptions").isEmpty) {
      hadoopConf.set("fs.s3a.downgrade.syncable.exceptions", "true", setBySpark)
    }
    // In Hadoop 3.3.1, AWS region handling with the default "" endpoint only works
    // in EC2 deployments or when the AWS CLI is installed.
    // The workaround is to set the name of the S3 endpoint explicitly,
    // if not already set. See HADOOP-17771.
    if (hadoopConf.get("fs.s3a.endpoint", "").isEmpty &&
      hadoopConf.get("fs.s3a.endpoint.region") == null) {
      // set to US central endpoint which can also connect to buckets
      // in other regions at the expense of a HEAD request during fs creation
      hadoopConf.set("fs.s3a.endpoint", "s3.amazonaws.com", setBySpark)
    }
  }

  private def appendSparkHiveConfigs(conf: SparkConf, hadoopConf: Configuration): Unit = {
    // Copy any "spark.hive.foo=bar" spark properties into conf as "hive.foo=bar"
    for ((key, value) <- conf.getAll if key.startsWith("spark.hive.")) {
      hadoopConf.set(key.substring("spark.".length), value, SOURCE_SPARK_HIVE)
    }
  }

  /**
   * Extract the sources of a configuration key, or a default value if
   * the key is not found or it has no known sources.
   * Note that options provided by credential providers (JCEKS stores etc)
   * are not resolved, so values retrieved by Configuration.getPassword()
   * may not be recorded as having an origin.
   * @param hadoopConf hadoop configuration to examine.
   * @param key key to look up
   * @return the origin of the current entry in the configuration, or the empty string.
   */
  def propertySources(hadoopConf: Configuration, key: String): String = {
    val sources = hadoopConf.getPropertySources(key)
    if (sources != null && sources.nonEmpty) {
      sources.mkString(",")
    } else {
      ""
    }
  }

  // scalastyle:off line.size.limit
  /**
   * Create a file on the given file system, optionally making sure erasure coding is disabled.
   *
   * Disabling EC can be helpful as HDFS EC doesn't support hflush(), hsync(), or append().
   * https://hadoop.apache.org/docs/r3.0.0/hadoop-project-dist/hadoop-hdfs/HDFSErasureCoding.html#Limitations
   */
  // scalastyle:on line.size.limit
  def createFile(fs: FileSystem, path: Path, allowEC: Boolean): FSDataOutputStream = {
    if (allowEC) {
      fs.create(path)
    } else {
      try {
        // Use reflection as this uses APIs only available in Hadoop 3
        val builderMethod = fs.getClass().getMethod("createFile", classOf[Path])
        // the builder api does not resolve relative paths, nor does it create parent dirs, while
        // the old api does.
        if (!fs.mkdirs(path.getParent())) {
          throw new IOException(s"Failed to create parents of $path")
        }
        val qualifiedPath = fs.makeQualified(path)
        val builder = builderMethod.invoke(fs, qualifiedPath)
        val builderCls = builder.getClass()
        // this may throw a NoSuchMethodException if the path is not on hdfs
        val replicateMethod = builderCls.getMethod("replicate")
        val buildMethod = builderCls.getMethod("build")
        val b2 = replicateMethod.invoke(builder)
        buildMethod.invoke(b2).asInstanceOf[FSDataOutputStream]
      } catch {
        case  _: NoSuchMethodException =>
          // No createFile() method, we're using an older hdfs client, which doesn't give us control
          // over EC vs. replication.  Older hdfs doesn't have EC anyway, so just create a file with
          // old apis.
          fs.create(path)
      }
    }
  }

}
