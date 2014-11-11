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
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.FileSystem.Statistics
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.{Logging, SparkContext, SparkConf, SparkException}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils

import scala.collection.JavaConversions._

/**
 * :: DeveloperApi ::
 * Contains util methods to interact with Hadoop from Spark.
 */
@DeveloperApi
class SparkHadoopUtil extends Logging {
  val conf: Configuration = newConfiguration(new SparkConf())
  UserGroupInformation.setConfiguration(conf)

  /**
   * Runs the given function with a Hadoop UserGroupInformation as a thread local variable
   * (distributed to child threads), used for authenticating HDFS and YARN calls.
   *
   * IMPORTANT NOTE: If this function is going to be called repeated in the same process
   * you need to look https://issues.apache.org/jira/browse/HDFS-3545 and possibly
   * do a FileSystem.closeAllForUGI in order to avoid leaking Filesystems
   */
  def runAsSparkUser(func: () => Unit) {
    val user = Option(System.getenv("SPARK_USER")).getOrElse(SparkContext.SPARK_UNKNOWN_USER)
    if (user != SparkContext.SPARK_UNKNOWN_USER) {
      logDebug("running as user: " + user)
      val ugi = UserGroupInformation.createRemoteUser(user)
      transferCredentials(UserGroupInformation.getCurrentUser(), ugi)
      ugi.doAs(new PrivilegedExceptionAction[Unit] {
        def run: Unit = func()
      })
    } else {
      logDebug("running as SPARK_UNKNOWN_USER")
      func()
    }
  }

  def transferCredentials(source: UserGroupInformation, dest: UserGroupInformation) {
    for (token <- source.getTokens()) {
      dest.addToken(token)
    }
  }

  @Deprecated
  def newConfiguration(): Configuration = newConfiguration(null)

  /**
   * Return an appropriate (subclass) of Configuration. Creating config can initializes some Hadoop
   * subsystems.
   */
  def newConfiguration(conf: SparkConf): Configuration = {
    val hadoopConf = new Configuration()

    // Note: this null check is around more than just access to the "conf" object to maintain
    // the behavior of the old implementation of this code, for backwards compatibility.
    if (conf != null) {
      // Explicitly check for S3 environment variables
      if (System.getenv("AWS_ACCESS_KEY_ID") != null &&
          System.getenv("AWS_SECRET_ACCESS_KEY") != null) {
        hadoopConf.set("fs.s3.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY_ID"))
        hadoopConf.set("fs.s3n.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY_ID"))
        hadoopConf.set("fs.s3.awsSecretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"))
        hadoopConf.set("fs.s3n.awsSecretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"))
      }
      // Copy any "spark.hadoop.foo=bar" system properties into conf as "foo=bar"
      conf.getAll.foreach { case (key, value) =>
        if (key.startsWith("spark.hadoop.")) {
          hadoopConf.set(key.substring("spark.hadoop.".length), value)
        }
      }
      val bufferSize = conf.get("spark.buffer.size", "65536")
      hadoopConf.set("io.file.buffer.size", bufferSize)
    }

    hadoopConf
  }

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

  def loginUserFromKeytab(principalName: String, keytabFilename: String) {
    UserGroupInformation.loginUserFromKeytab(principalName, keytabFilename)
  }

  /**
   * Returns a function that can be called to find Hadoop FileSystem bytes read. If
   * getFSBytesReadOnThreadCallback is called from thread r at time t, the returned callback will
   * return the bytes read on r since t.  Reflection is required because thread-level FileSystem
   * statistics are only available as of Hadoop 2.5 (see HADOOP-10688).
   * Returns None if the required method can't be found.
   */
  private[spark] def getFSBytesReadOnThreadCallback(path: Path, conf: Configuration)
    : Option[() => Long] = {
    val qualifiedPath = path.getFileSystem(conf).makeQualified(path)
    val scheme = qualifiedPath.toUri().getScheme()
    val stats = FileSystem.getAllStatistics().filter(_.getScheme().equals(scheme))
    try {
      val threadStats = stats.map(Utils.invoke(classOf[Statistics], _, "getThreadStatistics"))
      val statisticsDataClass =
        Class.forName("org.apache.hadoop.fs.FileSystem$Statistics$StatisticsData")
      val getBytesReadMethod = statisticsDataClass.getDeclaredMethod("getBytesRead")
      val f = () => threadStats.map(getBytesReadMethod.invoke(_).asInstanceOf[Long]).sum
      val baselineBytesRead = f()
      Some(() => f() - baselineBytesRead)
    } catch {
      case e: NoSuchMethodException => {
        logDebug("Couldn't find method for retrieving thread-level FileSystem input data", e)
        None
      }
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
       case e: Exception => throw new SparkException("Unable to load YARN support", e)
      }
    } else {
      new SparkHadoopUtil
    }
  }

  def get: SparkHadoopUtil = {
    hadoop
  }
}
