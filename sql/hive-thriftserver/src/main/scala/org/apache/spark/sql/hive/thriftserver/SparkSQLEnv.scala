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

package org.apache.spark.sql.hive.thriftserver

import java.io.{InputStream, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveUtils}
import org.apache.spark.util.Utils

/** A singleton object for the master program. The slaves should not access this. */
private[hive] object SparkSQLEnv extends Logging {

  var sqlContext: SQLContext = _
  var sparkContext: SparkContext = _

  private var stdIn: InputStream = System.in
  private var stdOut: PrintStream = System.out
  private var stdErr: PrintStream = System.out

  def setOut(out: PrintStream): Unit = stdOut = out
  def setErr(err: PrintStream): Unit = stdErr = err
  def setIn(in: InputStream): Unit = stdIn = in

  // scalastyle:off println
  def printStream: String => Unit = (str: String) => scala.Console.withOut(stdOut)(println(str))
  def printErrStream: String => Unit = (str: String) => scala.Console.withOut(stdErr)(println(str))
  // scalastyle:on println

  def readStream: String => String = {
    prefix =>
      scala.Console.withOut(stdOut) {
        printf(prefix)
        stdOut.flush()
      }
      scala.io.Source.fromInputStream(stdIn).bufferedReader().readLine()
  }

  def init(conf: SparkConf = null): Unit = {
    logDebug("Initializing SparkSQLEnv")
    if (sqlContext == null) {
      val sparkConf = if (conf == null) {
        new SparkConf(loadDefaults = true)
      } else {
        conf
      }
      // If user doesn't specify the appName, we want to get [SparkSQL::localHostName] instead of
      // the default appName [SparkSQLCLIDriver] in cli or beeline.
      val maybeAppName = sparkConf
        .getOption("spark.app.name")
        .filterNot(_ == classOf[SparkSQLCLIDriver].getName)
        .filterNot(_ == classOf[HiveThriftServer2].getName)

      sparkConf
        .setAppName(maybeAppName.getOrElse(s"SparkSQL::${Utils.localHostName()}"))

      val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()
      sparkContext = sparkSession.sparkContext
      sqlContext = sparkSession.sqlContext

      // SPARK-29604: force initialization of the session state with the Spark class loader,
      // instead of having it happen during the initialization of the Hive client (which may use a
      // different class loader).
      sparkSession.sessionState

      val metadataHive = sparkSession
        .sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client
      metadataHive.setOut(new PrintStream(System.out, true, UTF_8.name()))
      metadataHive.setInfo(new PrintStream(System.err, true, UTF_8.name()))
      metadataHive.setError(new PrintStream(System.err, true, UTF_8.name()))
      sparkSession.conf.set(HiveUtils.FAKE_HIVE_VERSION.key, HiveUtils.builtinHiveVersion)
    }
  }

  /** Cleans up and shuts down the Spark SQL environments. */
  def stop(): Unit = {
    logDebug("Shutting down Spark SQL Environment")
    // Stop the SparkContext
    if (SparkSQLEnv.sparkContext != null) {
      sparkContext.stop()
      sparkContext = null
      sqlContext = null
    }
  }
}
