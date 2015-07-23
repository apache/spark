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

import java.io.PrintStream

import scala.collection.JavaConversions._

import org.apache.spark.scheduler.StatsReportListener
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.util.Utils

/** A singleton object for the master program. The slaves should not access this. */
private[hive] object SparkSQLEnv extends Logging {
  logDebug("Initializing SparkSQLEnv")

  var hiveContext: HiveContext = _
  var sparkContext: SparkContext = _

  def init() {
    if (hiveContext == null) {
      val sparkConf = new SparkConf(loadDefaults = true)
      val maybeSerializer = sparkConf.getOption("spark.serializer")
      val maybeKryoReferenceTracking = sparkConf.getOption("spark.kryo.referenceTracking")
      // If user doesn't specify the appName, we want to get [SparkSQL::localHostName] instead of
      // the default appName [SparkSQLCLIDriver] in cli or beeline.
      val maybeAppName = sparkConf
        .getOption("spark.app.name")
        .filterNot(_ == classOf[SparkSQLCLIDriver].getName)

      sparkConf
        .setAppName(maybeAppName.getOrElse(s"SparkSQL::${Utils.localHostName()}"))
        .set(
          "spark.serializer",
          maybeSerializer.getOrElse("org.apache.spark.serializer.KryoSerializer"))
        .set(
          "spark.kryo.referenceTracking",
          maybeKryoReferenceTracking.getOrElse("false"))

      sparkContext = new SparkContext(sparkConf)
      sparkContext.addSparkListener(new StatsReportListener())
      hiveContext = new HiveContext(sparkContext)

      hiveContext.metadataHive.setOut(new PrintStream(System.out, true, "UTF-8"))
      hiveContext.metadataHive.setInfo(new PrintStream(System.err, true, "UTF-8"))
      hiveContext.metadataHive.setError(new PrintStream(System.err, true, "UTF-8"))

      hiveContext.setConf("spark.sql.hive.version", HiveContext.hiveExecutionVersion)

      if (log.isDebugEnabled) {
        hiveContext.hiveconf.getAllProperties.toSeq.sorted.foreach { case (k, v) =>
          logDebug(s"HiveConf var: $k=$v")
        }
      }
    }
  }

  /** Cleans up and shuts down the Spark SQL environments. */
  def stop() {
    logDebug("Shutting down Spark SQL Environment")
    // Stop the SparkContext
    if (SparkSQLEnv.sparkContext != null) {
      sparkContext.stop()
      sparkContext = null
      hiveContext = null
    }
  }
}
