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

import scala.collection.JavaConversions._

import org.apache.spark.scheduler.StatsReportListener
import org.apache.spark.sql.hive.{HiveShim, HiveContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}

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

      sparkConf
        .setAppName(s"SparkSQL::${java.net.InetAddress.getLocalHost.getHostName}")
        .set("spark.sql.hive.version", HiveShim.version)
        .set(
          "spark.serializer",
          maybeSerializer.getOrElse("org.apache.spark.serializer.KryoSerializer"))
        .set(
          "spark.kryo.referenceTracking",
          maybeKryoReferenceTracking.getOrElse("false"))

      sparkContext = new SparkContext(sparkConf)
      sparkContext.addSparkListener(new StatsReportListener())
      hiveContext = new HiveContext(sparkContext)

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
