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

package org.apache.spark.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.github.sps.metrics.OpenTsdbReporter
import com.github.sps.metrics.opentsdb.OpenTsdb
import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.metrics.MetricsSystem

/**
 * Created by kapil on 1/12/15.
 */
private[spark] class OpenTsdbSink(val property: Properties, val registry: MetricRegistry,
                   securityMgr: SecurityManager) extends Sink with Logging {
  val OPENTSDB_DEFAULT_PERIOD = 10
  val OPENTSDB_DEFAULT_UNIT = "SECONDS"
  val OPENTSDB_DEFAULT_PREFIX = ""
  val OPENTSDB_DEFAULT_TAG_NAME_1 = "appId"
  val OPENTSDB_DEFAULT_TAG_VALUE_1 = SparkEnv.get.conf.getAppId

  val OPENTSDB_KEY_HOST = "host"
  val OPENTSDB_KEY_PORT = "port"
  val OPENTSDB_KEY_PERIOD = "period"
  val OPENTSDB_KEY_UNIT = "unit"
  val OPENTSDB_KEY_PREFIX = "prefix"

  val OPENTSDB_KEY_TAG_NAME_1 = "tagName1"
  val OPENTSDB_KEY_TAG_VALUE_1 = "tagValue1"
  val OPENTSDB_KEY_TAG_NAME_2 = "tagName2"
  val OPENTSDB_KEY_TAG_VALUE_2 = "tagValue2"
  val OPENTSDB_KEY_TAG_NAME_3 = "tagName3"
  val OPENTSDB_KEY_TAG_VALUE_3 = "tagValue3"
  val OPENTSDB_KEY_TAG_NAME_4 = "tagName4"
  val OPENTSDB_KEY_TAG_VALUE_4 = "tagValue4"
  val OPENTSDB_KEY_TAG_NAME_5 = "tagName5"
  val OPENTSDB_KEY_TAG_VALUE_5 = "tagValue5"
  val OPENTSDB_KEY_TAG_NAME_6 = "tagName6"
  val OPENTSDB_KEY_TAG_VALUE_6 = "tagValue6"
  val OPENTSDB_KEY_TAG_NAME_7 = "tagName7"
  val OPENTSDB_KEY_TAG_VALUE_7 = "tagValue7"
  val OPENTSDB_KEY_TAG_NAME_8 = "tagName8"
  val OPENTSDB_KEY_TAG_VALUE_8 = "tagValue8"

  def propertyToOption(prop: String): Option[String] = Option(property.getProperty(prop))

  if (!propertyToOption(OPENTSDB_KEY_HOST).isDefined) {
    throw new Exception(s"OpenTSDB sink requires '$OPENTSDB_KEY_HOST' property.")
  }

  if (!propertyToOption(OPENTSDB_KEY_PORT).isDefined) {
    throw new Exception(s"OpenTSDB sink requires '$OPENTSDB_KEY_PORT' property.")
  }

  val host = propertyToOption(OPENTSDB_KEY_HOST).get
  val port = propertyToOption(OPENTSDB_KEY_PORT).get.toInt

  val pollPeriod = propertyToOption(OPENTSDB_KEY_PERIOD) match {
    case Some(s) => s.toInt
    case None => OPENTSDB_DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = propertyToOption(OPENTSDB_KEY_UNIT) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase())
    case None => TimeUnit.valueOf(OPENTSDB_DEFAULT_UNIT)
  }

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val prefix = propertyToOption(OPENTSDB_KEY_PREFIX).getOrElse(OPENTSDB_DEFAULT_PREFIX)

  val tagName1 = propertyToOption(OPENTSDB_KEY_TAG_NAME_1) match {
    case Some(n) => n
    case None =>
      logWarning(s"""'$OPENTSDB_KEY_TAG_NAME_1' property not specified for OpenTSDB sink,
           | using '$OPENTSDB_DEFAULT_TAG_NAME_1'"""".stripMargin)
      OPENTSDB_DEFAULT_TAG_NAME_1
  }

  val tagValue1 = propertyToOption(OPENTSDB_KEY_TAG_NAME_1) match {
    case Some(n) =>
      propertyToOption(OPENTSDB_KEY_TAG_VALUE_1) match {
        case Some(v) => v
        case None =>
          logWarning(s"""'$OPENTSDB_KEY_TAG_VALUE_1' property not specified for OpenTSDB sink,
               |using '$OPENTSDB_DEFAULT_TAG_VALUE_1'""".stripMargin)
          OPENTSDB_DEFAULT_TAG_VALUE_1
      }
    case None =>
      propertyToOption(OPENTSDB_KEY_TAG_VALUE_1) match {
        case Some(v) =>
          throw new Exception(
            s"""'$OPENTSDB_KEY_TAG_VALUE_1' property cannot be specified for OpenTSDB sink
               |without specifying '$OPENTSDB_KEY_TAG_NAME_1' property.""".stripMargin)
        case None =>
          logWarning(s"""'$OPENTSDB_KEY_TAG_VALUE_1' property not specified for OpenTSDB sink,
               |using '$OPENTSDB_DEFAULT_TAG_VALUE_1'""".stripMargin)
          OPENTSDB_DEFAULT_TAG_VALUE_1
      }
  }

  private val tags = new java.util.HashMap[String, String]()

  private def getTags(): java.util.Map[String, String] = {
    tags.put(tagName1, tagValue1)
    updateTagsForTag(OPENTSDB_KEY_TAG_NAME_2, OPENTSDB_KEY_TAG_VALUE_2)
    updateTagsForTag(OPENTSDB_KEY_TAG_NAME_3, OPENTSDB_KEY_TAG_VALUE_3)
    updateTagsForTag(OPENTSDB_KEY_TAG_NAME_4, OPENTSDB_KEY_TAG_VALUE_4)
    updateTagsForTag(OPENTSDB_KEY_TAG_NAME_5, OPENTSDB_KEY_TAG_VALUE_5)
    updateTagsForTag(OPENTSDB_KEY_TAG_NAME_6, OPENTSDB_KEY_TAG_VALUE_6)
    updateTagsForTag(OPENTSDB_KEY_TAG_NAME_7, OPENTSDB_KEY_TAG_VALUE_7)
    updateTagsForTag(OPENTSDB_KEY_TAG_NAME_8, OPENTSDB_KEY_TAG_VALUE_8)
    tags
  }

  private def updateTagsForTag(tagName: String, tagValue: String): Unit = {
    propertyToOption(tagName) match {
      case Some(n) => propertyToOption(tagValue) match {
        case Some(v) => tags.put(n, v)
        case None =>
          throw new Exception(
            s"OpenTSDB sink requires '$tagValue' property when '$tagName' property is specified."
          )
      }
      case None =>
    }
  }

  val openTsdb = OpenTsdb.forService("http://" + host + ":" + port).create()

  val reporter: OpenTsdbReporter = OpenTsdbReporter.forRegistry(registry)
    .prefixedWith(prefix)
    .withTags(getTags)
    .build(openTsdb)

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}
