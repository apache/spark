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

package org.apache.spark.sql.kafka010

import java.util.{Properties, SortedMap}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import com.codahale.metrics._
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.internal.Logging

class KafkaReporter(
    registry: MetricRegistry,
    kafkaEndpoint: String,
    kafkaTopic: String,
    properties: Properties) extends ScheduledReporter (
  registry,
  "kafka-reporter",
  MetricFilter.ALL,
  TimeUnit.SECONDS,
  TimeUnit.MILLISECONDS) with Logging {

  var producer: Option[KafkaProducer[String, String]] = None
  val gaugesLabel = ("type" -> "gauge")
  val countersLabel = ("type" -> "counter")
  val metersLabel = ("type" -> "meter")
  val histogramsLabel = ("type" -> "histogram")
  val timersLabel = ("type" -> "timer")
  val NAME = "Name"
  val VALUE = "value"
  val COUNT = "Count"
  val KAFKA_TOPIC = kafkaTopic
  val KAFKA_ENDPOINTS = kafkaEndpoint

  /**
   * Any user properties set in the metrics config file
   * prodconf_foo=this.setting.key=value
   * prodconf_bar=this.setting.key2=value2
   */
  private def setUserProperties(props: Properties): Unit = {
    properties.entrySet().asScala.foreach { entry =>
      if (entry.getKey().asInstanceOf[String].startsWith("prodconf_")) {
        val entryValue = entry.getValue()
        val kv = entryValue.toString.split('=')
        if (kv.length != 2) {
          logError(s"Ignoring bad prodconf_* setting: ${entryValue}")
        } else {
          props.put(kv(0), kv(1))
        }
      }
    }
  }

  private def topicExists(bootstrap_servers: String, topic: String): Boolean = {
    val prop = new Properties()
    prop.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers)
    AdminClient.create(prop).listTopics().names().get().contains(topic)
  }

  override def start(period: Long, unit: TimeUnit): Unit = {
    val KAFKA_KEY_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer"
    val KAFKA_VALUE_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer"

    super.start(period, unit)
    Try {
      logInfo(s"Opening Kafka endpoint $KAFKA_ENDPOINTS")
      val props = new Properties()
      // Set these, but may be overridden in setUserProperties
      val clientIdValue = (s"KafkaReporter-$KAFKA_ENDPOINTS-$KAFKA_TOPIC").replace(':', '-')
      props.put(ProducerConfig.CLIENT_ID_CONFIG, clientIdValue)
      // load any KafkaProducer conf settings passed in from metrics config
      setUserProperties(props)
      // Anything here takes precedence over user settings
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ENDPOINTS)
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_KEY_SERIALIZER_CLASS)
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_VALUE_SERIALIZER_CLASS)
      logInfo(s"Kafka producer properties:\n$props")
      if (topicExists(KAFKA_ENDPOINTS, KAFKA_TOPIC)) {
        logInfo(s"kafka topic $KAFKA_TOPIC exists")
      } else {
        logInfo(s"kafka topic $KAFKA_TOPIC doesn't exist")
      }
      new KafkaProducer[String, String](props)
    } match {
      case Success(kafka_producer) =>
        logInfo(s"Kafka producer connected to $KAFKA_ENDPOINTS")
        producer = Some(kafka_producer)
      case Failure(err) =>
        logError(s"Failure opening Kafka endpoint $KAFKA_ENDPOINTS:\n$err")
    }
  }

  override def stop(): Unit = {
    logInfo(s"Stopping Kafka reporter at $KAFKA_ENDPOINTS")
    super.stop()
  }

  def report(
              gauges: SortedMap[String, Gauge[_]],
              counters: SortedMap[String, Counter],
              histograms: SortedMap[String, Histogram],
              meters: SortedMap[String, Meter],
              timers: SortedMap[String, Timer]): Unit = {
    if (producer.isEmpty) {
      logError(s"Failed Kafka client for $KAFKA_ENDPOINTS: metric output ignored")
    } else {
      // Publish metric output to the kafka topic
      val prod = producer.get
      gauges.entrySet().asScala.foreach {
        entry => prod.send(metricRec(entry.getKey(), gaugeJSON(entry.getKey, entry.getValue)))
      }
      counters.entrySet().asScala.foreach {
        entry => prod.send(metricRec(entry.getKey(), counterJSON(entry.getKey, entry.getValue)))
      }
      histograms.entrySet().asScala.foreach {
        entry => prod.send(metricRec(entry.getKey(), histJSON(entry.getKey, entry.getValue)))
      }
      meters.entrySet().asScala.foreach {
        entry => prod.send(metricRec(entry.getKey(), meterJSON(entry.getKey, entry.getValue)))
      }
      timers.entrySet().asScala.foreach {
        entry => prod.send(metricRec(entry.getKey(), timerJSON(entry.getKey, entry.getValue)))
      }
    }
  }

  private def metricRec(key: String, value: String) =
    new ProducerRecord[String, String](KAFKA_TOPIC, key, value)

  private def gaugeJSON(key: String, gauge: Gauge[_]): String = {
    val name = (NAME -> key)
    val value = gauge.getValue().toString
    compact(render(gaugesLabel ~ name ~ (VALUE -> value)))
  }

  private def counterJSON(key: String, counter: Counter): String = {
    val name = (NAME -> key)
    compact(render(countersLabel ~ name ~ (VALUE -> counter.getCount())))
  }

  private def histJSON(key: String, hist: Histogram): String = {
    val HISTOGRAM_QUANTILES = "histquantiles"
    val histro = samplingAST(hist, HISTOGRAM_QUANTILES).get
    val countr = (COUNT -> hist.getCount())
    compact(render(histogramsLabel ~ (NAME -> key) ~ (VALUE -> (countr ~ histro))))
  }

  private def meterJSON(key: String, meter: Meter): String = {
    val meterd = meteredAST(meter)
    val countr = (COUNT -> meter.getCount())
    compact(render(metersLabel ~ (NAME -> key) ~ (VALUE -> (countr ~ meterd))))
  }

  private def timerJSON(key: String, timer: Timer): String = {
    val TIMER_QUANTILES = "timerquantiles"
    val histogrm = samplingAST(timer, TIMER_QUANTILES).get
    val meterd = meteredAST(timer)
    val countr = (COUNT -> timer.getCount())
    compact(render(timersLabel ~ (NAME -> key) ~ (VALUE -> (countr ~ histogrm ~ meterd))))
  }

  private def samplingAST(hist: Sampling, qsetting: String): Option[JObject] = {
    val DEFAULT_PERCENTILES = "0.5,0.75,0.95,0.98,0.99,0.999"
    val snapshot = hist.getSnapshot()
    Try {
      val quantile_settings = Option(properties.getProperty(qsetting)).getOrElse(
        DEFAULT_PERCENTILES)
      val qKey = quantile_settings.split(",").map(_.toDouble).toVector
      val qValue = qKey.map { z => snapshot.getValue(z) }
      (qKey, qValue)
    } match {
      case Failure(_) =>
        val quantile_settings = properties.getProperty(qsetting)
        logError(s"Bad quantile setting: $quantile_settings\nIgnoring histogram metric output")
        None
      case Success((qKey, qValue)) =>
        val histogrm =
          ("QPercentiles" -> qKey) ~
            ("QPercentile Values" -> qValue) ~
            ("Min" -> snapshot.getMin()) ~
            ("Max" -> snapshot.getMax()) ~
            ("Mean" -> snapshot.getMean()) ~
            ("StdDev" -> snapshot.getStdDev())
        Some(histogrm)
    }
  }

  private def meteredAST(meter: Metered): JObject = {
    val metrd =
      ("1-minute rate" -> meter.getOneMinuteRate()) ~
        ("5-minute rate" -> meter.getFiveMinuteRate()) ~
        ("15-minute rate" -> meter.getFifteenMinuteRate()) ~
        ("Mean rate" -> meter.getMeanRate())
    metrd
  }
}
