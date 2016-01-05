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

package org.apache.spark.scheduler

import java.net.URI

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

import scala.util.Try

/**
  * An interface to be implemented if a custom scheduler is to be used. A class name of the
  * particular implementation has to be added to Spark configuration at
  * `spark.scheduler.factory.<name>=<class-name>`. Name is the master URI scheme which
  * will make SparkContext use the particular scheduler factory.
  */
trait SchedulerFactory {
  /**
    * The method creates TaskScheduler. Currently it just needs to create instance of
    * [[TaskSchedulerImpl]].
    */
  def createScheduler(sc: SparkContext): TaskScheduler

  /**
    * The method creates a custom scheduler backend. The custom backend must implement
    * [[CoarseGrainedSchedulerBackend]].
    */
  def createSchedulerBackend(
      scheduler: TaskScheduler, sc: SparkContext, uri: URI): CoarseGrainedSchedulerBackend
}

private[spark] object SchedulerFactory {
  private val schedulerFactoryPattern = """^spark\.scheduler\.factory\.(.+)$""".r
  private val schedulerClientPattern = """^spark\.scheduler\.client\.(.+)$""".r

  def getSchedulerFactoryClassName(
      conf: Iterable[(String, String)],
      schedulerName: String): Option[String] =
    conf.collectFirst {
      case (schedulerFactoryPattern(name), clazzName) if name.equalsIgnoreCase(schedulerName) =>
        clazzName
    }

  def getSchedulerClientClassName(
      conf: Iterable[(String, String)],
      schedulerName: String): Option[String] =
    conf.collectFirst {
      case (schedulerClientPattern(name), clazzName) if name.equalsIgnoreCase(schedulerName) =>
        clazzName
    }

  def unapply(masterUri: String): Option[String] = {
    for (uri <- Try(new URI(masterUri)).toOption) yield uri.getScheme
  }

}
