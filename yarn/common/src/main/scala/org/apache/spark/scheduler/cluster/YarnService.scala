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

package org.apache.spark.scheduler.cluster

import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.spark.{Logging, SparkContext}
import scala.collection.mutable.LinkedList

private [spark] trait YarnService {
  // For Yarn services, SparkContext, and ApplicationId is the basic info required.
  // May change upon new services added.
  def start(sc: SparkContext, appId: ApplicationId): Unit
  def stop: Unit
}

private[spark] object YarnServices extends Logging{
  var services: LinkedList[YarnService] = _
  def start(sc: SparkContext, appId: ApplicationId) {
    val sNames = sc.getConf.getOption("spark.yarn.services")
    sNames match {
      case Some(names) =>
        services = new LinkedList[YarnService]
        val sClasses = names.split(",")
        sClasses.foreach {
          sClass => {
            try {
              val instance = Class.forName(sClass)
                .newInstance()
                .asInstanceOf[YarnService]
              instance.start(sc, appId)
              services :+= instance
              logInfo("Service " + sClass + " started")
            } catch {
              case e: Exception =>
                logWarning("Cannot start Yarn service $sClass ", e)
            }
          }
        }
      case _ =>
    }
  }
  //stop all services
  def stop() {
    if (services != null) {
      services.foreach(_.stop)
    }
  }
}

