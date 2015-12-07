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

package org.apache.spark.deploy.history.yarn

import com.codahale.metrics.{Metric, Timer}

import org.apache.spark.metrics.source.Source

/**
 * An extended metrics source with some operations to build up the registry, and
 * to time a closure.
 */
private[history] trait ExtendedMetricsSource extends Source {

  /**
   * A map to build up of all metrics to register and include in the string value
   * @return
   */
  def metricsMap: Map[String, Metric]

  protected def init(): Unit = {
    metricsMap.foreach(elt => metricRegistry.register(elt._1, elt._2))
  }

  override def toString: String = {
    def sb = new StringBuilder()
    metricsMap.foreach(elt => sb.append(s" ${elt._1} = ${elt._2}\n"))
    sb.toString()
  }

  /**
   * Time a closure, returning its output.
   * @param t timer
   * @param f function
   * @tparam T type of return value of the function
   * @return the result of the function.
   */
  def time[T](t: Timer)(f: => T): T = {
    val timeCtx = t.time()
    try {
      f
    } finally {
      timeCtx.close()
    }
  }
}
