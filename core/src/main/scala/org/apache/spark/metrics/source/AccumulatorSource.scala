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

package org.apache.spark.metrics.source

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.util.{AccumulatorV2, DoubleAccumulator, LongAccumulator}

/**
 * AccumulatorSource is a Spark metric Source that reports the current value
 * of the accumulator as a gauge.
 *
 * It is restricted to the LongAccumulator and the DoubleAccumulator, as those
 * are the current built-in numerical accumulators with Spark, and excludes
 * the CollectionAccumulator, as that is a List of values (hard to report,
 * to a metrics system)
 */
private[spark] class AccumulatorSource extends Source {
  private val registry = new MetricRegistry
  protected def register[T](accumulators: Map[String, AccumulatorV2[_, T]]): Unit = {
    accumulators.foreach {
      case (name, accumulator) =>
        val gauge = new Gauge[T] {
          override def getValue: T = accumulator.value
        }
        registry.register(MetricRegistry.name(name), gauge)
    }
  }

  override def sourceName: String = "AccumulatorSource"
  override def metricRegistry: MetricRegistry = registry
}

@Experimental
class LongAccumulatorSource extends AccumulatorSource

@Experimental
class DoubleAccumulatorSource extends AccumulatorSource

/**
 * :: Experimental ::
 * Metrics source specifically for LongAccumulators. Accumulators
 * are only valid on the driver side, so these metrics are reported
 * only by the driver.
 * Register LongAccumulators using:
 *    LongAccumulatorSource.register(sc, {"name" -> longAccumulator})
 */
@Experimental
object LongAccumulatorSource {
  def register(sc: SparkContext, accumulators: Map[String, LongAccumulator]): Unit = {
    val source = new LongAccumulatorSource
    source.register(accumulators)
    sc.env.metricsSystem.registerSource(source)
  }
}

/**
 * :: Experimental ::
 * Metrics source specifically for DoubleAccumulators. Accumulators
 * are only valid on the driver side, so these metrics are reported
 * only by the driver.
 * Register DoubleAccumulators using:
 *    DoubleAccumulatorSource.register(sc, {"name" -> doubleAccumulator})
 */
@Experimental
object DoubleAccumulatorSource {
  def register(sc: SparkContext, accumulators: Map[String, DoubleAccumulator]): Unit = {
    val source = new DoubleAccumulatorSource
    source.register(accumulators)
    sc.env.metricsSystem.registerSource(source)
  }
}
