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

package org.apache.spark.sql.pipelines.util

import scala.concurrent.duration._
import scala.math.{log10, pow}

/**
 * A `BackoffStrategy` determines the backoff duration (how long we should wait) for
 * retries after failures.
 */
trait BackoffStrategy {

  /** Returns the amount of time to wait after `numFailures` failures. */
  def waitDuration(numFailures: Int): FiniteDuration
}

/**
 * A `BackoffStrategy` where the back-off time grows exponentially for each
 * successive retry.
 *
 * The back-off time after `n` failures is min(maxTime, (2 ** n) * stepSize).
 *
 * @param maxTime Maximum back-off time.
 * @param stepSize Minimum step size to increment back-off.
 */
case class ExponentialBackoffStrategy(maxTime: FiniteDuration, stepSize: FiniteDuration)
    extends BackoffStrategy {

  require(
    stepSize >= 0.seconds,
    s"Back-off step size must be non-negative. Given value: $stepSize"
  )
  require(
    maxTime >= 0.seconds,
    s"Back-off max time must be non-negative. Given value: $stepSize"
  )

  override def waitDuration(numFailures: Int): FiniteDuration = {
    require(
      numFailures >= 0,
      s"Number of failures must be non-negative. Given value: $numFailures."
    )

    if (stepSize <= 0.seconds) return 0.seconds
    if (stepSize >= maxTime) return maxTime

    def log2(x: Double) = log10(x) / log10(2.0)
    val willExceedMax = numFailures >= log2(maxTime.toNanos.toDouble / stepSize.toNanos) + 1
    if (!willExceedMax) pow(2, numFailures - 1).toLong * stepSize else maxTime
  }
}
