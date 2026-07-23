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

package org.apache.spark.resource

/**
 * The fixed, normalized [[scala.math.BigDecimal]] representation used for all CPU accounting
 * (cores / cpus per task / free cores / task slots). `spark.task.cpus` can be fractional (e.g.
 * `0.2`), and a naive `Double` accounting would drift (`1.0 / 0.1 == 9.999...` floors to 9 instead
 * of 10); carrying the values as an exact `BigDecimal` avoids that.
 *
 * Every CPU value is normalized to a fixed scale of [[SCALE]] (9 fractional digits, i.e. `1e-9`
 * resolution -- far finer than any realistic `spark.task.cpus`). With the integral part bounded by
 * the 32-bit core range, the value is a "precision 19, scale 9" decimal whose unscaled magnitude
 * stays below `Long.MAX_VALUE`. That keeps `BigDecimal` on its compact, `long`-backed fast path
 * (no `BigInteger` allocation) on the scheduler hot path, and -- because `BigDecimal` add/subtract
 * align to the larger scale -- arithmetic over normalized values stays at scale 9 without any
 * re-normalization, giving one uniform representation everywhere.
 */
private[spark] object CpuAmount {

  /** Number of fractional decimal digits carried by every normalized CPU accounting value. */
  val SCALE: Int = 9

  /** Smallest positive amount representable at [[SCALE]], i.e. 1e-9. */
  val MIN_AMOUNT: BigDecimal = BigDecimal(java.math.BigDecimal.valueOf(1, SCALE))

  /**
   * Largest supported amount. Executor capacity is an `Int` number of cores everywhere (config,
   * registration RPC, scheduler bookkeeping), so a per-task amount beyond `Int.MaxValue` could
   * never be satisfied.
   */
  val MAX_AMOUNT: BigDecimal = BigDecimal(Int.MaxValue)

  /**
   * Normalize a value to the fixed [[SCALE]] so it stays in the compact, `long`-backed form.
   * Constructing values from their exact decimal string (never from a `Double`) and normalizing
   * here is what guarantees the uniform representation.
   */
  def normalize(v: BigDecimal): BigDecimal = v.setScale(SCALE, BigDecimal.RoundingMode.HALF_UP)

  /**
   * Whether an amount lies in the valid cpus range [[MIN_AMOUNT]] to [[MAX_AMOUNT]]. The
   * comparison is cheap at any scale, so callers handling untrusted input can (and should)
   * check the compact parsed value with this before [[normalize]] -- setScale on an extreme
   * exponent (e.g. 1e10000000) would materialize an enormous unscaled value.
   */
  def isInRange(v: BigDecimal): Boolean = v >= MIN_AMOUNT && v <= MAX_AMOUNT

  /**
   * The minimal-scale form (trailing zeros removed). Use this for logging/printing (e.g.
   * `0.200000000` renders as `0.2`) and for products that would otherwise blow the normalized
   * unscaled value past `Long` range -- most notably `durationNanos * cpus` in the executor CPU
   * time metric, where a scale-9 `cpus` would inflate the intermediate into a `BigInteger`.
   */
  def stripTrailingZeros(v: BigDecimal): BigDecimal = BigDecimal(v.bigDecimal.stripTrailingZeros())

  /** Render a normalized value for logs/messages/UIs with trailing zeros trimmed. */
  def toDisplayString(v: BigDecimal): String = v.bigDecimal.stripTrailingZeros().toPlainString
}
