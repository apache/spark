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

package org.apache.spark.sql.streaming;

import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.execution.streaming.AvailableNowTrigger$;
import org.apache.spark.sql.execution.streaming.ContinuousTrigger;
import org.apache.spark.sql.execution.streaming.OneTimeTrigger$;
import org.apache.spark.sql.execution.streaming.ProcessingTimeTrigger;

/**
 * Policy used to indicate how often results should be produced by a [[StreamingQuery]].
 *
 * @since 2.0.0
 */
@Evolving
public class Trigger {

  /**
   * A trigger policy that runs a query periodically based on an interval in processing time.
   * If `interval` is 0, the query will run as fast as possible.
   *
   * @since 2.2.0
   */
  public static Trigger ProcessingTime(long intervalMs) {
      return ProcessingTimeTrigger.create(intervalMs, TimeUnit.MILLISECONDS);
  }

  /**
   * (Java-friendly)
   * A trigger policy that runs a query periodically based on an interval in processing time.
   * If `interval` is 0, the query will run as fast as possible.
   *
   * {{{
   *    import java.util.concurrent.TimeUnit
   *    df.writeStream().trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
   * }}}
   *
   * @since 2.2.0
   */
  public static Trigger ProcessingTime(long interval, TimeUnit timeUnit) {
      return ProcessingTimeTrigger.create(interval, timeUnit);
  }

  /**
   * (Scala-friendly)
   * A trigger policy that runs a query periodically based on an interval in processing time.
   * If `duration` is 0, the query will run as fast as possible.
   *
   * {{{
   *    import scala.concurrent.duration._
   *    df.writeStream.trigger(Trigger.ProcessingTime(10.seconds))
   * }}}
   * @since 2.2.0
   */
  public static Trigger ProcessingTime(Duration interval) {
      return ProcessingTimeTrigger.apply(interval);
  }

  /**
   * A trigger policy that runs a query periodically based on an interval in processing time.
   * If `interval` is effectively 0, the query will run as fast as possible.
   *
   * {{{
   *    df.writeStream.trigger(Trigger.ProcessingTime("10 seconds"))
   * }}}
   * @since 2.2.0
   */
  public static Trigger ProcessingTime(String interval) {
      return ProcessingTimeTrigger.apply(interval);
  }

  /**
   * A trigger that processes all available data in a single batch then terminates the query.
   *
   * @since 2.2.0
   * @deprecated This is deprecated as of Spark 3.4.0. Use {@link #AvailableNow()} to leverage
   *             better guarantee of processing, fine-grained scale of batches, and better gradual
   *             processing of watermark advancement including no-data batch.
   *             See the NOTES in {@link #AvailableNow()} for details.
   */
  @Deprecated
  public static Trigger Once() {
    return OneTimeTrigger$.MODULE$;
  }

  /**
   * A trigger that processes all available data at the start of the query in one or multiple
   * batches, then terminates the query.
   *
   * Users are encouraged to set the source options to control the size of the batch as similar as
   * controlling the size of the batch in {@link #ProcessingTime(long)} trigger.
   *
   * NOTES:
   * - This trigger provides a strong guarantee of processing: regardless of how many batches were
   *   left over in previous run, it ensures all available data at the time of execution gets
   *   processed before termination. All uncommitted batches will be processed first.
   * - Watermark gets advanced per each batch, and no-data batch gets executed before termination
   *   if the last batch advances the watermark. This helps to maintain smaller and predictable
   *   state size and smaller latency on the output of stateful operators.
   *
   * @since 3.3.0
   */
  public static Trigger AvailableNow() {
    return AvailableNowTrigger$.MODULE$;
  }

  /**
   * A trigger that continuously processes streaming data, asynchronously checkpointing at
   * the specified interval.
   *
   * @since 2.3.0
   */
  public static Trigger Continuous(long intervalMs) {
    return ContinuousTrigger.apply(intervalMs);
  }

  /**
   * A trigger that continuously processes streaming data, asynchronously checkpointing at
   * the specified interval.
   *
   * {{{
   *    import java.util.concurrent.TimeUnit
   *    df.writeStream.trigger(Trigger.Continuous(10, TimeUnit.SECONDS))
   * }}}
   *
   * @since 2.3.0
   */
  public static Trigger Continuous(long interval, TimeUnit timeUnit) {
    return ContinuousTrigger.create(interval, timeUnit);
  }

  /**
   * (Scala-friendly)
   * A trigger that continuously processes streaming data, asynchronously checkpointing at
   * the specified interval.
   *
   * {{{
   *    import scala.concurrent.duration._
   *    df.writeStream.trigger(Trigger.Continuous(10.seconds))
   * }}}
   * @since 2.3.0
   */
  public static Trigger Continuous(Duration interval) {
    return ContinuousTrigger.apply(interval);
  }

  /**
   * A trigger that continuously processes streaming data, asynchronously checkpointing at
   * the specified interval.
   *
   * {{{
   *    df.writeStream.trigger(Trigger.Continuous("10 seconds"))
   * }}}
   * @since 2.3.0
   */
  public static Trigger Continuous(String interval) {
    return ContinuousTrigger.apply(interval);
  }
}
