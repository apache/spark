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
   * For better scalability, AvailableNow can be used alternatively to process the data in
   * multiple batches.
   *
   * @since 2.2.0
   */
  public static Trigger Once() {
    return OneTimeTrigger$.MODULE$;
  }

  /**
   * A trigger that processes all available data at the start of the query in one or multiple
   * batches, then terminates the query.
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
