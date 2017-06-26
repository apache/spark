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

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.execution.streaming.OneTimeTrigger$;

/**
 * Policy used to indicate how often results should be produced by a [[StreamingQuery]].
 *
 * @since 2.0.0
 */
@InterfaceStability.Evolving
public class Trigger {

  /**
   * A trigger policy that runs a query periodically based on an interval in processing time.
   * If `interval` is 0, the query will run as fast as possible.
   *
   * @since 2.2.0
   */
  public static Trigger ProcessingTime(long intervalMs) {
      return ProcessingTime.create(intervalMs, TimeUnit.MILLISECONDS);
  }

  /**
   * (Java-friendly)
   * A trigger policy that runs a query periodically based on an interval in processing time.
   * If `interval` is 0, the query will run as fast as possible.
   *
   * {{{
   *    import java.util.concurrent.TimeUnit
   *    df.writeStream.trigger(ProcessingTime.create(10, TimeUnit.SECONDS))
   * }}}
   *
   * @since 2.2.0
   */
  public static Trigger ProcessingTime(long interval, TimeUnit timeUnit) {
      return ProcessingTime.create(interval, timeUnit);
  }

  /**
   * (Scala-friendly)
   * A trigger policy that runs a query periodically based on an interval in processing time.
   * If `duration` is 0, the query will run as fast as possible.
   *
   * {{{
   *    import scala.concurrent.duration._
   *    df.writeStream.trigger(ProcessingTime(10.seconds))
   * }}}
   * @since 2.2.0
   */
  public static Trigger ProcessingTime(Duration interval) {
      return ProcessingTime.apply(interval);
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
      return ProcessingTime.apply(interval);
  }

  /**
   * A trigger that process only one batch of data in a streaming query then terminates
   * the query.
   *
   * @since 2.2.0
   */
  public static Trigger Once() {
    return OneTimeTrigger$.MODULE$;
  }
}
