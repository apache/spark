package org.apache.spark.sql.streaming;

import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.execution.streaming.OneTimeTrigger$;

/**
 * :: Experimental ::
 * Policy used to indicate how often results should be produced by a [[StreamingQuery]].
 *
 * @since 2.0.0
 */
@Experimental
@InterfaceStability.Evolving
public abstract class Trigger {

  /**
   * :: Experimental ::
   * A trigger policy that runs a query periodically based on an interval in processing time.
   * If `interval` is 0, the query will run as fast as possible.
   *
   * @since 2.2.0
   */
  public static Trigger ProcessingTime(long intervalMs) {
      return ProcessingTime.apply(intervalMs);
  }

  /**
   * :: Experimental ::
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
   * :: Experimental ::
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
   * :: Experimental ::
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
