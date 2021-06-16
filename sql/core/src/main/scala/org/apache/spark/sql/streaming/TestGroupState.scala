package org.apache.spark.sql.streaming

import org.apache.spark.api.java.Optional
import org.apache.spark.sql.execution.streaming.GroupStateImpl
import org.apache.spark.sql.execution.streaming.GroupStateImpl._

/**
 * The extended version of [[GroupState]] interface with extra getters of state machine fields
 * to improve testability of the [[GroupState]] implementations
 * which inherit from the extended interface.
 *
 * @tparam S User-defined type of the state to be stored for each group. Must be encodable into
 *           Spark SQL types (see `Encoder` for more details).
 */
trait TestGroupState[S] extends GroupState[S] {
  /** Whether the state has been marked for removing */
  def hasRemoved: Boolean

  /** Whether the state has been updated but not removed */
  def hasUpdated: Boolean

  /**
   * Returns the timestamp if setTimeoutTimestamp is called.
   * Or, batch processing time + the duration will be returned when
   * setTimeoutDuration is called.
   *
   * Otherwise, returns Optional.empty if not set.
   */
  def getTimeoutTimestampMs: Optional[Long]
}

object TestGroupState {

  /**
   * Creates TestGroupState instances for general testing purposes.
   *
   * @param optionalState         Optional value of the state.
   * @param timeoutConf           Type of timeout configured. Based on this, different operations will
   *                              be supported.
   * @param batchProcessingTimeMs Processing time of current batch, used to calculate timestamp
   *                              for processing time timeouts.
   * @param eventTimeWatermarkMs  Optional value of event time watermark in ms. Set as None if
   *                              watermark is not present. Otherwise, event time watermark
   *                              should be a positive long and the timestampMs
   *                              set through setTimeoutTimestamp.
   *                              cannot be less than the set eventTimeWatermarkMs.
   * @param hasTimedOut           Whether the key for which this state wrapped is being created is
   *                              getting timed out or not.
   */
  @throws[IllegalArgumentException]("if 'batchProcessingTimeMs' is not positive")
  @throws[IllegalArgumentException]("if 'eventTimeWatermarkMs' present but is not 0 or positive")
  @throws[UnsupportedOperationException](
    "if 'hasTimedOut' is true however there's no timeout configured")
  def create[S](
      optionalState: Optional[S],
      timeoutConf: GroupStateTimeout,
      batchProcessingTimeMs: Long,
      eventTimeWatermarkMs: Optional[Long],
      hasTimedOut: Boolean): TestGroupState[S] = {
    GroupStateImpl.createForStreaming[S](
      Option(optionalState.orNull),
      batchProcessingTimeMs,
      eventTimeWatermarkMs.orElse(NO_TIMESTAMP),
      timeoutConf,
      hasTimedOut,
      eventTimeWatermarkMs.isPresent())
  }
}
