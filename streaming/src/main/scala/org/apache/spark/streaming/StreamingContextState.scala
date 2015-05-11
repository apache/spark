package org.apache.spark.streaming

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 *
 * Represents the state of the StreamingContext.
 */
@DeveloperApi
class StreamingContextState private (enumValue: Int) {

  override def hashCode: Int = enumValue

  override def equals(other: Any): Boolean = {
    other match {
      case otherState: StreamingContextState =>
        otherState.hashCode == this.hashCode
      case _ =>
        false
    }
  }
}

/**
 * :: DeveloperApi ::
 *
 * Object enumerating all the states that a StreamingContext can be.
 */
@DeveloperApi
object StreamingContextState {
  val INITIALIZED = new StreamingContextState(0)
  val STARTED = new StreamingContextState(1)
  val STOPPED = new StreamingContextState(2)
}