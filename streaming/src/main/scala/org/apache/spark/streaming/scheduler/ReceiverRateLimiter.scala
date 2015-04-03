package org.apache.spark.streaming.scheduler

import java.util.concurrent.TimeUnit._

import akka.actor.ActorRef
import scala.annotation.tailrec

import org.apache.spark.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.UpdatedDynamicRate

/** Provides waitToPush() method to limit the rate at which receivers consume data.
  *
  * waitToPush method will block the thread if too many messages have been pushed too quickly,
  * and only return when a new message has been pushed. It assumes that only one message is
  * pushed at a time.
  *
  * The spark configuration spark.streaming.receiver.maxRate gives the maximum number of messages
  * per second that each receiver will accept.
  *
  */
private[streaming]
abstract class ReceiverRateLimiter extends Serializable {
  self: RateLimiter =>

  private val SYNC_INTERVAL = NANOSECONDS.convert(10, SECONDS)
  private var lastSyncTime = System.nanoTime
  private var messagesWrittenSinceSync = 0L

  if (isDriver) {
    val dynamicRateUpdater = new StreamingListener {
      override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
        val processedRecords = batchCompleted.batchInfo.receivedBlockInfo.get(streamId)
          .map(r => r.map(_.numRecords).sum)
          .getOrElse(0L)
        val processedTimeInMs = batchCompleted.batchInfo.processingDelay.getOrElse(
          throw new IllegalStateException("Illegal status: cannot get the processed time"))

        computeEffectiveRate(processedRecords, processedTimeInMs)
        updateDynamicRate(
          streamingContext.scheduler.receiverTracker.receiverInfo(streamId).actor)
      }
    }

    streamingContext.addStreamingListener(dynamicRateUpdater)
  }

  def streamId: Int

  def streamingContext: StreamingContext

  def updateDynamicRate(receiver: ActorRef): Unit

  def remoteUpdateDynamicRate(updatedRate: Double): Unit

   @tailrec
  final def waitToPush(): Unit = {
    val timeToWait = computeWaitTime(effectiveRate)
    if (timeToWait <= 0) {
      return
    } else {
      Thread.sleep(timeToWait)
      waitToPush()
    }
  }

  private def computeWaitTime(effectiveRate: Double): Long = {
    val now = System.nanoTime
    val elapsedNanosecs = math.max(now - lastSyncTime, 1)
    val receiveRate = messagesWrittenSinceSync.toDouble * 1000000000 / elapsedNanosecs

    if (receiveRate <= effectiveRate) {
      // It's okay to write; just update some variables and return
      messagesWrittenSinceSync += 1
      if (now > lastSyncTime + SYNC_INTERVAL) {
        // Sync interval has passed; let's resync
        lastSyncTime = now
        messagesWrittenSinceSync = 1
      }
      0L
    } else {
      // Calculate how much time we should sleep to bring ourselves to the desired rate.
      val targetTimeInMillis = (messagesWrittenSinceSync * 1000 / effectiveRate).toInt
      val elapsedTimeInMillis = elapsedNanosecs / 1000000
      val sleepTimeInMillis = targetTimeInMillis - elapsedTimeInMillis
      if (sleepTimeInMillis > 0L) sleepTimeInMillis else 0L
    }
  }
}

private[streaming]
class FixedReceiverRateLimiter(
    val isDriver: Boolean,
    val streamId: Int,
    val defaultRate: Double,
    @transient val streamingContext: StreamingContext = null)
  extends ReceiverRateLimiter with Logging with FixedRateLimiter {

  if (isDriver) assert(streamingContext != null)

  def updateDynamicRate(receiver: ActorRef): Unit = {
    assert(isDriver)
  }

  def remoteUpdateDynamicRate(updatedRate: Double): Unit = {
    assert(!isDriver)
  }
}

class DynamicReceiverRateLimiter(
    val isDriver: Boolean,
    val streamId: Int,
    val defaultRate: Double,
    val slowStartInitialRate: Double,
    @transient val streamingContext: StreamingContext = null)
  extends ReceiverRateLimiter with Logging with DynamicRateLimiter {

  if (isDriver) assert(streamingContext != null)

  def updateDynamicRate(receiver: ActorRef): Unit = {
    assert(isDriver)
    receiver ! UpdatedDynamicRate(effectiveRate)
  }

  def remoteUpdateDynamicRate(updatedRate: Double): Unit = {
    assert(!isDriver)
    dynamicRate = updatedRate
  }
}
