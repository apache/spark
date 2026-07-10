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

package org.apache.spark.shuffle.streaming

import java.util.concurrent.{CancellationException, CompletableFuture, CountDownLatch, LinkedBlockingDeque, Semaphore, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import javax.annotation.concurrent.NotThreadSafe

import scala.util.Try

import io.netty.buffer.{ByteBuf, ByteBufOutputStream, CompositeByteBuf, Unpooled}
import io.netty.channel.{ChannelFuture, ChannelOption}

import org.apache.spark.{SparkContext, SparkEnv, StreamingShuffleTaskLocation, TaskContext}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config.{EXECUTOR_ID, STREAMING_SHUFFLE_CHECKSUM_ENABLED, STREAMING_SHUFFLE_NETWORK_BUFFER_MAX_WAIT_TIME_MS, STREAMING_SHUFFLE_NETWORK_BUFFER_SIZE, STREAMING_SHUFFLE_WRITER_MAX_MEMORY}
import org.apache.spark.internal.config.Network.RPC_IO_THREADS
import org.apache.spark.memory.{MemoryConsumer, MemoryMode}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client.TransportClient
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server.TransportServer
import org.apache.spark.network.shuffle.streaming.{DataMessage, ShuffleChecksum, StreamingShuffleMessage, StreamingShuffleMessageType, TerminationControlMessage}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.{JavaSerializerInstance, SerializationStream}
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleWriter}
import org.apache.spark.util.{ErrorNotifier, Utils}

class StreamingShuffleWriter[K, V](
    handle: ShuffleHandle,
    mapId: Long,
    val context: TaskContext,
    serverHandler: Option[StreamingShuffleServerHandler] = None,
    private[streaming] val errorNotifier: ErrorNotifier = new ErrorNotifier())
    extends ShuffleWriter[K, V] with TaskContextAwareLogging {
  assert(SparkEnv.get.streamingShuffleOutputTracker.isDefined)
  // Spark params.
  private val conf = SparkEnv.get.conf
  private val SEND_BUFFER_SIZE: Integer = 32 << 10 // 32 KB
  private val RECV_BUFFER_SIZE: Integer = 512
  // The target network buffer size
  private val BUFFER_SIZE: Integer = conf.get(STREAMING_SHUFFLE_NETWORK_BUFFER_SIZE)
  // The interval at which we flush pending messages.
  private val MAX_BUFFERING_TIME_MS = conf.get(STREAMING_SHUFFLE_NETWORK_BUFFER_MAX_WAIT_TIME_MS)

  // Shuffle details.
  private val streamingShuffleHandle = handle.asInstanceOf[StreamingShuffleHandle[K, V, _]]
  private val serializerInstance = streamingShuffleHandle.dependency.serializer.newInstance()
  private val partitioner = streamingShuffleHandle.dependency.partitioner
  private val numPartitions = partitioner.numPartitions
  private val shuffleWriterId = context.partitionId()
  // Total size of TCP buffers. Use Long math to avoid 32-bit overflow when numPartitions
  // is large (numPartitions * buffer sizes can exceed Int.MaxValue).
  private val TOTAL_TCPBUF_BYTES: Long =
    numPartitions.toLong * (SEND_BUFFER_SIZE + RECV_BUFFER_SIZE)
  // Total allowed memory for buffered rows, excluding TCP buffers.
  private val MAX_BUFFER_BYTES: Long = math.max(numPartitions.toLong * BUFFER_SIZE * 2,
    conf.get(STREAMING_SHUFFLE_WRITER_MAX_MEMORY).toLong - TOTAL_TCPBUF_BYTES)
  require(MAX_BUFFER_BYTES >= BUFFER_SIZE && MAX_BUFFER_BYTES <= Int.MaxValue,
    s"Streaming shuffle writer memory budget ($MAX_BUFFER_BYTES bytes) is invalid for " +
      s"$numPartitions partitions; increase ${STREAMING_SHUFFLE_WRITER_MAX_MEMORY.key} or " +
      "reduce the number of partitions.")
  // The per-partition floor (2 buffers per partition) can push the total in-force budget above
  // the configured writerMaxMemory when the partition count is high; surface the effective total
  // (including TCP buffers) so operators can see the limit they set is not the one in force.
  private val effectiveBudget = MAX_BUFFER_BYTES + TOTAL_TCPBUF_BYTES
  if (effectiveBudget > conf.get(STREAMING_SHUFFLE_WRITER_MAX_MEMORY).toLong) {
    logWarning(log"Streaming shuffle writer effective memory budget " +
      log"${MDC(LogKeys.MAX_MEMORY_SIZE, Utils.bytesToString(effectiveBudget))} exceeds the " +
      log"configured ${MDC(LogKeys.CONFIG, STREAMING_SHUFFLE_WRITER_MAX_MEMORY.key)}=" +
      log"${MDC(LogKeys.MEMORY_SIZE, Utils.bytesToString(
        conf.get(STREAMING_SHUFFLE_WRITER_MAX_MEMORY).toLong))} because the per-partition " +
      log"minimum for ${MDC(LogKeys.NUM_PARTITIONS, numPartitions)} partitions takes precedence.")
  }
  private val CHECKSUM_ENABLED = conf.get(STREAMING_SHUFFLE_CHECKSUM_ENABLED)

  // Helper objects.

  // Exposed for testing
  private[streaming] val transportServerHandler: StreamingShuffleServerHandler =
    serverHandler.getOrElse(
      new StreamingShuffleServerHandler(
        onTerminationAckReceived,
        streamingShuffleHandle.shuffleId,
        numPartitions,
        context,
        errorNotifier))

  private[streaming] val server: TransportServer = startShuffleServer()

  private val memoryConsumer = new MemoryConsumer(
    context.taskMemoryManager(), BUFFER_SIZE.longValue(), MemoryMode.OFF_HEAP) {
    // Spilling not supported for simplicity.
    override def spill(size: Long, trigger: MemoryConsumer): Long = 0
  }

  // Runtime state.

  // Will reach zero when we've received termination acks from all readers. Public for testing.
  private[streaming] val allAcksReceived = new CountDownLatch(numPartitions)

  // Holds per-shard state. Public for testing.
  private[streaming] val shards: Array[ShardState] = Array.tabulate(numPartitions)(ShardState(_))

  private val allocatedBufferBytesSemaphore: Semaphore = new Semaphore(MAX_BUFFER_BYTES.toInt)

  // Data payloads use a dedicated direct-buffer free-list (bufferPool) of fixed BUFFER_SIZE
  // buffers so full-size send buffers can be recycled across the task; the small, variable-size
  // message envelopes instead use the server's pooled allocator (see ShardState.send). The two
  // allocation paths are intentionally separate.
  private[streaming] val bufferPool = new LinkedBlockingDeque[ByteBuf]()

  setShuffleIdForLogging(streamingShuffleHandle.shuffleId)

  // Ensure resources are cleaned up on any task completion.
  context.addTaskCompletionListener[Unit] { _ =>
    cleanupResources()
  }

  private def startShuffleServer(): TransportServer = {
    val role = conf.get(EXECUTOR_ID).map { id =>
      if (SparkContext.isDriver(id)) "driver" else "executor"
    }

    val serverConf = SparkTransportConf.fromSparkConf(
      conf,
      s"streaming-shuffle-writer-${streamingShuffleHandle.shuffleId}-${shuffleWriterId}",
      conf.get(RPC_IO_THREADS).getOrElse(0), // zero will use default number of cores
      role)
    val serverContext = new TransportContext(serverConf, transportServerHandler)
    logInfo(log"Creating shuffle server for shuffle writer" +
      log" ${MDC(LogKeys.SHUFFLE_WRITER_ID, shuffleWriterId)}" +
      log" for shuffle ${MDC(LogKeys.SHUFFLE_ID, streamingShuffleHandle.shuffleId)}")
    val server = serverContext.createServer()
    val hostname = if (SparkEnv.get.rpcEnv.address != null) {
      // used and not null when running in an actual cluster but may be null for running tests
      SparkEnv.get.rpcEnv.address.host
    } else {
      Utils.localCanonicalHostName()
    }
    val tracker = SparkEnv.get.streamingShuffleOutputTracker.get
    val taskLocation =
      StreamingShuffleTaskLocation(SparkEnv.get.executorId, hostname, server.getPort)
    // The Boolean return is intentionally not acted on: a false means the shuffle was
    // (concurrently) unregistered, which the tracker already logs a warning for. That only
    // happens while the shuffle is being torn down, in which case this writer task is going
    // away too, so there is nothing useful to do here.
    tracker.registerShuffleWriterTask(streamingShuffleHandle.shuffleId, mapId, taskLocation)
    logInfo(log"Created shuffle server for writer ${MDC(LogKeys.SHUFFLE_WRITER_ID,
      shuffleWriterId)} at ${MDC(LogKeys.TASK_LOCATION, taskLocation)}" +
      log" for shuffle ${MDC(LogKeys.SHUFFLE_ID, streamingShuffleHandle.shuffleId)}")
    server
  }

  /** A buffer with metadata. Not thread safe: only supports single-threaded access. */
  @NotThreadSafe
  private[streaming] case class TimestampedBuffer(buffer: ByteBuf) {
    val serializationStream: SerializationStream =
      serializerInstance.serializeStream(new ByteBufOutputStream(buffer))
    private val creationTimeNs = System.nanoTime()
    private val shuffleChecksum = if (CHECKSUM_ENABLED) new ShuffleChecksum() else null

    // Start from beginning to include serialization stream headers
    private var lastBufferPosition = 0

    def totalByteSize(): Long = buffer.readableBytes()
    def ageMs(): Long = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - creationTimeNs)

    /* Checksum calculation for order-dependent per-row checksums. */
    def updateChecksum(): Unit = {
      if (shuffleChecksum != null) {
        val currentPosition = buffer.writerIndex()
        val newDataLength = currentPosition - lastBufferPosition
        shuffleChecksum.updateChecksum(buffer, lastBufferPosition, newDataLength)
        lastBufferPosition = currentPosition
      }
    }

    def getChecksumValue(): Long = if (shuffleChecksum != null) shuffleChecksum.getValue else 0L
  }

  // The state for each shuffle destination.
  private[streaming] case class ShardState(id: Int) {
    // client may be accessed from other threads via cancel(); @volatile to be safe.
    @volatile private var client: Either[TransportClient, CompletableFuture[TransportClient]] =
      Right(transportServerHandler.futureClients(id).thenApply(c => {
        c.getChannel.config.setOption(ChannelOption.SO_SNDBUF, SEND_BUFFER_SIZE)
        c.getChannel.config.setOption(ChannelOption.SO_RCVBUF, RECV_BUFFER_SIZE)
        c
      }))
    val buffer: AtomicReference[TimestampedBuffer] = new AtomicReference(null)
    val lastSentSequenceNum: AtomicLong = new AtomicLong(-1)
    val terminationAckReceived: AtomicBoolean = new AtomicBoolean(false)

    // send will never block; push back is instead handled by blocking buffer allocation in write
    // on `allocatedBufferBytesSemaphore`. All send methods are synchronized to preserve message
    // order.
    def send(message: StreamingShuffleMessage, done: () => Unit = () => ()): Unit = synchronized {
      message.setSeqNum(lastSentSequenceNum.incrementAndGet())
      var buf: CompositeByteBuf = null
      try {
        buf = server.getPooledByteBufAllocator.compositeBuffer().capacity(message.headerLength())
        message.encode(buf)
      } catch {
        case e: Throwable =>
          if (buf != null) buf.release()
          throw e
      } finally {
        message.release()
      }

      def sendToClient(client: TransportClient): Unit = {
        try {
          client.send(buf).addListener((future: ChannelFuture) => {
            if (!future.isSuccess) {
              errorNotifier.markError(future.cause())
            }
            done()
          })
        } catch {
          case e: Throwable =>
            buf.release()
            done()
            errorNotifier.markError(e)
            throw e
        }
      }

      client match {
        case Left(c) =>
          sendToClient(c)
        case Right(future) =>
          // Add another completion stage to ensure queued messages are sent in order.
          // If the future is already completed, this will be executed immediately.
          val newFuture = future.whenComplete { (client, ex) =>
            ex match {
              case null => sendToClient(client)
              case _ => buf.release(); done()
            }
          }
          // Once the future is completed, stop accumulating CompletionStages.
          client = if (newFuture.isDone) Left(newFuture.join()) else Right(newFuture)
      }
    }

    // Sends buffer as a DataMessage to the shuffle reader. Takes ownership of the buffer.
    def send(timestampedBuffer: TimestampedBuffer): Unit = synchronized {
      timestampedBuffer.serializationStream.close()
      val rawBuffer = timestampedBuffer.buffer
      val dataSize = rawBuffer.writerIndex()
      timestampedBuffer.updateChecksum()
      val checksumValue = timestampedBuffer.getChecksumValue()
      val dataMessage = new DataMessage(shuffleWriterId, id, dataSize, rawBuffer, checksumValue)

      // We keep a reference to rawBuffer so we can return it to the pool.
      send(dataMessage, () => {
        if (rawBuffer.refCnt() != 1) {
          // Throw an internal exception for unexpected state.
          errorNotifier.markError(new AssertionError(
            s"INTERNAL ERROR: Unexpected refcnt ${rawBuffer.refCnt()}"))
          rawBuffer.release()
        } else {
          rawBuffer.clear()
          if (context.isFailed() || context.isCompleted() || rawBuffer.capacity() != BUFFER_SIZE) {
            rawBuffer.release()
          } else {
            bufferPool.offerLast(rawBuffer)
          }
          allocatedBufferBytesSemaphore.release(BUFFER_SIZE)
        }
      })
    }

    // Consume the current buffer, if it exists, and send it as a DataMessage.
    def send(): Unit = synchronized {
      val b = takeBuffer()
      if (b != null) send(b)
    }

    def takeBuffer(): TimestampedBuffer = buffer.getAndSet(null)

    def putBuffer(b: TimestampedBuffer): Unit = assert(buffer.getAndSet(b) == null)

    def close(): Unit = {
      send()
      send(new TerminationControlMessage(shuffleWriterId, id))
    }

    def cancel(): Unit = {
      val error = context.getTaskFailure.getOrElse(new CancellationException())
      transportServerHandler.futureClients(id).completeExceptionally(error)
      client.foreach(_.completeExceptionally(error))
      Option(takeBuffer()).foreach(_.buffer.release())
    }

    // For testing only.
    def hasClient: Boolean = client match {
      case Left(_) => true
      case Right(future) => future.isDone
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    // No-op: the streaming shuffle lifecycle is handled elsewhere. write() blocks until all
    // readers ack termination on the normal path, and the task-completion listener
    // (cleanupResources) closes the server and releases buffers on both success and failure.
    //
    // Streaming shuffle readers locate writers through StreamingShuffleOutputTracker and pull
    // data directly over Netty; they never consult MapStatus block sizes, and streaming shuffle
    // has no standard block-fetch fallback path. This MapStatus is therefore only a placeholder
    // to satisfy the ShuffleWriter contract and the DAGScheduler / MapOutputTracker bookkeeping;
    // its all-zero partition lengths are never read by any reducer.
    Some(MapStatus(
      SparkEnv.get.blockManager.shuffleServerId,
      Array.fill(numPartitions)(0L),
      mapId))
  }

  /** Get the lengths of each partition */
  override def getPartitionLengths(): Array[Long] = {
    Array.fill(numPartitions)(0L)
  }

  /**
   * Invoked on a Netty event-loop thread by [[StreamingShuffleServerHandler]] when a reader's
   * termination ack arrives. Validates the reader's last-seen sequence number against what this
   * writer sent; on a mismatch it throws STREAMING_SHUFFLE_INCORRECT_SEQUENCE_NUMBER, which the
   * handler captures via the shared [[ErrorNotifier]]. The per-partition latch decrement is
   * idempotent, so duplicate acks are ignored. Exposed for testing.
   */
  private[streaming] def onTerminationAckReceived(
      partitionId: Int, lastSeqNumSeenByReader: Long): Unit = {
    val lastSeqNumSent = shards(partitionId).lastSentSequenceNum.get()
    if (lastSeqNumSent != lastSeqNumSeenByReader) {
      throw StreamingShuffleManager.streamingShuffleIncorrectSequenceNumber(
        StreamingShuffleMessageType.TERMINATION_ACK_MESSAGE,
        shuffleWriterId,
        partitionId,
        lastSeqNumSent,
        lastSeqNumSeenByReader)
    }
    if (shards(partitionId).terminationAckReceived.compareAndSet(false, true)) {
      allAcksReceived.countDown()
    }
    val receivedAcks = numPartitions - allAcksReceived.getCount.toInt
    logInfo(log"Received termination ack from reader ${MDC(LogKeys.SHUFFLE_READER_ID,
      partitionId)}. Now have ${MDC(LogKeys.NUM_TERMINATION_ACKS, receivedAcks)} / ${MDC(
      LogKeys.NUM_SHUFFLE_READERS, numPartitions)} termination acks")
  }

  /**
   * Cleans up all writer resources.
   * This method should be idempotent.
   */
  private[streaming] def cleanupResources(): Unit = {
    val cleanupStartTime = System.currentTimeMillis()
    Utils.tryLogNonFatalError {
      shards.foreach(_.cancel())
    }
    Utils.tryLogNonFatalError {
      server.close()
    }
    Utils.tryLogNonFatalError {
      val list = new java.util.ArrayList[ByteBuf]()
      bufferPool.drainTo(list)
      list.forEach(buf => { buf.release(); () })
    }
    Utils.tryLogNonFatalError {
      memoryConsumer.freeMemory(memoryConsumer.getUsed())
    }
    logInfo(log"Resource cleanup took ${MDC(LogKeys.DURATION,
      System.currentTimeMillis() - cleanupStartTime)} ms")
  }

  private def throwErrorIfExists(): Unit = {
    context.getTaskFailure.foreach { throw _ }
    errorNotifier.throwErrorIfExists()
  }

  private def newBuffer(): TimestampedBuffer = {
    // Back-pressure is accounted per network buffer (BUFFER_SIZE permits each), not by exact
    // byte size, so this bounds in-flight memory only on a best-effort basis: a single
    // serialized row larger than BUFFER_SIZE (rows are not split across buffers, see write())
    // grows its buffer past BUFFER_SIZE and thus exceeds the tracked budget.
    if (!allocatedBufferBytesSemaphore.tryAcquire(BUFFER_SIZE, 10, TimeUnit.MICROSECONDS)) {
      shards.foreach(_.send())
      while (!allocatedBufferBytesSemaphore.tryAcquire(BUFFER_SIZE, 10, TimeUnit.MILLISECONDS)) {
        throwErrorIfExists()
      }
    }
    val buffer = bufferPool.pollLast()
    TimestampedBuffer(if (buffer != null) buffer else Unpooled.directBuffer(BUFFER_SIZE))
  }

  /**
   * Write a sequence of records to downstream shuffle readers.
   *
   * For each record, the reader partition is determined using the key and the
   * partitioner. Multiple rows can be packed into a single DataMessage; the maximum
   * number of rows that can be packed depends on the STREAMING_SHUFFLE_NETWORK_BUFFER_SIZE
   * config. Each DataMessage is sent to the reader for its partition over that reader's
   * Netty connection.
   */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val isWriteFinished = new CountDownLatch(1)
    val flushThread = new Thread(() =>
      Try {
        while (!isWriteFinished.await(MAX_BUFFERING_TIME_MS, TimeUnit.MILLISECONDS))
          shards.foreach(_.send())
      }.recover { case e => errorNotifier.markError(e) }
      , "time-based-flush-for-shuffle-writer-" +
        s"${streamingShuffleHandle.shuffleId}-${shuffleWriterId}")
    try {
      // Reserve the budget with the task memory manager for accounting/visibility. In-flight
      // buffer memory is bounded by allocatedBufferBytesSemaphore; we ignore the return value
      // because we cannot act on a partial grant here (this consumer cannot spill).
      memoryConsumer.acquireMemory(TOTAL_TCPBUF_BYTES + MAX_BUFFER_BYTES)
      flushThread.start()
      records.foreach { record =>
        val shard = shards(partitioner.getPartition(record._1))
        var timestampedBuffer = shard.takeBuffer()
        if (timestampedBuffer == null) {
          timestampedBuffer = newBuffer()
        }
        val partitionSerializationStream = timestampedBuffer.serializationStream
        // When UnsafeRowSerializer, the key, record._1, is only used for determining
        // the partition, and it doesn't need to be sent to the shuffle readers.
        // However, if JavaSerializer is used (for test mainly), we need to serialize
        // the key since we will be attempting to read it from the shuffle reader
        //
        // TODO we are actually not guaranteeing that a buffer used to send data for a
        // partition does not exceed BUFFER_SIZE. We currently are not implementing spanning rows
        // across multiple buffers as it requires interface changes in the serializers
        if (serializerInstance.isInstanceOf[JavaSerializerInstance]) {
          partitionSerializationStream.writeKey(record._1.asInstanceOf[Any])
        }
        partitionSerializationStream.writeValue(record._2.asInstanceOf[Any])
        partitionSerializationStream.flush()

        timestampedBuffer.updateChecksum()

        // Flush immediately if the buffer is almost full or stale.
        if (timestampedBuffer.totalByteSize() < BUFFER_SIZE * 9 / 10 &&
            timestampedBuffer.ageMs() < MAX_BUFFERING_TIME_MS) {
          shard.putBuffer(timestampedBuffer)
        } else {
          shard.send(timestampedBuffer)
          throwErrorIfExists()
        }
      }
      isWriteFinished.countDown()
      shards.foreach(_.close())
      logInfo(log"StreamingShuffleWriter finished writing data and termination messages for " +
        log"shuffle writer ${MDC(LogKeys.SHUFFLE_WRITER_ID, shuffleWriterId)}. Shutting down now.")
      // Wait for all termination acks. This has no wall-clock timeout by design, and that is
      // deliberate for correctness: a term-ack is the writer's only confirmation that a reader
      // received every message through the final sequence number (validated in
      // onTerminationAckReceived), so finishing write() without all acks would risk marking the
      // map task successful while a reader is silently missing data. The loop instead exits only
      // on all-acks, on an ErrorNotifier error surfaced by throwErrorIfExists(), or on task
      // cancellation. A reader that dies fails its own reduce task, which restarts the query and
      // tears down this writer too, so writer-side reader-liveness detection is unnecessary.
      while (!allAcksReceived.await(1, TimeUnit.MILLISECONDS)) {
        throwErrorIfExists()
      }
      logInfo(log"Received all termination acks for shuffle writer ${MDC(
        LogKeys.SHUFFLE_WRITER_ID, shuffleWriterId)}. Closing server channel.")
      throwErrorIfExists()
    } finally {
      isWriteFinished.countDown() // Duplicate countDowns are a no-op.
      flushThread.join()
    }
  }
}
