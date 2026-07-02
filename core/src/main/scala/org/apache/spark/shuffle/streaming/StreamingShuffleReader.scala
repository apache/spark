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

import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, ConcurrentLinkedQueue, LinkedBlockingQueue, Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import io.netty.buffer.ByteBufInputStream

import org.apache.spark.{ShuffleLocationResponse, SparkContext, SparkEnv, SparkRuntimeException, TaskContext}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config.{EXECUTOR_ID, STREAMING_SHUFFLE_CHECKSUM_ENABLED, STREAMING_SHUFFLE_READER_MAX_MEMORY}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client.{TransportClient, TransportClientFactory}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.streaming.{DataMessage, ShuffleChecksum, StreamingShuffleMessage, TerminationControlMessage}
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleReader}
import org.apache.spark.util.{ErrorNotifier, NextIterator, ThreadUtils, Utils}

/**
 * Default factory used by `StreamingShuffleReader` to create its output iterator.
 */
class StreamingShuffleReaderIteratorFactory {
  def create[K, C](
      messageQueue: LinkedBlockingQueue[StreamingShuffleMessage],
      handleTerminationMessage: TerminationControlMessage => Boolean,
      handleDataMessage: DataMessage => Iterator[(K, C)],
      checkTaskFailure: () => Unit
    ): Iterator[Product2[K, C]] = {
    new NextIterator[Product2[K, C]] {
      // Iterator that iterates through multiple rows in data message buffer. When the iterator
      // does not have any more rows, we should fetch another message from message queue.
      private var rowIterator: Iterator[(K, C)] = Iterator.empty

      def getNext(): Product2[K, C] = {
        while (!rowIterator.hasNext) {
          checkTaskFailure()
          messageQueue.poll(10, TimeUnit.MILLISECONDS) match {
            case msg: TerminationControlMessage =>
              if (handleTerminationMessage(msg)) {
                finished = true
                return null.asInstanceOf[Product2[K, C]]
              }
            case dataMessage: DataMessage =>
              rowIterator = handleDataMessage(dataMessage)
            case null => // no message received, continue to poll
            case other =>
              throw new IllegalArgumentException(
                s"Unexpected message type in reader queue: ${other.getClass.getName}")
          }
        }
        rowIterator.next()
      }

      override def close(): Unit = {
        // no-op. handleTerminationMessage will take care of final cleanup.
      }
    }
  }
}

class StreamingShuffleReader[K, C](
    handle: ShuffleHandle,
    val context: TaskContext,
    clientHandler: Option[StreamingShuffleClientHandler] = None,
    private[streaming] val errorNotifier: ErrorNotifier = new ErrorNotifier())
    extends ShuffleReader[K, C] with TaskContextAwareLogging {
  assert(SparkEnv.get.streamingShuffleOutputTracker.isDefined)
  private val conf = SparkEnv.get.conf

  private val streamingShuffleHandle = handle.asInstanceOf[StreamingShuffleHandle[K, _, C]]
  setShuffleIdForLogging(streamingShuffleHandle.shuffleId)
  // a mapping of mapId and client
  private[spark] val clientMap = new ConcurrentHashMap[Long, TransportClient]()
  // Track factories so we can close them on shutdown.
  // TODO: refactor to reuse a single client factory across writers.
  private val clientFactories = new ConcurrentLinkedQueue[TransportClientFactory]()
  private val tracker = SparkEnv.get.streamingShuffleOutputTracker.get

  private val role = conf.get(EXECUTOR_ID).map { id =>
    if (SparkContext.isDriver(id)) "driver" else "executor"
  }

  private val clientConf = SparkTransportConf.fromSparkConf(
    conf,
    s"streaming-shuffle-reader-${streamingShuffleHandle.shuffleId}-${context.partitionId()}",
    1, // a client should only need to use 1 thread
    role)

  private[spark] val taskDiscoveryExecutor =
    ThreadUtils.newDaemonSingleThreadExecutor(
      s"streaming-shuffle-task-discovery-thread-" +
        s"${streamingShuffleHandle.shuffleId}-${context.partitionId()}")

  private val totalNumShuffleWriters: AtomicInteger = new AtomicInteger(-1)
  private var perWriterByteLimit: Long = 1

  // We might need to revisit if the size limit is enough. If not, there should be a way to tie it
  // the per task memory limit from task context
  private val MAX_MEMORY = conf.get(STREAMING_SHUFFLE_READER_MAX_MEMORY)
  // Data and termination messages from all writers are put into this queue.
  private[spark] val messageQueue = new LinkedBlockingQueue[StreamingShuffleMessage]()

  private val memoryConsumer =
    new MemoryConsumer(context.taskMemoryManager(), MemoryMode.OFF_HEAP) {
      override def spill(size: Long, trigger: MemoryConsumer): Long = 0
    }

  private val shuffleChecksum = if (conf.get(STREAMING_SHUFFLE_CHECKSUM_ENABLED)) {
    new ShuffleChecksum()
  } else {
    null
  }

  // The set shuffle writers that this reader have successfully received
  // termination ack messages from.  This is used to make sure all term ack messages
  // are successfully sent before exiting.
  private[spark] val terminationAckControlMessageSet = ConcurrentHashMap.newKeySet[Long]()

  private val allTermAcksSentNotice = new Semaphore(0)

  // thread pool used to perform client creation in parallel
  private[spark] val clientCreationExecutor = ThreadUtils.newDaemonFixedThreadPool(
    Runtime.getRuntime.availableProcessors,
    s"streaming-shuffle-async-client-creation-${context.partitionId()}")

  // Signal for other threads that task discovery should be done
  // for example, we get all the term messages before we actually put the client
  // in the client map.  In this scenario we can just exit
  // without checking the client map on whether we can created all the clients
  // or not
  @volatile private var taskDiscoveryShouldStop = false

  private var currentDataMessage: StreamingShuffleMessage = _

  private def shutdownExecutorService(
      executor: java.util.concurrent.ExecutorService,
      name: String): Unit = {
    executor.shutdownNow()
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        logWarning(log"${MDC(LogKeys.NAME, name)} did not shut down within the timeout.")
      } else {
        logInfo(log"${MDC(LogKeys.NAME, name)} shut down successfully.")
      }
    } catch {
      case _: InterruptedException =>
        // make sure we clean up even if the task thread is interrupted during shutdown
        shutdownExecutorService(executor, name)
        // Restore the interrupt flag so downstream code knows we were interrupted
        Thread.currentThread().interrupt()
    }
  }

  /**
   * Cleans up all reader resources. This method should be idempotent and
   * can be called multiple times without issue.
   */
  private[streaming] def cleanupResources(): Unit = {
    val cleanupStartTime = System.currentTimeMillis()

    Utils.tryLogNonFatalError {
      stopTaskDiscovery()
    }
    Utils.tryLogNonFatalError {
      shutdownExecutorService(clientCreationExecutor, "Client Creation Executor")
    }
    Utils.tryLogNonFatalError {
      clientMap.forEach((_, client) => client.close())
    }
    Utils.tryLogNonFatalError {
      clientFactories.forEach(factory => factory.close())
    }
    Utils.tryLogNonFatalError {
      if (currentDataMessage != null) {
        currentDataMessage.release()
        currentDataMessage = null
      }
    }
    Utils.tryLogNonFatalError {
      val list = new java.util.ArrayList[StreamingShuffleMessage]()
      messageQueue.drainTo(list)
      list.forEach(_.release())
    }
    Utils.tryLogNonFatalError {
      memoryConsumer.freeMemory(memoryConsumer.getUsed())
    }
    logInfo(log"Resource cleanup took ${MDC(LogKeys.DURATION,
      System.currentTimeMillis() - cleanupStartTime)} ms")
  }

  // Register a task completion listener to make sure we clean up resources
  // when the task is completed.  Without this, threads, client connections/factories
  // message buffers, and memory allocations could be leaked.
  context.addTaskCompletionListener[Unit] { _ =>
    cleanupResources()
  }

  taskDiscoveryExecutor.execute(() => {
    val startTime = System.currentTimeMillis()
    try {
      logInfo(log"Task discovery thread started.")
      // a mapping of mapId and client creation future.  Used for parallel client creation
      val clientFutureMap = new ConcurrentHashMap[Long, CompletableFuture[Void]]()

      var isDone = false

      def shouldStop(): Boolean = {
        (
          // Got all locations for shuffle writers
          // and is creating the clients to communicate with them
          isDone
          // signal from main thread that task discovery should terminate.
          // For example, we got all the term messages from all clients
          || taskDiscoveryShouldStop
          // task has failed or interrupted.
          || context.isFailed()
          || context.isInterrupted()
          )
      }

      while (!shouldStop()) {
        val shuffleLocationResponseOption =
          tracker.getAvailableShuffleWriterTaskLocations(streamingShuffleHandle.shuffleId)

        var retryCount = 0
        shuffleLocationResponseOption.foreach {
          case ShuffleLocationResponse(shuffleWriterLocations, numShuffleWriters) =>
            if (!totalNumShuffleWriters.compareAndSet(-1, numShuffleWriters)) {
              assert(totalNumShuffleWriters.get() == numShuffleWriters)
            } else {
              perWriterByteLimit = Math.max(MAX_MEMORY / numShuffleWriters, 1)
              memoryConsumer.acquireMemory(MAX_MEMORY)
            }
            shuffleWriterLocations
              .foreach {
                case (mapId, location) =>
                  if (!clientFutureMap.containsKey(mapId)) {
                    val future = createClientAsync(mapId.toInt, location.host, location.port)
                      .thenAccept((shuffleClient: TransportClient) => {
                        clientMap.put(mapId, shuffleClient)
                        logInfo(
                          log"Created shuffle client to shuffle " +
                            log"writer with id ${MDC(LogKeys.MAP_ID, mapId)} and location ${MDC(
                              LogKeys.TASK_LOCATION, location)}. ${MDC(
                              LogKeys.NUM_CONNECTED_SHUFFLE_WRITERS, clientMap.size)} / ${MDC(
                              LogKeys.NUM_SHUFFLE_WRITERS, numShuffleWriters)} shuffle writer " +
                            log"tasks connected.")
                      }).exceptionally(th => {
                        val errorMsg = s"Error creating transport client to shuffle writer" +
                          s" with id ${mapId} and location ${location}."
                        // The real exception is likely wrapped in CompletionException
                        logError(errorMsg, th.getCause)
                        errorNotifier.markError(th.getCause)
                        throw new RuntimeException(errorMsg)
                      })
                    clientFutureMap.put(mapId, future)
                  }
              }

            val numClients = clientFutureMap.size()
            // we should not be getting more clients then expected
            assert(!(numClients > numShuffleWriters))
            if (numClients != numShuffleWriters) {
              // TODO also implement timeout
              Thread.sleep(10)
              retryCount += 1
              if (retryCount % 100 == 0) {
                logInfo(log"Still attempting to get shuffle writer locations." +
                  log" Got the location of ${MDC(LogKeys.NUM_CONNECTED_SHUFFLE_WRITERS,
                    clientFutureMap.size)} / ${MDC(LogKeys.NUM_SHUFFLE_WRITERS,
                    numShuffleWriters)} shuffle writers")
              }
            } else {
              // we have received all shuffle writer locations
              // and is the process of creating clients to connect to them
              isDone = true
            }
        }
      }

      if (isDone && !taskDiscoveryShouldStop && !context.isFailed() && !context.isInterrupted()) {
        // wait for all futures to finish
        CompletableFuture.allOf(clientFutureMap.values().asScala.toSeq: _*).get()
        assert(
          clientMap.size() == totalNumShuffleWriters.get(),
          s"actual num of clients: ${clientMap.size()} " +
          s"expected num clients: ${totalNumShuffleWriters.get()}"
        )
      }
    } catch {
      case th: Throwable =>
        logError(log"Task discovery thread failed.", th)
        errorNotifier.markError(th)
    } finally {
      clientCreationExecutor.shutdown()
      logInfo(log"Task discovery thread exited. Took ${MDC(LogKeys.DURATION,
        System.currentTimeMillis() - startTime)} ms")
    }
  })

  private def stopTaskDiscovery(): Unit = {
    taskDiscoveryShouldStop = true
    shutdownExecutorService(taskDiscoveryExecutor, "Task Discovery Executor")
  }

  private def createClientAsync(
      mapId: Int,
      remoteHost: String,
      remotePort: Int): CompletableFuture[TransportClient] = {
    val future = new CompletableFuture[TransportClient]()
    clientCreationExecutor.execute(() => {
      try {
        val shuffleClient = createClient(mapId, remoteHost, remotePort)
        future.complete(shuffleClient)
      } catch {
        case th: Throwable =>
          future.completeExceptionally(th)
      }
    })
    future
  }

  protected def onTermAckResponse(shuffleWriterId: Int): Unit = {
    terminationAckControlMessageSet.add(shuffleWriterId.toLong)
    val curSent = terminationAckControlMessageSet.size()
    val totalSentNeeded = totalNumShuffleWriters.get()
    logInfo(log"Termination ack message sent successfully to shuffle writer " +
      log"${MDC(LogKeys.SHUFFLE_WRITER_ID, shuffleWriterId)}." +
      log" ${MDC(LogKeys.NUM_TERMINATION_ACKS, curSent)} / " +
      log"${MDC(LogKeys.NUM_SHUFFLE_WRITERS, totalSentNeeded)} sent successfully.")

    // if we have sent all term acks successfully
    if (totalSentNeeded > 0 && curSent == totalSentNeeded) {
      allTermAcksSentNotice.release()
    }
  }

  /**
   * Verifies the checksum of a DataMessage if checksum is enabled.
   * @throws SparkRuntimeException if checksum verification fails
   */
  private def verifyDataMessageChecksum(dataMessage: DataMessage): Unit = {
    if (shuffleChecksum != null) {
      val data = dataMessage.data
      shuffleChecksum.reset()
      shuffleChecksum.updateChecksum(data, data.readerIndex(), data.readableBytes())
      val calculatedChecksum = shuffleChecksum.getValue()
      if (dataMessage.checksum != calculatedChecksum) {
        throw new SparkRuntimeException(
          errorClass = "STREAMING_SHUFFLE_CHECKSUM_VERIFICATION_FAILED",
          messageParameters = Map(
            "writerId" -> dataMessage.shuffleWriterId.toString,
            "expectedChecksum" -> dataMessage.checksum.toString,
            "calculatedChecksum" -> calculatedChecksum.toString,
            "dataLength" -> data.readableBytes().toString
          )
        )
      }
    }
  }

  // visible for test
  protected def createClient(mapId: Int, remoteHost: String, remotePort: Int): TransportClient = {
    val handler = clientHandler.getOrElse(new StreamingShuffleClientHandler(
      mapId,
      context.partitionId(),
      messageQueue,
      streamingShuffleHandle.shuffleId,
      perWriterByteLimit,
      context,
      errorNotifier
    ))
    handler.setOnTermAckResponseHandler(onTermAckResponse)
    val clientContext =
      new TransportContext(clientConf, handler)

    val factory = clientContext.createClientFactory()
    clientFactories.add(factory)
    factory.createClient(remoteHost, remotePort)
  }

  private var streamingShuffleReaderIteratorFactory: StreamingShuffleReaderIteratorFactory = _

  def setStreamingShuffleReaderIteratorFactory(
      factory: StreamingShuffleReaderIteratorFactory): Unit = {
    streamingShuffleReaderIteratorFactory = factory
  }

  private def checkTaskFailure(): Unit = {
    context.getTaskFailure.foreach(throw _)
    // Surface any background error on the task thread so that markTaskFailed and
    // markTaskCompleted are called from this thread, ensuring completion listeners
    // (including cleanupResources) run without contention.
    errorNotifier.throwErrorIfExists()
    if (context.isInterrupted()) {
      throw new InterruptedException("Task interrupted. Exiting read loop.")
    }
  }

  override def read(): Iterator[Product2[K, C]] = {
    val serializerInstance = streamingShuffleHandle.dependency.serializer.newInstance()
    // Termination messages are added to a set that contains the shuffle writer ids that have sent
    // termination messages. When the set size reaches the number of shuffle writers, we know
    // that we will not receive any future messages, and the reader can be closed. When a data
    // message is read, the actual data (UnsafeRow) is extracted and emitted through the iterator.
    val terminationControlMessageSet = collection.mutable.Set[Long]()

    /**
     * Returns true if the reader should stop after handling the termination message, which means
     * we have received termination messages from all shuffle writers.
     */
    def handleTerminationMessage(msg: TerminationControlMessage): Boolean = {
      val finishedId = msg.shuffleWriterId
      terminationControlMessageSet += finishedId
      val logMsg = if (totalNumShuffleWriters.get() > 0) {
        log"Got termination message from ${MDC(LogKeys.SHUFFLE_WRITER_ID, finishedId)}." +
          log" ${MDC(LogKeys.NUM_TERMINATION_ACKS, terminationControlMessageSet.size)} / ${
            MDC(LogKeys.NUM_SHUFFLE_WRITERS, totalNumShuffleWriters.get())}" +
          log" termination messages received."
      } else {
        log"Got termination message from ${MDC(LogKeys.SHUFFLE_WRITER_ID, finishedId)}." +
          log" ${MDC(LogKeys.NUM_TERMINATION_ACKS, terminationControlMessageSet.size)}" +
          log" termination messages received."
      }
      logInfo(logMsg)
      if (totalNumShuffleWriters.get() > 0
        && totalNumShuffleWriters.get() == terminationControlMessageSet.size) {
        logInfo(
          log"Got termination messages from all" +
            log" shuffle writers ${MDC(LogKeys.SHUFFLE_WRITERS,
              terminationControlMessageSet)}. Shutting down.")

        // make sure all term acks have been sent successfully
        while (!allTermAcksSentNotice.tryAcquire(100, TimeUnit.MILLISECONDS)) {
          checkTaskFailure()
        }
        true
      } else {
        false
      }
    }

    def handleDataMessage(dataMessage: DataMessage): Iterator[(K, C)] = {
      currentDataMessage = dataMessage
      verifyDataMessageChecksum(dataMessage)
      val recordData = dataMessage.getRecordData()
      val deserializedIterator = serializerInstance
        .deserializeStream(new ByteBufInputStream(recordData))
        .asKeyValueIterator
        .asInstanceOf[Iterator[(K, C)]]
      assert(
        deserializedIterator.hasNext,
        formatMessage(
          s"data message that had no data to deserialize:" +
            s" ${dataMessage.data}. Readable bytes: " +
            s"${dataMessage.data.readableBytes()}"
        )
      )
      new NextIterator[(K, C)] {
        override def getNext(): (K, C) = {
          if (!deserializedIterator.hasNext) {
            finished = true
            null.asInstanceOf[(K, C)]
          } else {
            deserializedIterator.next()
          }
        }
        override def close(): Unit = {
          dataMessage.release()
          currentDataMessage = null
        }
      }
    }

    if (streamingShuffleReaderIteratorFactory == null) {
      streamingShuffleReaderIteratorFactory = new StreamingShuffleReaderIteratorFactory()
    }
    streamingShuffleReaderIteratorFactory.create(
      messageQueue,
      handleTerminationMessage,
      handleDataMessage,
      checkTaskFailure
    )
  }
}
