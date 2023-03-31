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

package org.apache.spark.broadcast

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.rpc.RpcCallContext
import org.apache.spark.rpc.RpcEndpoint
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util.ByteBufferInputStream
import org.apache.spark.util.RpcUtils
import org.apache.spark.util.SerializableBuffer
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.Utils


private[spark] sealed trait SmallBroadcastTrackerMessage

private[spark] case class GetSmallBroadcastData(id: Long)
  extends SmallBroadcastTrackerMessage

private[spark] case class RemoveSmallBroadcastData(id: Long)
  extends SmallBroadcastTrackerMessage

/** RpcEndpoint class for SmallBroadcastTrackerMaster */
private[spark] class SmallBroadcastTrackerMasterEndpoint(
  override val rpcEnv: RpcEnv, val tracker: SmallBroadcastTrackerMaster)
  extends RpcEndpoint with Logging {

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetSmallBroadcastData(id: Long) =>
      val hostPort = context.senderAddress.hostPort
      logInfo(s"Asked from $hostPort to send small broadcast data for $id")
      val bytesOpt = tracker.getSerialized(id)
        .map(bytes => new SerializableBuffer(ByteBuffer.wrap(bytes, 0, bytes.length)))
      context.reply(bytesOpt)
    case RemoveSmallBroadcastData(id: Long) =>
      val hostPort = context.senderAddress.hostPort
      logInfo(s"Asked from $hostPort to remove small broadcast data for $id")
      tracker.unregister(id)
      context.reply(true)
  }
}

private[spark] abstract class SmallBroadcastTracker(val conf: SparkConf)
  extends Logging {
  // This points to SmallBroadcastTrackerMasterEndpoint
  var trackerEndpoint: RpcEndpointRef = _

  /**
   * Send a message to the trackerEndpoint and get its result within a default timeout, or
   * throw a SparkException if this fails.
   */
  protected def askTracker[T: ClassTag](message: Any): T = {
    try {
      trackerEndpoint.askSync[T](message)
    } catch {
      case e: Exception =>
        val errMsg = "Error communicating with SmallBroadcastTracker " + message
        logError(errMsg, e)
        throw new SparkException(errMsg, e)
    }
  }

  def register[T: ClassTag](id: Long, obj: T): Unit

  def unregister(id: Long): Unit

  def get[T: ClassTag](id: Long): Option[T]

  def destroy(id: Long, blocking: Boolean): Unit = {
    // We only remove from master, data in worker will be expired automatically
    val future = trackerEndpoint.ask[Boolean](RemoveSmallBroadcastData(id))
    future.failed.foreach(e => {
      logWarning(s"Failed to remove SmallBroadcast $id ${e.getMessage}", e)
    })(ThreadUtils.sameThread)
    if (blocking) {
      // the underlying Futures will timeout anyway, so it's safe to use infinite timeout here
      RpcUtils.INFINITE_TIMEOUT.awaitResult(future)
    }
  }

  def stop(): Unit
}

private[spark] class SmallBroadcastTrackerMaster(conf: SparkConf)
  extends SmallBroadcastTracker(conf) {

  private val serializationWorker = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonSingleThreadExecutor("small-broadcast-tracker-master-worker"))

  /**
   * When registering data, serialization will be run in worker thread asynchronously,
   * which will update the value to future later
   */
  private[spark] val cache = new ConcurrentHashMap[Long, (Any, Future[Array[Byte]])]

  override def register[T: ClassTag](id: Long, obj: T): Unit = {
    cache.computeIfAbsent(id, id => {
      (obj, Future {
        SmallBroadcastTracker.serializeData(obj)
      }(serializationWorker))
    })._2.failed.foreach(exception => {
      logError(s"Error during broadcast ${id} serialization: $exception, will remove data" +
        s" from cache")
      cache.remove(id)
    })(serializationWorker)
  }

  override def unregister(id: Long): Unit = {
    cache.remove(id)
  }

  override def get[T: ClassTag](id: Long): Option[T] = {
    Option(cache.get(id)).map(_._1.asInstanceOf[T])
  }

  /**
   * If key not exist => return None
   * If key exist, block until serialization done
   *    - success => return Some(data)
   *    - fail => return None
   */
  def getSerialized(id: Long): Option[Array[Byte]] = {
    Option(cache.get(id)).flatMap(value => {
      val completedFuture = ThreadUtils.awaitReady(value._2, 10.seconds)
      completedFuture.value.get match {
        case Success(value) =>
          Some(value)
        case Failure(exception) =>
          logError(s"exception during SmallBroadcast $id serialization: $exception")
          None
      }
    })
  }

  override def stop(): Unit = {
    serializationWorker.shutdown()
    cache.clear()
  }
}

private[spark] class SmallBroadcastTrackerWorker(conf: SparkConf)
  extends SmallBroadcastTracker(conf) {
  /**
   * Use LoadingCache with time eviction policy to expire old data, since executor is not aware of
   * stage completion
   */
  private[spark] val cache: LoadingCache[java.lang.Long, AnyRef] = {
    CacheBuilder.newBuilder()
      .maximumSize(conf.get(config.SMALL_BROADCAST_EXECUTOR_CACHE_SIZE))
      .expireAfterAccess(
        conf.get(config.SMALL_BROADCAST_EXECUTOR_CACHE_EXPIRE_SECONDS), TimeUnit.SECONDS)
      .build[java.lang.Long, AnyRef](
        new CacheLoader[java.lang.Long, AnyRef]() {
          /**
           * Fetch from remote if not exist in cache. If another thread is currently loading the
           * value for this key, simply waits for that thread to finish and returns that value. So
           * we don't need extra synchronization here
           */
          override def load(key: java.lang.Long): AnyRef = {
            logInfo("Smallbroadcast load data from remote " + key)
            askTracker[Option[SerializableBuffer]](GetSmallBroadcastData(key))
              .map(buf => SmallBroadcastTracker.deserializeData(buf.buffer))
              .getOrElse(throw new Exception(s"failed to load SmallBroadcast ${key} from remote"))
          }
        })
  }

  /**
   * Get data in local cache or fetch from remote.
   */
  override def get[T: ClassTag](id: Long): Option[T] = {
    Option(cache.get(id).asInstanceOf[T])
  }

  override def register[T: ClassTag](id: Long, obj: T): Unit = {
    cache.put(id, obj.asInstanceOf[AnyRef])
  }

  override def unregister(id: Long): Unit = {
    cache.invalidate(id)
  }

  override def stop(): Unit = {
    cache.invalidateAll()
  }
}


private[spark] object SmallBroadcastTracker {

  val ENDPOINT_NAME = "SmallBroadcastTracker"

  def serializeData[T: ClassTag](obj: T): Array[Byte] = {
    val out = new ByteArrayOutputStream(1024)
    val objOut = new ObjectOutputStream(out)
    Utils.tryWithSafeFinally {
      objOut.writeObject(obj)
    } {
      objOut.close()
    }
    out.toByteArray
  }

  def deserializeData(buffer: ByteBuffer): AnyRef = {
    assert(buffer.limit() > 0)
    val objIn = new ObjectInputStream(new ByteBufferInputStream(buffer))
    Utils.tryWithSafeFinally {
      objIn.readObject()
    } {
      objIn.close()
    }
  }
}

/**
 * A lightweight implementation of [[org.apache.spark.broadcast.Broadcast]] with star topology and
 * in-memory storage.
 *
 * The mechanism is as follows:
 *
 * The driver store the serialized object in memory.
 *
 * On each executor, the executor first attempts to get the data from local cache, if not exist
 * then fetch from driver and cache it.
 *
 * In terms of small data, this won't create too much pressure on driver side and will be more
 * efficient, which is suitable for lightweight and latency sensitive job.
 *
 * When initialized, SmallBroadcast objects read SparkEnv.get.conf.
 *
 * @param obj object to broadcast
 * @param id A unique identifier for the broadcast variable.
 */
private[spark] class SmallBroadcast[T: ClassTag](obj: T, id: Long)
  extends Broadcast[T](id) with Logging with Serializable {

  /**
   * The value should always exist on driver broadcast object. On executor, it's fetched from
   * remote and cached in tracker
   */
  @transient private var _value: Option[T] = None
  /** Is the execution in local mode. */
  @transient private var isLocalMaster: Boolean = _

  private def setConf(conf: SparkConf): Unit = {
    isLocalMaster = Utils.isLocalMaster(conf)
  }
  setConf(SparkEnv.get.conf)

  override protected def getValue(): T = synchronized {
    if (_value.isEmpty) {
      _value = Some(SparkEnv.get.broadcastManager.smallBroadcastTracker.get(id).get)
    }
    _value.get
  }

  override protected def doUnpersist(blocking: Boolean): Unit = {
    SparkEnv.get.broadcastManager.smallBroadcastTracker.destroy(id, blocking)
  }

  override protected def doDestroy(blocking: Boolean): Unit = {
    SparkEnv.get.broadcastManager.smallBroadcastTracker.destroy(id, blocking)
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    _value = None
  }

  /** Used by the JVM when serializing this object. */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    assertValid()
    out.defaultWriteObject()
  }
}
