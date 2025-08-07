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

package org.apache.spark.sql.execution.python.streaming.benchmark

import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.StandardProtocolFamily
import java.net.UnixDomainSocketAddress
import java.nio.channels.ServerSocketChannel
import java.util.UUID

import scala.collection.mutable

import org.apache.spark.internal.config.Python.PYTHON_UNIX_DOMAIN_SOCKET_DIR
import org.apache.spark.internal.config.Python.PYTHON_UNIX_DOMAIN_SOCKET_ENABLED
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.python.streaming.TransformWithStateInPySparkStateServer
import org.apache.spark.sql.execution.streaming.ImplicitGroupingKeyTracker
import org.apache.spark.sql.execution.streaming.QueryInfoImpl
import org.apache.spark.sql.execution.streaming.StatefulProcessorHandleImplBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.ListState
import org.apache.spark.sql.streaming.MapState
import org.apache.spark.sql.streaming.QueryInfo
import org.apache.spark.sql.streaming.TimeMode
import org.apache.spark.sql.streaming.TTLConfig
import org.apache.spark.sql.streaming.ValueState
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * This is a benchmark purposed implementation of StatefulProcessorHandleImplBase that stores
 * state in memory. This leverages Scala collection types.
 *
 * NOTE: TTL is not supported in this implementation since it complicates the thing a lot and
 * this is the benchmark purposed implementation.
 */
class InMemoryStatefulProcessorHandleImpl(
    timeMode: TimeMode,
    keyExprEnc: ExpressionEncoder[Any])
  extends StatefulProcessorHandleImplBase(timeMode, keyExprEnc) {

  class InMemoryValueState[T] extends ValueState[T] {
    private val keyToStateValue = mutable.Map[Any, T]()

    private def getValue: Option[T] = {
      keyToStateValue.get(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
    }

    override def exists(): Boolean = {
      getValue.isDefined
    }

    override def get(): T = getValue.getOrElse(null.asInstanceOf[T])

    override def update(newState: T): Unit = {
      keyToStateValue.put(ImplicitGroupingKeyTracker.getImplicitKeyOption.get, newState)
    }

    override def clear(): Unit = {
      keyToStateValue.remove(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
    }
  }

  class InMemoryListState[T] extends ListState[T] {
    private val keyToStateValue = mutable.Map[Any, mutable.ArrayBuffer[T]]()

    private def getList: Option[mutable.ArrayBuffer[T]] = {
      keyToStateValue.get(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
    }

    override def exists(): Boolean = getList.isDefined

    override def get(): Iterator[T] = {
      getList.orElse(Some(mutable.ArrayBuffer.empty[T])).get.iterator
    }

    override def put(newState: Array[T]): Unit = {
      keyToStateValue.put(
        ImplicitGroupingKeyTracker.getImplicitKeyOption.get,
        mutable.ArrayBuffer.empty[T] ++ newState
      )
    }

    override def appendValue(newState: T): Unit = {
      if (!exists()) {
        keyToStateValue.put(
          ImplicitGroupingKeyTracker.getImplicitKeyOption.get,
          mutable.ArrayBuffer.empty[T]
        )
      }
      keyToStateValue(ImplicitGroupingKeyTracker.getImplicitKeyOption.get) += newState
    }

    override def appendList(newState: Array[T]): Unit = {
      if (!exists()) {
        keyToStateValue.put(
          ImplicitGroupingKeyTracker.getImplicitKeyOption.get,
          mutable.ArrayBuffer.empty[T]
        )
      }
      keyToStateValue(ImplicitGroupingKeyTracker.getImplicitKeyOption.get) ++= newState
    }

    override def clear(): Unit = {
      keyToStateValue.remove(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
    }
  }

  class InMemoryMapState[K, V] extends MapState[K, V] {
    private val keyToStateValue = mutable.Map[Any, mutable.HashMap[K, V]]()

    private def getMap: Option[mutable.HashMap[K, V]] = {
      keyToStateValue.get(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
    }

    override def exists(): Boolean = getMap.isDefined

    override def getValue(key: K): V = {
      getMap
        .orElse(Some(mutable.HashMap.empty[K, V]))
        .get
        .getOrElse(key, null.asInstanceOf[V])
    }

    override def containsKey(key: K): Boolean = {
      getMap
        .orElse(Some(mutable.HashMap.empty[K, V]))
        .get
        .contains(key)
    }

    override def updateValue(key: K, value: V): Unit = {
      if (!exists()) {
        keyToStateValue.put(
          ImplicitGroupingKeyTracker.getImplicitKeyOption.get,
          mutable.HashMap.empty[K, V]
        )
      }

      keyToStateValue(ImplicitGroupingKeyTracker.getImplicitKeyOption.get) += (key -> value)
    }

    override def iterator(): Iterator[(K, V)] = {
      getMap
        .orElse(Some(mutable.HashMap.empty[K, V]))
        .get
        .iterator
    }

    override def keys(): Iterator[K] = {
      getMap
        .orElse(Some(mutable.HashMap.empty[K, V]))
        .get
        .keys
        .iterator
    }

    override def values(): Iterator[V] = {
      getMap
        .orElse(Some(mutable.HashMap.empty[K, V]))
        .get
        .values
        .iterator
    }

    override def removeKey(key: K): Unit = {
      getMap
        .orElse(Some(mutable.HashMap.empty[K, V]))
        .get
        .remove(key)
    }

    override def clear(): Unit = {
      keyToStateValue.remove(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
    }
  }

  class InMemoryTimers {
    private val keyToTimers = mutable.Map[Any, mutable.TreeSet[Long]]()

    def registerTimer(expiryTimestampMs: Long): Unit = {
      val groupingKey = ImplicitGroupingKeyTracker.getImplicitKeyOption.get
      if (!keyToTimers.contains(groupingKey)) {
        keyToTimers.put(groupingKey, mutable.TreeSet[Long]())
      }
      keyToTimers(groupingKey).add(expiryTimestampMs)
    }

    def deleteTimer(expiryTimestampMs: Long): Unit = {
      val groupingKey = ImplicitGroupingKeyTracker.getImplicitKeyOption.get
      if (keyToTimers.contains(groupingKey)) {
        keyToTimers(groupingKey).remove(expiryTimestampMs)
      }
    }

    def listTimers(): Iterator[Long] = {
      val groupingKey = ImplicitGroupingKeyTracker.getImplicitKeyOption.get
      keyToTimers.get(groupingKey) match {
        case Some(timers) => timers.iterator
        case None => Iterator.empty
      }
    }
  }

  override def getValueState[T](
      stateName: String,
      valEncoder: Encoder[T],
      ttlConfig: TTLConfig): ValueState[T] = {
    assert(ttlConfig == TTLConfig.NONE, "TTL is not supported in this implementation")
    new InMemoryValueState[T]
  }

  override def getValueState[T: Encoder](stateName: String, ttlConfig: TTLConfig): ValueState[T] = {
    getValueState(stateName, implicitly[Encoder[T]], ttlConfig)
  }

  override def getListState[T](
      stateName: String,
      valEncoder: Encoder[T],
      ttlConfig: TTLConfig): ListState[T] = {
    assert(ttlConfig == TTLConfig.NONE, "TTL is not supported in this implementation")
    new InMemoryListState[T]
  }

  override def getListState[T: Encoder](stateName: String, ttlConfig: TTLConfig): ListState[T] = {
    getListState(stateName, implicitly[Encoder[T]], ttlConfig)
  }

  override def getMapState[K, V](
      stateName: String,
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V],
      ttlConfig: TTLConfig): MapState[K, V] = {
    assert(ttlConfig == TTLConfig.NONE, "TTL is not supported in this implementation")
    new InMemoryMapState[K, V]
  }

  override def getMapState[K: Encoder, V: Encoder](
      stateName: String,
      ttlConfig: TTLConfig): MapState[K, V] = {
    getMapState(stateName, implicitly[Encoder[K]], implicitly[Encoder[V]], ttlConfig)
  }

  override def getQueryInfo(): QueryInfo = {
    new QueryInfoImpl(UUID.randomUUID(), UUID.randomUUID(), 0L)
  }

  private val timers = new InMemoryTimers()

  override def registerTimer(expiryTimestampMs: Long): Unit = {
    timers.registerTimer(expiryTimestampMs)
  }

  override def deleteTimer(expiryTimestampMs: Long): Unit = {
    timers.deleteTimer(expiryTimestampMs)
  }

  override def listTimers(): Iterator[Long] = {
    timers.listTimers()
  }

  override def deleteIfExists(stateName: String): Unit = {
    // No-op for this implementation - we don't even track the state definitions
  }
}

// scalastyle:off line.size.limit
/**
 * This spins up standalone TransformWithStateInPySparkStateServer with in-memory state
 * implementations. This is useful for understanding the performance of state intercommunication,
 * since the logic of state processing is really lightweight (compared to the actual
 * implementation leveraging RocksDB).
 *
 * The instruction to run this benchmark:
 * 1. Build Spark with `./dev/make-distribution.sh`
 * 2. `cd dist`
 * 3. `java -classpath "./jars&#47;*" org.apache.spark.sql.execution.python.streaming.benchmark.BenchmarkTransformWithStateInPySparkStateServer`
 *    To run this with Unix Domain Socket, set the environment variable `PYSPARK_UDS_MODE=true`
 *
 * The app will show the port number of the server, which is needed to connect to the server.
 */
// scalastyle:on line.size.limit
object BenchmarkTransformWithStateInPySparkStateServer extends App {
  val spark = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()

  val sqlConf = spark.conf

  // configurations for TransformWithStateInPySparkStateServer
  val timeZoneId: String = sqlConf.get(SQLConf.SESSION_LOCAL_TIMEZONE)
  val errorOnDuplicatedFieldNames: Boolean = true
  val largeVarTypes: Boolean = sqlConf.get(
    SQLConf.ARROW_EXECUTION_USE_LARGE_VAR_TYPES)
  val arrowTransformWithStateInPySparkMaxRecordsPerBatch =
    sqlConf.get(SQLConf.ARROW_TRANSFORM_WITH_STATE_IN_PYSPARK_MAX_STATE_RECORDS_PER_BATCH)

  // key schema
  val groupingKeySchema = StructType(
    Array(
      StructField("groupingKey", StringType)
    )
  )

  // Start with a socket - using random port
  var serverSocketChannel: ServerSocketChannel = _
  var sockPath: File = null
  var stateServerSocketPort = -1

  val isUnixDomainSock = spark.sparkContext.conf.get(PYTHON_UNIX_DOMAIN_SOCKET_ENABLED)

  if (isUnixDomainSock) {
    sockPath = new File(
      spark.sparkContext.conf.get(PYTHON_UNIX_DOMAIN_SOCKET_DIR)
        .getOrElse(System.getProperty("java.io.tmpdir")),
      s".${UUID.randomUUID()}.sock")
    serverSocketChannel = ServerSocketChannel.open(StandardProtocolFamily.UNIX)
    sockPath.deleteOnExit()
    serverSocketChannel.bind(UnixDomainSocketAddress.of(sockPath.getPath))
  } else {
    serverSocketChannel = ServerSocketChannel.open()
      .bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 1)
    stateServerSocketPort = serverSocketChannel.socket().getLocalPort
  }

  val stateHandleImpl = new InMemoryStatefulProcessorHandleImpl(
    TimeMode.None(),
    // NOTE: This is actually not used, so no need to worry about its validity
    null
  )

  val stateServer = new TransformWithStateInPySparkStateServer(
    serverSocketChannel,
    stateHandleImpl,
    groupingKeySchema,
    timeZoneId,
    errorOnDuplicatedFieldNames,
    largeVarTypes,
    arrowTransformWithStateInPySparkMaxRecordsPerBatch
  )
  // scalastyle:off println
  if (stateServerSocketPort >= 0) {
    println(
      s"TransformWithStateInPySparkStateServer is starting on port $stateServerSocketPort")
  } else {
    println(
      s"TransformWithStateInPySparkStateServer is starting with UDS at " +
        s"${sockPath.getAbsolutePath}")
  }
  // scalastyle:on println
  try {
    stateServer.run()
  } finally {
    serverSocketChannel.close()
  }
}
