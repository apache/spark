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
package org.apache.spark.sql.connect

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.broadcast.Broadcast

/**
 * (SPARK-51705) A client-side stand-in for a driver-side [[Broadcast]] that a Spark Connect Scala
 * client can capture inside a UDF closure even though the client has no SparkContext (so it can
 * never call `sc.broadcast(v)` to obtain a real [[org.apache.spark.broadcast.TorrentBroadcast]]).
 *
 * This is the Scala analogue of the Python `ConnectBroadcast` proxy
 * (`python/pyspark/sql/connect/broadcast.py`). It solves the same gap that motivated
 * `SparkSession.broadcast()` for Python: the Connect client is JVM-less with respect to the
 * cluster, so the user cannot construct the `Broadcast[T]` object that classic Spark expects them
 * to capture.
 *
 * End to end (Scala): `SparkSession.broadcast(v)` serializes `v`, uploads it through the cache
 * artifact channel, sends a `CreateBroadcastCommand(value_type = JVM)`, and returns a
 * `ConnectBroadcast[T]` holding the server-assigned `Broadcast.id` (plus the local value for
 * driver-side `.value` reads). When the user captures it in a UDF, [[writeReplace]] substitutes
 * an id-only [[ConnectBroadcastRef]] token into the serialized closure; the captured value itself
 * is never written to the wire (it already travels once, out of band, via the cache artifact). On
 * the server, [[ConnectBroadcastRef.readResolve]] swaps the token for the real driver-side
 * `Broadcast[T]` while the closure is deserialized (see `SparkConnectPlanner.unpackScalaUDF`).
 *
 * @param bid
 *   the server-assigned driver-side `Broadcast.id`
 * @param value_
 *   the local value, retained only so client-side `.value` reads work (parity with classic
 *   driver-side reads). Marked `@transient` so it is never serialized even if [[writeReplace]]
 *   were somehow bypassed.
 */
private[sql] class ConnectBroadcast[T: ClassTag](
    bid: Long,
    @transient private val value_ : T,
    @transient private val unpersistFn: (Long, Boolean, Boolean) => Unit)
    extends Broadcast[T](bid) {

  override protected def getValue(): T = value_

  override protected def doUnpersist(blocking: Boolean): Unit =
    unpersistFn(id, blocking, false)

  override protected def doDestroy(blocking: Boolean): Unit =
    unpersistFn(id, blocking, true)

  /**
   * When the enclosing UDF closure is Java-serialized, substitute an id-only token for this
   * object. Mirrors the Python `ConnectBroadcast.__reduce__ -> (_from_id, (bid,))` contract.
   * Because this returns the token, neither `value_` (already `@transient`) nor any
   * SparkContext-bound state is ever serialized -- which is essential, since none exists on the
   * client. Recording the id here (serialization is what proves the broadcast was captured) lets
   * `UdfToProtoUtils.toProto` drain the exact set of captured ids into
   * `ScalarScalaUDF.broadcast_ids`.
   */
  private def writeReplace(): AnyRef = {
    ConnectBroadcastCapture.record(id)
    new ConnectBroadcastRef(id)
  }
}

/**
 * The wire token that stands in for a [[ConnectBroadcast]] inside a serialized Scala UDF closure.
 * Carries only the broadcast id.
 *
 * [[readResolve]] runs during closure deserialization. On the server, `SparkConnectPlanner`
 * installs the per-session broadcast registry into [[ConnectBroadcastResolver]] immediately
 * before deserializing, so this resolves the id to the real driver-side `Broadcast[_]`. When no
 * registry is bound (for example the client-side `checkDeserializable` round-trip that
 * `UdfToProtoUtils.toUdfPacketBytes` performs to validate the closure), it returns a lightweight
 * unresolved placeholder rather than failing -- that placeholder is only ever produced during the
 * client's own validation round-trip and is never executed.
 */
private[sql] class ConnectBroadcastRef(val id: Long) extends Serializable {
  private def readResolve(): AnyRef =
    ConnectBroadcastResolver.resolve(id).getOrElse(new UnresolvedConnectBroadcast(id))
}

/**
 * Placeholder produced by [[ConnectBroadcastRef.readResolve]] when no registry is bound to the
 * deserializing thread (client-side validation round-trip only). It intentionally throws if its
 * value is ever read, so a misuse cannot silently return wrong data.
 */
private[sql] class UnresolvedConnectBroadcast(bid: Long) extends Broadcast[Any](bid) {
  private def fail(): Nothing = throw new IllegalStateException(
    s"ConnectBroadcast($bid) was not resolved to a driver-side broadcast. This placeholder is " +
      "only expected during client-side closure validation and must never be executed.")
  override protected def getValue(): Any = fail()
  override protected def doUnpersist(blocking: Boolean): Unit = fail()
  override protected def doDestroy(blocking: Boolean): Unit = fail()
}

/**
 * Thread-local bridge that lets [[ConnectBroadcastRef.readResolve]] (which receives no context
 * from `ObjectInputStream`) reach the per-session broadcast registry that only the server's
 * `SparkConnectPlanner` holds.
 *
 * This type lives in `sql/connect/common` so both the client (which defines the token) and the
 * server (which depends on common) see the identical class -- required for Java deserialization
 * to bind the token. A thread-local is safe because `unpackScalaUDF` deserializes synchronously
 * on the request-handling thread; the value is always cleared in a `finally`.
 */
private[sql] object ConnectBroadcastResolver {
  private val bound = new ThreadLocal[Map[Long, Broadcast[_]]]()

  def withRegistry[R](registry: Map[Long, Broadcast[_]])(body: => R): R = {
    if (registry.isEmpty) {
      // Nothing to resolve; avoid touching the thread-local at all.
      body
    } else {
      val prev = bound.get()
      bound.set(registry)
      try body
      finally {
        if (prev == null) bound.remove() else bound.set(prev)
      }
    }
  }

  def resolve(id: Long): Option[Broadcast[_]] =
    Option(bound.get()).flatMap(_.get(id))
}

/**
 * Client-side thread-local that records the ids of [[ConnectBroadcast]]s captured while building
 * a single UDF proto. `UdfToProtoUtils.toProto` drains this into `ScalarScalaUDF.broadcast_ids`
 * right after serializing the closure (serialization is what triggers
 * [[ConnectBroadcast.writeReplace]]), mirroring the Python `PythonUDF.to_plan` drain of its
 * `threading.local` registry. Drain-then- clear on the same plan-build thread.
 */
private[sql] object ConnectBroadcastCapture {
  private val captured = new ThreadLocal[mutable.LinkedHashSet[Long]] {
    override def initialValue(): mutable.LinkedHashSet[Long] = mutable.LinkedHashSet.empty
  }

  def record(id: Long): Unit = captured.get() += id

  /** Return the captured ids and clear the thread-local. */
  def drain(): Seq[Long] = {
    val ids = captured.get().toSeq
    captured.remove()
    ids
  }
}
