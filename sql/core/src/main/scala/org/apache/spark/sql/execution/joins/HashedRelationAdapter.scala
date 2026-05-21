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

package org.apache.spark.sql.execution.joins

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import io.netty.buffer.Unpooled

import org.apache.spark.SparkEnv
import org.apache.spark.network.buffer.{ManagedBuffer, NettyManagedBuffer}
import org.apache.spark.network.util.NettyUtils
import org.apache.spark.shard.{ShardLookupAdapter, ShardManager}
import org.apache.spark.sql.catalyst.expressions.{BasePredicate, Expression, Predicate, UnsafeRow}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.types.StructType

/**
 * Build-side RPC handler for distributed map join.
 *
 * Receives batched key lookups from probe-side executors, performs hash
 * lookups against the local [[HashedRelation]], and returns matching rows.
 *
 * When a build-only filter is stored in the shard meta, it is loaded once
 * per setId and evaluated server-side to reduce network transfer.
 *
 * Wire format (request):
 * {{{
 *   (setId:long)(shardId:int)(numKeyFields:int)
 *   [(keyLen:int)(keyBytes)]...
 * }}}
 */
private[spark] class HashedRelationAdapter extends ShardLookupAdapter {

  private val INITIAL_RESPONSE_BUFFER_BYTES = 1 << 20
  private val alloc = NettyUtils.getSharedPooledByteBufAllocator(true, true)

  private val filterCache = new ConcurrentHashMap[Long, Option[BasePredicate]]()
  private val cleanupRegistered = new AtomicBoolean(false)

  override def lookup(manager: ShardManager, reqMsg: ManagedBuffer): ManagedBuffer = {
    if (cleanupRegistered.compareAndSet(false, true)) {
      manager.registerCleanupCallback(setId => filterCache.remove(setId))
    }
    val keysBuf = Unpooled.wrappedBuffer(reqMsg.nioByteBuffer())
    val setId = keysBuf.readLong()
    val shard = keysBuf.readInt()
    val numKeyFields = keysBuf.readInt()

    val keyUr = new UnsafeRow(numKeyFields)
    val rel = manager.getLocalValue[HashedRelation](setId, shard).asReadOnlyCopy()
    val valuesBuf = alloc.buffer(INITIAL_RESPONSE_BUFFER_BYTES)
    valuesBuf.writeLong(setId)
    valuesBuf.writeInt(shard)

    val advanceReadKey = UnsafeRowBufCodec.makeAdvanceRead(keyUr, keysBuf)
    val advanceWrite = UnsafeRowBufCodec.makeAdvanceWrite(valuesBuf)

    val predicate = filterCache.computeIfAbsent(setId, _ => {
      manager.getFilterBytes(setId, shard).map { case (exprBytes, schemaBytes) =>
        val ser = SparkEnv.get.serializer.newInstance()
        val expr = ser.deserialize[Expression](java.nio.ByteBuffer.wrap(exprBytes))
        if (schemaBytes.nonEmpty) {
          val schema = ser.deserialize[StructType](java.nio.ByteBuffer.wrap(schemaBytes))
          Predicate.create(expr, DataTypeUtils.toAttributes(schema))
        } else {
          Predicate.create(expr)
        }
      }
    })

    predicate match {
      case Some(pred) =>
        while (keysBuf.isReadable) {
          val klen = keysBuf.readInt()
          val key = advanceReadKey(klen)
          val iter = rel.get(key)
          if (iter != null) {
            while (iter.hasNext) {
              val buildRow = iter.next().asInstanceOf[UnsafeRow]
              if (pred.eval(buildRow)) {
                valuesBuf.writeInt(buildRow.getSizeInBytes)
                advanceWrite(buildRow)
              }
            }
          }
          valuesBuf.writeInt(0)
        }
      case None =>
        while (keysBuf.isReadable) {
          val klen = keysBuf.readInt()
          val key = advanceReadKey(klen)
          val iter = rel.get(key)
          if (iter != null) {
            while (iter.hasNext) {
              val buildRow = iter.next().asInstanceOf[UnsafeRow]
              valuesBuf.writeInt(buildRow.getSizeInBytes)
              advanceWrite(buildRow)
            }
          }
          valuesBuf.writeInt(0)
        }
    }

    new NettyManagedBuffer(valuesBuf)
  }
}
