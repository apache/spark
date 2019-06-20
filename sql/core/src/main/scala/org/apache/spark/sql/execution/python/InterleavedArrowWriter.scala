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

package org.apache.spark.sql.execution.python

import java.io.OutputStream
import java.nio.channels.Channels

import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer


class InterleavedArrowWriter( leftRoot: VectorSchemaRoot,
                              rightRoot: VectorSchemaRoot,
                              out: WriteChannel) extends AutoCloseable{


  private var started = false
  private val leftUnloader = new VectorUnloader(leftRoot)
  private val rightUnloader = new VectorUnloader(rightRoot)

  def start(): Unit = {
    this.ensureStarted()
  }

  def writeBatch(): Unit = {
    this.ensureStarted()
    val leftBatch = leftUnloader.getRecordBatch
    val rightBatch = rightUnloader.getRecordBatch
    MessageSerializer.serialize(out, leftBatch)
    MessageSerializer.serialize(out, rightBatch)
    leftBatch.close()
    rightBatch.close()
  }

  private def ensureStarted(): Unit = {
    if (!started) {
      started = true
      MessageSerializer.serialize(out, leftRoot.getSchema)
      MessageSerializer.serialize(out, rightRoot.getSchema)
    }
  }

  def end(): Unit = {
    ensureStarted()
    ensureEnded()
  }

  def ensureEnded(): Unit = {
    out.writeIntLittleEndian(0)
  }

  def close(): Unit = {
    out.close()
  }

}

object InterleavedArrowWriter{

  def apply(leftRoot: VectorSchemaRoot,
            rightRoot: VectorSchemaRoot,
            out: OutputStream): InterleavedArrowWriter = {
    new InterleavedArrowWriter(leftRoot, rightRoot, new WriteChannel(Channels.newChannel(out)))
  }

}
