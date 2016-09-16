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

package org.apache.spark.streaming.receiver

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

/** Trait representing a received block */
private[streaming] sealed trait ReceivedBlock

/** class representing a block received as an ArrayBuffer */
private[streaming] case class ArrayBufferBlock(arrayBuffer: ArrayBuffer[_]) extends ReceivedBlock

/** class representing a block received as an Iterator */
private[streaming] case class IteratorBlock(iterator: Iterator[_]) extends ReceivedBlock

/** class representing a block received as a ByteBuffer */
private[streaming] case class ByteBufferBlock(byteBuffer: ByteBuffer) extends ReceivedBlock
