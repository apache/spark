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

package org.apache.spark.graphx.impl

import java.io.{EOFException, InputStream, OutputStream}
import java.nio.ByteBuffer

import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.serializer._

private[graphx]
class VertexIdMsgSerializer(conf: SparkConf) extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance {

    override def serializeStream(s: OutputStream) = new ShuffleSerializationStream(s) {
      def writeObject[T](t: T) = {
        val msg = t.asInstanceOf[(VertexId, _)]
        writeVarLong(msg._1, optimizePositive = false)
        this
      }
    }

    override def deserializeStream(s: InputStream) = new ShuffleDeserializationStream(s) {
      override def readObject[T](): T = {
        (readVarLong(optimizePositive = false), null).asInstanceOf[T]
      }
    }
  }
}

/** A special shuffle serializer for VertexBroadcastMessage[Int]. */
private[graphx]
class IntVertexBroadcastMsgSerializer(conf: SparkConf) extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance {

    override def serializeStream(s: OutputStream) = new ShuffleSerializationStream(s) {
      def writeObject[T](t: T) = {
        val msg = t.asInstanceOf[VertexBroadcastMsg[Int]]
        writeVarLong(msg.vid, optimizePositive = false)
        writeInt(msg.data)
        this
      }
    }

    override def deserializeStream(s: InputStream) = new ShuffleDeserializationStream(s) {
      override def readObject[T](): T = {
        val a = readVarLong(optimizePositive = false)
        val b = readInt()
        new VertexBroadcastMsg[Int](0, a, b).asInstanceOf[T]
      }
    }
  }
}

/** A special shuffle serializer for VertexBroadcastMessage[Long]. */
private[graphx]
class LongVertexBroadcastMsgSerializer(conf: SparkConf) extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance {

    override def serializeStream(s: OutputStream) = new ShuffleSerializationStream(s) {
      def writeObject[T](t: T) = {
        val msg = t.asInstanceOf[VertexBroadcastMsg[Long]]
        writeVarLong(msg.vid, optimizePositive = false)
        writeLong(msg.data)
        this
      }
    }

    override def deserializeStream(s: InputStream) = new ShuffleDeserializationStream(s) {
      override def readObject[T](): T = {
        val a = readVarLong(optimizePositive = false)
        val b = readLong()
        new VertexBroadcastMsg[Long](0, a, b).asInstanceOf[T]
      }
    }
  }
}

/** A special shuffle serializer for VertexBroadcastMessage[Double]. */
private[graphx]
class DoubleVertexBroadcastMsgSerializer(conf: SparkConf) extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance {

    override def serializeStream(s: OutputStream) = new ShuffleSerializationStream(s) {
      def writeObject[T](t: T) = {
        val msg = t.asInstanceOf[VertexBroadcastMsg[Double]]
        writeVarLong(msg.vid, optimizePositive = false)
        writeDouble(msg.data)
        this
      }
    }

    override def deserializeStream(s: InputStream) = new ShuffleDeserializationStream(s) {
      def readObject[T](): T = {
        val a = readVarLong(optimizePositive = false)
        val b = readDouble()
        new VertexBroadcastMsg[Double](0, a, b).asInstanceOf[T]
      }
    }
  }
}

/** A special shuffle serializer for AggregationMessage[Int]. */
private[graphx]
class IntAggMsgSerializer(conf: SparkConf) extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance {

    override def serializeStream(s: OutputStream) = new ShuffleSerializationStream(s) {
      def writeObject[T](t: T) = {
        val msg = t.asInstanceOf[(VertexId, Int)]
        writeVarLong(msg._1, optimizePositive = false)
        writeUnsignedVarInt(msg._2)
        this
      }
    }

    override def deserializeStream(s: InputStream) = new ShuffleDeserializationStream(s) {
      override def readObject[T](): T = {
        val a = readVarLong(optimizePositive = false)
        val b = readUnsignedVarInt()
        (a, b).asInstanceOf[T]
      }
    }
  }
}

/** A special shuffle serializer for AggregationMessage[Long]. */
private[graphx]
class LongAggMsgSerializer(conf: SparkConf) extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance {

    override def serializeStream(s: OutputStream) = new ShuffleSerializationStream(s) {
      def writeObject[T](t: T) = {
        val msg = t.asInstanceOf[(VertexId, Long)]
        writeVarLong(msg._1, optimizePositive = false)
        writeVarLong(msg._2, optimizePositive = true)
        this
      }
    }

    override def deserializeStream(s: InputStream) = new ShuffleDeserializationStream(s) {
      override def readObject[T](): T = {
        val a = readVarLong(optimizePositive = false)
        val b = readVarLong(optimizePositive = true)
        (a, b).asInstanceOf[T]
      }
    }
  }
}

/** A special shuffle serializer for AggregationMessage[Double]. */
private[graphx]
class DoubleAggMsgSerializer(conf: SparkConf) extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance {

    override def serializeStream(s: OutputStream) = new ShuffleSerializationStream(s) {
      def writeObject[T](t: T) = {
        val msg = t.asInstanceOf[(VertexId, Double)]
        writeVarLong(msg._1, optimizePositive = false)
        writeDouble(msg._2)
        this
      }
    }

    override def deserializeStream(s: InputStream) = new ShuffleDeserializationStream(s) {
      def readObject[T](): T = {
        val a = readVarLong(optimizePositive = false)
        val b = readDouble()
        (a, b).asInstanceOf[T]
      }
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
// Helper classes to shorten the implementation of those special serializers.
////////////////////////////////////////////////////////////////////////////////

private[graphx]
abstract class ShuffleSerializationStream(s: OutputStream) extends SerializationStream {
  // The implementation should override this one.
  def writeObject[T](t: T): SerializationStream

  def writeInt(v: Int) {
    s.write(v >> 24)
    s.write(v >> 16)
    s.write(v >> 8)
    s.write(v)
  }

  def writeUnsignedVarInt(value: Int) {
    if ((value >>> 7) == 0) {
      s.write(value.toInt)
    } else if ((value >>> 14) == 0) {
      s.write((value & 0x7F) | 0x80)
      s.write(value >>> 7)
    } else if ((value >>> 21) == 0) {
      s.write((value & 0x7F) | 0x80)
      s.write(value >>> 7 | 0x80)
      s.write(value >>> 14)
    } else if ((value >>> 28) == 0) {
      s.write((value & 0x7F) | 0x80)
      s.write(value >>> 7 | 0x80)
      s.write(value >>> 14 | 0x80)
      s.write(value >>> 21)
    } else {
      s.write((value & 0x7F) | 0x80)
      s.write(value >>> 7 | 0x80)
      s.write(value >>> 14 | 0x80)
      s.write(value >>> 21 | 0x80)
      s.write(value >>> 28)
    }
  }

  def writeVarLong(value: Long, optimizePositive: Boolean) {
    val v = if (!optimizePositive) (value << 1) ^ (value >> 63) else value
    if ((v >>> 7) == 0) {
      s.write(v.toInt)
    } else if ((v >>> 14) == 0) {
      s.write(((v & 0x7F) | 0x80).toInt)
      s.write((v >>> 7).toInt)
    } else if ((v >>> 21) == 0) {
      s.write(((v & 0x7F) | 0x80).toInt)
      s.write((v >>> 7 | 0x80).toInt)
      s.write((v >>> 14).toInt)
    } else if ((v >>> 28) == 0) {
      s.write(((v & 0x7F) | 0x80).toInt)
      s.write((v >>> 7 | 0x80).toInt)
      s.write((v >>> 14 | 0x80).toInt)
      s.write((v >>> 21).toInt)
    } else if ((v >>> 35) == 0) {
      s.write(((v & 0x7F) | 0x80).toInt)
      s.write((v >>> 7 | 0x80).toInt)
      s.write((v >>> 14 | 0x80).toInt)
      s.write((v >>> 21 | 0x80).toInt)
      s.write((v >>> 28).toInt)
    } else if ((v >>> 42) == 0) {
      s.write(((v & 0x7F) | 0x80).toInt)
      s.write((v >>> 7 | 0x80).toInt)
      s.write((v >>> 14 | 0x80).toInt)
      s.write((v >>> 21 | 0x80).toInt)
      s.write((v >>> 28 | 0x80).toInt)
      s.write((v >>> 35).toInt)
    } else if ((v >>> 49) == 0) {
      s.write(((v & 0x7F) | 0x80).toInt)
      s.write((v >>> 7 | 0x80).toInt)
      s.write((v >>> 14 | 0x80).toInt)
      s.write((v >>> 21 | 0x80).toInt)
      s.write((v >>> 28 | 0x80).toInt)
      s.write((v >>> 35 | 0x80).toInt)
      s.write((v >>> 42).toInt)
    } else if ((v >>> 56) == 0) {
      s.write(((v & 0x7F) | 0x80).toInt)
      s.write((v >>> 7 | 0x80).toInt)
      s.write((v >>> 14 | 0x80).toInt)
      s.write((v >>> 21 | 0x80).toInt)
      s.write((v >>> 28 | 0x80).toInt)
      s.write((v >>> 35 | 0x80).toInt)
      s.write((v >>> 42 | 0x80).toInt)
      s.write((v >>> 49).toInt)
    } else {
      s.write(((v & 0x7F) | 0x80).toInt)
      s.write((v >>> 7 | 0x80).toInt)
      s.write((v >>> 14 | 0x80).toInt)
      s.write((v >>> 21 | 0x80).toInt)
      s.write((v >>> 28 | 0x80).toInt)
      s.write((v >>> 35 | 0x80).toInt)
      s.write((v >>> 42 | 0x80).toInt)
      s.write((v >>> 49 | 0x80).toInt)
      s.write((v >>> 56).toInt)
    }
  }

  def writeLong(v: Long) {
    s.write((v >>> 56).toInt)
    s.write((v >>> 48).toInt)
    s.write((v >>> 40).toInt)
    s.write((v >>> 32).toInt)
    s.write((v >>> 24).toInt)
    s.write((v >>> 16).toInt)
    s.write((v >>> 8).toInt)
    s.write(v.toInt)
  }

  //def writeDouble(v: Double): Unit = writeUnsignedVarLong(java.lang.Double.doubleToLongBits(v))
  def writeDouble(v: Double): Unit = writeLong(java.lang.Double.doubleToLongBits(v))

  override def flush(): Unit = s.flush()

  override def close(): Unit = s.close()
}

private[graphx]
abstract class ShuffleDeserializationStream(s: InputStream) extends DeserializationStream {
  // The implementation should override this one.
  def readObject[T](): T

  def readInt(): Int = {
    val first = s.read()
    if (first < 0) throw new EOFException
    (first & 0xFF) << 24 | (s.read() & 0xFF) << 16 | (s.read() & 0xFF) << 8 | (s.read() & 0xFF)
  }

  def readUnsignedVarInt(): Int = {
    var value: Int = 0
    var i: Int = 0
    def readOrThrow(): Int = {
      val in = s.read()
      if (in < 0) throw new EOFException
      in & 0xFF
    }
    var b: Int = readOrThrow()
    while ((b & 0x80) != 0) {
      value |= (b & 0x7F) << i
      i += 7
      if (i > 35) throw new IllegalArgumentException("Variable length quantity is too long")
      b = readOrThrow()
    }
    value | (b << i)
  }

  def readVarLong(optimizePositive: Boolean): Long = {
    def readOrThrow(): Int = {
      val in = s.read()
      if (in < 0) throw new EOFException
      in & 0xFF
    }
    var b = readOrThrow()
    var ret: Long = b & 0x7F
    if ((b & 0x80) != 0) {
      b = readOrThrow()
      ret |= (b & 0x7F) << 7
      if ((b & 0x80) != 0) {
        b = readOrThrow()
        ret |= (b & 0x7F) << 14
        if ((b & 0x80) != 0) {
          b = readOrThrow()
          ret |= (b & 0x7F) << 21
          if ((b & 0x80) != 0) {
            b = readOrThrow()
            ret |= (b & 0x7F).toLong << 28
            if ((b & 0x80) != 0) {
              b = readOrThrow()
              ret |= (b & 0x7F).toLong << 35
              if ((b & 0x80) != 0) {
                b = readOrThrow()
                ret |= (b & 0x7F).toLong << 42
                if ((b & 0x80) != 0) {
                  b = readOrThrow()
                  ret |= (b & 0x7F).toLong << 49
                  if ((b & 0x80) != 0) {
                    b = readOrThrow()
                    ret |= b.toLong << 56
                  }
                }
              }
            }
          }
        }
      }
    }
    if (!optimizePositive) (ret >>> 1) ^ -(ret & 1) else ret
  }

  def readLong(): Long = {
    val first = s.read()
    if (first < 0) throw new EOFException()
    (first.toLong << 56) |
      (s.read() & 0xFF).toLong << 48 |
      (s.read() & 0xFF).toLong << 40 |
      (s.read() & 0xFF).toLong << 32 |
      (s.read() & 0xFF).toLong << 24 |
      (s.read() & 0xFF) << 16 |
      (s.read() & 0xFF) << 8 |
      (s.read() & 0xFF)
  }

  //def readDouble(): Double = java.lang.Double.longBitsToDouble(readUnsignedVarLong())
  def readDouble(): Double = java.lang.Double.longBitsToDouble(readLong())

  override def close(): Unit = s.close()
}

private[graphx] sealed trait ShuffleSerializerInstance extends SerializerInstance {

  override def serialize[T](t: T): ByteBuffer = throw new UnsupportedOperationException

  override def deserialize[T](bytes: ByteBuffer): T = throw new UnsupportedOperationException

  override def deserialize[T](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException

  // The implementation should override the following two.
  override def serializeStream(s: OutputStream): SerializationStream
  override def deserializeStream(s: InputStream): DeserializationStream
}
