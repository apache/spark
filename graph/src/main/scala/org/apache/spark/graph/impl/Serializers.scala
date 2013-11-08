package org.apache.spark.graph.impl

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance, Serializer}


/** A special shuffle serializer for VertexBroadcastMessage[Int]. */
class IntVertexBroadcastMsgSerializer extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance {

    override def serializeStream(s: OutputStream) = new ShuffleSerializationStream(s) {
      def writeObject[T](t: T) = {
        val msg = t.asInstanceOf[VertexBroadcastMsg[Int]]
        writeLong(msg.vid)
        writeInt(msg.data)
        this
      }
    }

    override def deserializeStream(s: InputStream) = new ShuffleDeserializationStream(s) {
      override def readObject[T](): T = {
        new VertexBroadcastMsg[Int](0, readLong(), readInt()).asInstanceOf[T]
      }
    }
  }
}


/** A special shuffle serializer for VertexBroadcastMessage[Double]. */
class DoubleVertexBroadcastMsgSerializer extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance {

    override def serializeStream(s: OutputStream) = new ShuffleSerializationStream(s) {
      def writeObject[T](t: T) = {
        val msg = t.asInstanceOf[VertexBroadcastMsg[Double]]
        writeLong(msg.vid)
        writeDouble(msg.data)
        this
      }
    }

    override def deserializeStream(s: InputStream) = new ShuffleDeserializationStream(s) {
      def readObject[T](): T = {
        new VertexBroadcastMsg[Double](0, readLong(), readDouble()).asInstanceOf[T]
      }
    }
  }
}


/** A special shuffle serializer for AggregationMessage[Int]. */
class IntAggMsgSerializer extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance {

    override def serializeStream(s: OutputStream) = new ShuffleSerializationStream(s) {
      def writeObject[T](t: T) = {
        val msg = t.asInstanceOf[AggregationMsg[Int]]
        writeLong(msg.vid)
        writeInt(msg.data)
        this
      }
    }

    override def deserializeStream(s: InputStream) = new ShuffleDeserializationStream(s) {
      override def readObject[T](): T = {
        new AggregationMsg[Int](readLong(), readInt()).asInstanceOf[T]
      }
    }
  }
}


/** A special shuffle serializer for AggregationMessage[Double]. */
class DoubleAggMsgSerializer extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance {

    override def serializeStream(s: OutputStream) = new ShuffleSerializationStream(s) {
      def writeObject[T](t: T) = {
        val msg = t.asInstanceOf[AggregationMsg[Double]]
        writeLong(msg.vid)
        writeDouble(msg.data)
        this
      }
    }

    override def deserializeStream(s: InputStream) = new ShuffleDeserializationStream(s) {
      def readObject[T](): T = {
        new AggregationMsg[Double](readLong(), readDouble()).asInstanceOf[T]
      }
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
// Helper classes to shorten the implementation of those special serializers.
////////////////////////////////////////////////////////////////////////////////

sealed abstract class ShuffleSerializationStream(s: OutputStream) extends SerializationStream {
  // The implementation should override this one.
  def writeObject[T](t: T): SerializationStream

  def writeInt(v: Int) {
    s.write(v >> 24)
    s.write(v >> 16)
    s.write(v >> 8)
    s.write(v)
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

  def writeDouble(v: Double) {
    writeLong(java.lang.Double.doubleToLongBits(v))
  }

  override def flush(): Unit = s.flush()

  override def close(): Unit = s.close()
}


sealed abstract class ShuffleDeserializationStream(s: InputStream) extends DeserializationStream {
  // The implementation should override this one.
  def readObject[T](): T

  def readInt(): Int = {
    (s.read() & 0xFF) << 24 | (s.read() & 0xFF) << 16 | (s.read() & 0xFF) << 8 | (s.read() & 0xFF)
  }

  def readLong(): Long = {
    (s.read().toLong << 56) |
      (s.read() & 0xFF).toLong << 48 |
      (s.read() & 0xFF).toLong << 40 |
      (s.read() & 0xFF).toLong << 32 |
      (s.read() & 0xFF).toLong << 24 |
      (s.read() & 0xFF) << 16 |
      (s.read() & 0xFF) << 8 |
      (s.read() & 0xFF)
  }

  def readDouble(): Double = java.lang.Double.longBitsToDouble(readLong())

  override def close(): Unit = s.close()
}


sealed trait ShuffleSerializerInstance extends SerializerInstance {

  override def serialize[T](t: T): ByteBuffer = throw new UnsupportedOperationException

  override def deserialize[T](bytes: ByteBuffer): T = throw new UnsupportedOperationException

  override def deserialize[T](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException

  // The implementation should override the following two.
  override def serializeStream(s: OutputStream): SerializationStream
  override def deserializeStream(s: InputStream): DeserializationStream
}
