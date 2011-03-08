package spark

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.Channels

import scala.collection.immutable
import scala.collection.mutable

import com.esotericsoftware.kryo._

object ZigZag {
  def writeInt(n: Int, out: OutputStream) {
    var value = n
    if ((value & ~0x7F) == 0) {
      out.write(value)
      return
    }
    out.write(((value & 0x7F) | 0x80))
    value >>>= 7
    if ((value & ~0x7F) == 0) {
      out.write(value)
      return
    }
    out.write(((value & 0x7F) | 0x80))
    value >>>= 7
    if ((value & ~0x7F) == 0) {
      out.write(value)
      return
    }
    out.write(((value & 0x7F) | 0x80))
    value >>>= 7
    if ((value & ~0x7F) == 0) {
      out.write(value)
      return
    }
    out.write(((value & 0x7F) | 0x80))
    value >>>= 7
    out.write(value)
  }

  def readInt(in: InputStream): Int = {
    var offset = 0
    var result = 0
    while (offset < 32) {
      val b = in.read()
      if (b == -1) {
        throw new EOFException("End of stream")
      }
      result |= ((b & 0x7F) << offset)
      if ((b & 0x80) == 0) {
        return result
      }
      offset += 7
    }
    throw new SparkException("Malformed zigzag-encoded integer")
  }
}

class KryoSerializationStream(kryo: Kryo, out: OutputStream)
extends SerializationStream {
  val buf = ByteBuffer.allocateDirect(1024*1024)

  def writeObject[T](t: T) {
    kryo.writeClassAndObject(buf, t)
    ZigZag.writeInt(buf.position(), out)
    buf.flip()
    Channels.newChannel(out).write(buf)
    buf.clear()
  }

  def flush() { out.flush() }
  def close() { out.close() }
}

class KryoDeserializationStream(kryo: Kryo, in: InputStream)
extends DeserializationStream {
  val buf = new ObjectBuffer(kryo, 1024*1024)

  def readObject[T](): T = {
    val len = ZigZag.readInt(in)
    buf.readClassAndObject(in, len).asInstanceOf[T]
  }

  def close() { in.close() }
}

class KryoSerializer(kryo: Kryo) extends Serializer {
  val buf = new ObjectBuffer(kryo, 1024*1024)

  def serialize[T](t: T): Array[Byte] = {
    buf.writeClassAndObject(t)
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    buf.readClassAndObject(bytes).asInstanceOf[T]
  }

  def outputStream(s: OutputStream): SerializationStream = {
    new KryoSerializationStream(kryo, s)
  }

  def inputStream(s: InputStream): DeserializationStream = {
    new KryoDeserializationStream(kryo, s)
  }
}

// Used by clients to register their own classes
trait KryoRegistrator {
  def registerClasses(kryo: Kryo): Unit
}

class KryoSerialization extends SerializationStrategy with Logging {
  val kryo = createKryo()

  def createKryo(): Kryo = {
    val kryo = new Kryo()
    val toRegister: Seq[AnyRef] = Seq(
      // Arrays
      Array(1), Array(1.0), Array(1.0f), Array(1L), Array(""), Array(("", "")),
      // Specialized Tuple2s
      ("", ""), (1, 1), (1.0, 1.0), (1L, 1L),
      (1, 1.0), (1.0, 1), (1L, 1.0), (1.0, 1L), (1, 1L), (1L, 1),
      // Scala collections
      Nil, List(1), immutable.Map(1 -> 1), immutable.HashMap(1 -> 1),
      mutable.Map(1 -> 1), mutable.HashMap(1 -> 1), mutable.ArrayBuffer(1),
      // Options and Either
      Some(1), None, Left(1), Right(1),
      // Higher-dimensional tuples
      (1, 1, 1), (1, 1, 1, 1), (1, 1, 1, 1, 1)
    )
    for (obj <- toRegister) {
      kryo.register(obj.getClass)
    }
    val regCls = System.getProperty("spark.kryo.registrator")
    if (regCls != null) {
      logInfo("Running user registrator: " + regCls)
      val reg = Class.forName(regCls).newInstance().asInstanceOf[KryoRegistrator]
      reg.registerClasses(kryo)
    }
    kryo
  }

  def newSerializer(): Serializer = new KryoSerializer(kryo)
}
