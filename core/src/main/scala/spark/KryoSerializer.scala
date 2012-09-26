package spark

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.Channels

import scala.collection.immutable
import scala.collection.mutable

import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.{Serializer => KSerializer}
import com.esotericsoftware.kryo.serialize.ClassSerializer
import de.javakaffee.kryoserializers.KryoReflectionFactorySupport

/**
 * Zig-zag encoder used to write object sizes to serialization streams.
 * Based on Kryo's integer encoder.
 */
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

class KryoSerializationStream(kryo: Kryo, buf: ByteBuffer, out: OutputStream)
extends SerializationStream {
  val channel = Channels.newChannel(out)

  def writeObject[T](t: T) {
    kryo.writeClassAndObject(buf, t)
    ZigZag.writeInt(buf.position(), out)
    buf.flip()
    channel.write(buf)
    buf.clear()
  }

  def flush() { out.flush() }
  def close() { out.close() }
}

class KryoDeserializationStream(buf: ObjectBuffer, in: InputStream)
extends DeserializationStream {
  def readObject[T](): T = {
    val len = ZigZag.readInt(in)
    buf.readClassAndObject(in, len).asInstanceOf[T]
  }

  def close() { in.close() }
}

class KryoSerializerInstance(ks: KryoSerializer) extends SerializerInstance {
  val buf = ks.threadBuf.get()

  def serialize[T](t: T): Array[Byte] = {
    buf.writeClassAndObject(t)
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    buf.readClassAndObject(bytes).asInstanceOf[T]
  }

  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val oldClassLoader = ks.kryo.getClassLoader
    ks.kryo.setClassLoader(loader)
    val obj = buf.readClassAndObject(bytes).asInstanceOf[T]
    ks.kryo.setClassLoader(oldClassLoader)
    obj
  }

  def outputStream(s: OutputStream): SerializationStream = {
    new KryoSerializationStream(ks.kryo, ks.threadByteBuf.get(), s)
  }

  def inputStream(s: InputStream): DeserializationStream = {
    new KryoDeserializationStream(buf, s)
  }
}

// Used by clients to register their own classes
trait KryoRegistrator {
  def registerClasses(kryo: Kryo): Unit
}

class KryoSerializer extends Serializer with Logging {
  val kryo = createKryo()

  val bufferSize = 
    System.getProperty("spark.kryoserializer.buffer.mb", "2").toInt * 1024 * 1024 

  val threadBuf = new ThreadLocal[ObjectBuffer] {
    override def initialValue = new ObjectBuffer(kryo, bufferSize)
  }

  val threadByteBuf = new ThreadLocal[ByteBuffer] {
    override def initialValue = ByteBuffer.allocate(bufferSize)
  }

  def createKryo(): Kryo = {
    // This is used so we can serialize/deserialize objects without a zero-arg
    // constructor.
    val kryo = new KryoReflectionFactorySupport()

    // Register some commonly used classes
    val toRegister: Seq[AnyRef] = Seq(
      // Arrays
      Array(1), Array(1.0), Array(1.0f), Array(1L), Array(""), Array(("", "")),
      Array(new java.lang.Object), Array(1.toByte), Array(true), Array('c'),
      // Specialized Tuple2s
      ("", ""), (1, 1), (1.0, 1.0), (1L, 1L),
      (1, 1.0), (1.0, 1), (1L, 1.0), (1.0, 1L), (1, 1L), (1L, 1),
      // Scala collections
      List(1), mutable.ArrayBuffer(1),
      // Options and Either
      Some(1), Left(1), Right(1),
      // Higher-dimensional tuples
      (1, 1, 1), (1, 1, 1, 1), (1, 1, 1, 1, 1)
    )
    for (obj <- toRegister) {
      kryo.register(obj.getClass)
    }

    // Register the following classes for passing closures.
    kryo.register(classOf[Class[_]], new ClassSerializer(kryo))
    kryo.setRegistrationOptional(true)

    // Register some commonly used Scala singleton objects. Because these
    // are singletons, we must return the exact same local object when we
    // deserialize rather than returning a clone as FieldSerializer would.
    class SingletonSerializer(obj: AnyRef) extends KSerializer {
      override def writeObjectData(buf: ByteBuffer, obj: AnyRef) {}
      override def readObjectData[T](buf: ByteBuffer, cls: Class[T]): T = obj.asInstanceOf[T]
    }
    kryo.register(None.getClass, new SingletonSerializer(None))
    kryo.register(Nil.getClass, new SingletonSerializer(Nil))

    // Register maps with a special serializer since they have complex internal structure
    class ScalaMapSerializer(buildMap: Array[(Any, Any)] => scala.collection.Map[Any, Any])
    extends KSerializer {
      override def writeObjectData(buf: ByteBuffer, obj: AnyRef) {
        val map = obj.asInstanceOf[scala.collection.Map[Any, Any]]
        kryo.writeObject(buf, map.size.asInstanceOf[java.lang.Integer])
        for ((k, v) <- map) {
          kryo.writeClassAndObject(buf, k)
          kryo.writeClassAndObject(buf, v)
        }
      }
      override def readObjectData[T](buf: ByteBuffer, cls: Class[T]): T = {
        val size = kryo.readObject(buf, classOf[java.lang.Integer]).intValue
        val elems = new Array[(Any, Any)](size)
        for (i <- 0 until size)
          elems(i) = (kryo.readClassAndObject(buf), kryo.readClassAndObject(buf))
        buildMap(elems).asInstanceOf[T]
      }
    }
    kryo.register(mutable.HashMap().getClass, new ScalaMapSerializer(mutable.HashMap() ++ _))
    // TODO: add support for immutable maps too; this is more annoying because there are many
    // subclasses of immutable.Map for small maps (with <= 4 entries)
    val map1  = Map[Any, Any](1 -> 1)
    val map2  = Map[Any, Any](1 -> 1, 2 -> 2)
    val map3  = Map[Any, Any](1 -> 1, 2 -> 2, 3 -> 3)
    val map4  = Map[Any, Any](1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4)
    val map5  = Map[Any, Any](1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4, 5 -> 5)
    kryo.register(map1.getClass, new ScalaMapSerializer(mutable.HashMap() ++ _ toMap))
    kryo.register(map2.getClass, new ScalaMapSerializer(mutable.HashMap() ++ _ toMap))
    kryo.register(map3.getClass, new ScalaMapSerializer(mutable.HashMap() ++ _ toMap))
    kryo.register(map4.getClass, new ScalaMapSerializer(mutable.HashMap() ++ _ toMap))
    kryo.register(map5.getClass, new ScalaMapSerializer(mutable.HashMap() ++ _ toMap))

    // Allow the user to register their own classes by setting spark.kryo.registrator
    val regCls = System.getProperty("spark.kryo.registrator")
    if (regCls != null) {
      logInfo("Running user registrator: " + regCls)
      val classLoader = Thread.currentThread.getContextClassLoader
      val reg = Class.forName(regCls, true, classLoader).newInstance().asInstanceOf[KryoRegistrator]
      reg.registerClasses(kryo)
    }
    kryo
  }

  def newInstance(): SerializerInstance = new KryoSerializerInstance(this)
}
