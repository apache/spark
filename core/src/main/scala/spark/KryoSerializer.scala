package spark

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.Channels

import scala.collection.immutable
import scala.collection.mutable

import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.{Serializer => KSerializer}
import com.esotericsoftware.kryo.serialize.ClassSerializer
import com.esotericsoftware.kryo.serialize.SerializableSerializer
import de.javakaffee.kryoserializers.KryoReflectionFactorySupport

import serializer.{SerializerInstance, DeserializationStream, SerializationStream}
import spark.broadcast._
import spark.storage._

/**
 * Zig-zag encoder used to write object sizes to serialization streams.
 * Based on Kryo's integer encoder.
 */
private[spark] object ZigZag {
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

private[spark] 
class KryoSerializationStream(kryo: Kryo, threadBuffer: ByteBuffer, out: OutputStream)
extends SerializationStream {
  val channel = Channels.newChannel(out)

  def writeObject[T](t: T): SerializationStream = {
    kryo.writeClassAndObject(threadBuffer, t)
    ZigZag.writeInt(threadBuffer.position(), out)
    threadBuffer.flip()
    channel.write(threadBuffer)
    threadBuffer.clear()
    this
  }

  def flush() { out.flush() }
  def close() { out.close() }
}

private[spark] 
class KryoDeserializationStream(objectBuffer: ObjectBuffer, in: InputStream)
extends DeserializationStream {
  def readObject[T](): T = {
    val len = ZigZag.readInt(in)
    objectBuffer.readClassAndObject(in, len).asInstanceOf[T]
  }

  def close() { in.close() }
}

private[spark] class KryoSerializerInstance(ks: KryoSerializer) extends SerializerInstance {
  val kryo = ks.kryo
  val threadBuffer = ks.threadBuffer.get()
  val objectBuffer = ks.objectBuffer.get()

  def serialize[T](t: T): ByteBuffer = {
    // Write it to our thread-local scratch buffer first to figure out the size, then return a new
    // ByteBuffer of the appropriate size
    threadBuffer.clear()
    kryo.writeClassAndObject(threadBuffer, t)
    val newBuf = ByteBuffer.allocate(threadBuffer.position)
    threadBuffer.flip()
    newBuf.put(threadBuffer)
    newBuf.flip()
    newBuf
  }

  def deserialize[T](bytes: ByteBuffer): T = {
    kryo.readClassAndObject(bytes).asInstanceOf[T]
  }

  def deserialize[T](bytes: ByteBuffer, loader: ClassLoader): T = {
    val oldClassLoader = kryo.getClassLoader
    kryo.setClassLoader(loader)
    val obj = kryo.readClassAndObject(bytes).asInstanceOf[T]
    kryo.setClassLoader(oldClassLoader)
    obj
  }

  def serializeStream(s: OutputStream): SerializationStream = {
    threadBuffer.clear()
    new KryoSerializationStream(kryo, threadBuffer, s)
  }

  def deserializeStream(s: InputStream): DeserializationStream = {
    new KryoDeserializationStream(objectBuffer, s)
  }

  override def serializeMany[T](iterator: Iterator[T]): ByteBuffer = {
    threadBuffer.clear()
    while (iterator.hasNext) {
      val element = iterator.next()
      // TODO: Do we also want to write the object's size? Doesn't seem necessary.
      kryo.writeClassAndObject(threadBuffer, element)
    }
    val newBuf = ByteBuffer.allocate(threadBuffer.position)
    threadBuffer.flip()
    newBuf.put(threadBuffer)
    newBuf.flip()
    newBuf
  }

  override def deserializeMany(buffer: ByteBuffer): Iterator[Any] = {
    buffer.rewind()
    new Iterator[Any] {
      override def hasNext: Boolean = buffer.remaining > 0
      override def next(): Any = kryo.readClassAndObject(buffer)
    }
  }
}

/**
 * Interface implemented by clients to register their classes with Kryo when using Kryo
 * serialization.
 */
trait KryoRegistrator {
  def registerClasses(kryo: Kryo): Unit
}

/**
 * A Spark serializer that uses the [[http://code.google.com/p/kryo/wiki/V1Documentation Kryo 1.x library]].
 */
class KryoSerializer extends spark.serializer.Serializer with Logging {
  // Make this lazy so that it only gets called once we receive our first task on each executor,
  // so we can pull out any custom Kryo registrator from the user's JARs.
  lazy val kryo = createKryo()

  val bufferSize = System.getProperty("spark.kryoserializer.buffer.mb", "32").toInt * 1024 * 1024 

  val objectBuffer = new ThreadLocal[ObjectBuffer] {
    override def initialValue = new ObjectBuffer(kryo, bufferSize)
  }

  val threadBuffer = new ThreadLocal[ByteBuffer] {
    override def initialValue = ByteBuffer.allocate(bufferSize)
  }

  def createKryo(): Kryo = {
    val kryo = new KryoReflectionFactorySupport()

    // Register some commonly used classes
    val toRegister: Seq[AnyRef] = Seq(
      // Arrays
      Array(1), Array(1.0), Array(1.0f), Array(1L), Array(""), Array(("", "")),
      Array(new java.lang.Object), Array(1.toByte), Array(true), Array('c'),
      // Specialized Tuple2s
      ("", ""), ("", 1), (1, 1), (1.0, 1.0), (1L, 1L),
      (1, 1.0), (1.0, 1), (1L, 1.0), (1.0, 1L), (1, 1L), (1L, 1),
      // Scala collections
      List(1), mutable.ArrayBuffer(1),
      // Options and Either
      Some(1), Left(1), Right(1),
      // Higher-dimensional tuples
      (1, 1, 1), (1, 1, 1, 1), (1, 1, 1, 1, 1),
      None,
      ByteBuffer.allocate(1),
      StorageLevel.MEMORY_ONLY,
      PutBlock("1", ByteBuffer.allocate(1), StorageLevel.MEMORY_ONLY),
      GotBlock("1", ByteBuffer.allocate(1)),
      GetBlock("1")
    )
    for (obj <- toRegister) {
      kryo.register(obj.getClass)
    }

    // Register the following classes for passing closures.
    kryo.register(classOf[Class[_]], new ClassSerializer(kryo))
    kryo.setRegistrationOptional(true)

    // Allow sending SerializableWritable
    kryo.register(classOf[SerializableWritable[_]], new SerializableSerializer())
    kryo.register(classOf[HttpBroadcast[_]], new SerializableSerializer())

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
