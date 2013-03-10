package spark

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.Channels

import scala.collection.immutable
import scala.collection.mutable

import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.{Serializer => KSerializer}
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer}
import de.javakaffee.kryoserializers.KryoReflectionFactorySupport

import serializer.{SerializerInstance, DeserializationStream, SerializationStream}
import spark.broadcast._
import spark.storage._

private[spark]
class KryoSerializationStream(kryo: Kryo, outStream: OutputStream) extends SerializationStream {

  val output = new KryoOutput(outStream)

  def writeObject[T](t: T): SerializationStream = {
    kryo.writeClassAndObject(output, t)
    this
  }

  def flush() { output.flush() }
  def close() { output.close() }
}

private[spark]
class KryoDeserializationStream(kryo: Kryo, inStream: InputStream) extends DeserializationStream {

  val input = new KryoInput(inStream)

  def readObject[T](): T = {
    try {
      kryo.readClassAndObject(input).asInstanceOf[T]
    } catch {
      // DeserializationStream uses the EOF exception to indicate stopping condition.
      case e: com.esotericsoftware.kryo.KryoException => throw new java.io.EOFException
    }
  }

  def close() {
    // Kryo's Input automatically closes the input stream it is using.
    input.close()
  }
}

private[spark] class KryoSerializerInstance(ks: KryoSerializer) extends SerializerInstance {

  val kryo = ks.kryo.get()
  val output = ks.output.get()
  val input = ks.input.get()

  def serialize[T](t: T): ByteBuffer = {
    output.clear()
    kryo.writeClassAndObject(output, t)
    ByteBuffer.wrap(output.toBytes)
  }

  def deserialize[T](bytes: ByteBuffer): T = {
    input.setBuffer(bytes.array)
    kryo.readClassAndObject(input).asInstanceOf[T]
  }

  def deserialize[T](bytes: ByteBuffer, loader: ClassLoader): T = {
    val oldClassLoader = kryo.getClassLoader
    kryo.setClassLoader(loader)
    input.setBuffer(bytes.array)
    val obj = kryo.readClassAndObject(input).asInstanceOf[T]
    kryo.setClassLoader(oldClassLoader)
    obj
  }

  def serializeStream(s: OutputStream): SerializationStream = {
    new KryoSerializationStream(kryo, s)
  }

  def deserializeStream(s: InputStream): DeserializationStream = {
    new KryoDeserializationStream(kryo, s)
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

  val bufferSize = System.getProperty("spark.kryoserializer.buffer.mb", "2").toInt * 1024 * 1024

  val kryo = new ThreadLocal[Kryo] {
    override def initialValue = createKryo()
  }

  val output = new ThreadLocal[KryoOutput] {
    override def initialValue = new KryoOutput(bufferSize)
  }

  val input = new ThreadLocal[KryoInput] {
    override def initialValue = new KryoInput(bufferSize)
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

    // Allow sending SerializableWritable
    kryo.register(classOf[SerializableWritable[_]], new KryoJavaSerializer())
    kryo.register(classOf[HttpBroadcast[_]], new KryoJavaSerializer())

    // Register some commonly used Scala singleton objects. Because these
    // are singletons, we must return the exact same local object when we
    // deserialize rather than returning a clone as FieldSerializer would.
    class SingletonSerializer[T](obj: T) extends KSerializer[T] {
      override def write(kryo: Kryo, output: KryoOutput, obj: T) {}
      override def read(kryo: Kryo, input: KryoInput, cls: java.lang.Class[T]): T = obj
    }
    kryo.register(None.getClass, new SingletonSerializer[AnyRef](None))
    kryo.register(Nil.getClass, new SingletonSerializer[AnyRef](Nil))

    // Register maps with a special serializer since they have complex internal structure
    class ScalaMapSerializer(buildMap: Array[(Any, Any)] => scala.collection.Map[Any, Any])
      extends KSerializer[Array[(Any, Any)] => scala.collection.Map[Any, Any]] {

      //hack, look at https://groups.google.com/forum/#!msg/kryo-users/Eu5V4bxCfws/k-8UQ22y59AJ
      private final val FAKE_REFERENCE = new Object()
      override def write(
                          kryo: Kryo,
                          output: KryoOutput,
                          obj: Array[(Any, Any)] => scala.collection.Map[Any, Any]) {
        val map = obj.asInstanceOf[scala.collection.Map[Any, Any]]
        output.writeInt(map.size)
        for ((k, v) <- map) {
          kryo.writeClassAndObject(output, k)
          kryo.writeClassAndObject(output, v)
        }
      }
      override def read (
                          kryo: Kryo,
                          input: KryoInput,
                          cls: Class[Array[(Any, Any)] => scala.collection.Map[Any, Any]])
      : Array[(Any, Any)] => scala.collection.Map[Any, Any] = {
        kryo.reference(FAKE_REFERENCE)
        val size = input.readInt()
        val elems = new Array[(Any, Any)](size)
        for (i <- 0 until size) {
          val k = kryo.readClassAndObject(input)
          val v = kryo.readClassAndObject(input)
          elems(i)=(k,v)
        }
        buildMap(elems).asInstanceOf[Array[(Any, Any)] => scala.collection.Map[Any, Any]]
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

  def newInstance(): SerializerInstance = {
    this.kryo.get().setClassLoader(Thread.currentThread().getContextClassLoader)
    new KryoSerializerInstance(this)
  }
}
