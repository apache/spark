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

package org.apache.spark.serializer

import java.io._
import java.nio.ByteBuffer
import javax.annotation.Nullable

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import com.esotericsoftware.kryo.{Kryo, KryoException, Serializer => KryoClassSerializer}
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer}
import com.twitter.chill.{AllScalaRegistrar, EmptyScalaKryoInstantiator}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.roaringbitmap.RoaringBitmap

import org.apache.spark._
import org.apache.spark.api.python.PythonBroadcast
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.scheduler.{CompressedMapStatus, HighlyCompressedMapStatus}
import org.apache.spark.storage._
import org.apache.spark.util.{BoundedPriorityQueue, SerializableConfiguration, SerializableJobConf, Utils}
import org.apache.spark.util.collection.CompactBuffer

/**
 * A Spark serializer that uses the [[https://code.google.com/p/kryo/ Kryo serialization library]].
 *
 * Note that this serializer is not guaranteed to be wire-compatible across different versions of
 * Spark. It is intended to be used to serialize/de-serialize data within a single
 * Spark application.
 */
class KryoSerializer(conf: SparkConf)
  extends org.apache.spark.serializer.Serializer
  with Logging
  with Serializable {

  private val bufferSizeKb = conf.getSizeAsKb("spark.kryoserializer.buffer", "64k")

  if (bufferSizeKb >= ByteUnit.GiB.toKiB(2)) {
    throw new IllegalArgumentException("spark.kryoserializer.buffer must be less than " +
      s"2048 mb, got: + ${ByteUnit.KiB.toMiB(bufferSizeKb)} mb.")
  }
  private val bufferSize = ByteUnit.KiB.toBytes(bufferSizeKb).toInt

  val maxBufferSizeMb = conf.getSizeAsMb("spark.kryoserializer.buffer.max", "64m").toInt
  if (maxBufferSizeMb >= ByteUnit.GiB.toMiB(2)) {
    throw new IllegalArgumentException("spark.kryoserializer.buffer.max must be less than " +
      s"2048 mb, got: + $maxBufferSizeMb mb.")
  }
  private val maxBufferSize = ByteUnit.MiB.toBytes(maxBufferSizeMb).toInt

  private val referenceTracking = conf.getBoolean("spark.kryo.referenceTracking", true)
  private val registrationRequired = conf.getBoolean("spark.kryo.registrationRequired", false)
  private val userRegistrators = conf.get("spark.kryo.registrator", "")
    .split(',').map(_.trim)
    .filter(!_.isEmpty)
  private val classesToRegister = conf.get("spark.kryo.classesToRegister", "")
    .split(',').map(_.trim)
    .filter(!_.isEmpty)

  private val avroSchemas = conf.getAvroSchema

  def newKryoOutput(): KryoOutput = new KryoOutput(bufferSize, math.max(bufferSize, maxBufferSize))

  def newKryo(): Kryo = {
    val instantiator = new EmptyScalaKryoInstantiator
    val kryo = instantiator.newKryo()
    kryo.setRegistrationRequired(registrationRequired)

    val oldClassLoader = Thread.currentThread.getContextClassLoader
    val classLoader = defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)

    // Allow disabling Kryo reference tracking if user knows their object graphs don't have loops.
    // Do this before we invoke the user registrator so the user registrator can override this.
    kryo.setReferences(referenceTracking)

    for (cls <- KryoSerializer.toRegister) {
      kryo.register(cls)
    }
    for ((cls, ser) <- KryoSerializer.toRegisterSerializer) {
      kryo.register(cls, ser)
    }

    // For results returned by asJavaIterable. See JavaIterableWrapperSerializer.
    kryo.register(JavaIterableWrapperSerializer.wrapperClass, new JavaIterableWrapperSerializer)

    // Allow sending classes with custom Java serializers
    kryo.register(classOf[SerializableWritable[_]], new KryoJavaSerializer())
    kryo.register(classOf[SerializableConfiguration], new KryoJavaSerializer())
    kryo.register(classOf[SerializableJobConf], new KryoJavaSerializer())
    kryo.register(classOf[PythonBroadcast], new KryoJavaSerializer())

    kryo.register(classOf[GenericRecord], new GenericAvroSerializer(avroSchemas))
    kryo.register(classOf[GenericData.Record], new GenericAvroSerializer(avroSchemas))

    try {
      // scalastyle:off classforname
      // Use the default classloader when calling the user registrator.
      Thread.currentThread.setContextClassLoader(classLoader)
      // Register classes given through spark.kryo.classesToRegister.
      classesToRegister
        .foreach { className => kryo.register(Class.forName(className, true, classLoader)) }
      // Allow the user to register their own classes by setting spark.kryo.registrator.
      userRegistrators
        .map(Class.forName(_, true, classLoader).newInstance().asInstanceOf[KryoRegistrator])
        .foreach { reg => reg.registerClasses(kryo) }
      // scalastyle:on classforname
    } catch {
      case e: Exception =>
        throw new SparkException(s"Failed to register classes with Kryo", e)
    } finally {
      Thread.currentThread.setContextClassLoader(oldClassLoader)
    }

    // Register Chill's classes; we do this after our ranges and the user's own classes to let
    // our code override the generic serializers in Chill for things like Seq
    new AllScalaRegistrar().apply(kryo)

    // Register types missed by Chill.
    // scalastyle:off
    kryo.register(classOf[Array[Tuple1[Any]]])
    kryo.register(classOf[Array[Tuple2[Any, Any]]])
    kryo.register(classOf[Array[Tuple3[Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple4[Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple5[Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple6[Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple7[Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple8[Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple9[Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple11[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple12[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple13[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple14[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple15[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple16[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple17[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple18[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple19[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple20[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple21[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple22[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])

    // scalastyle:on

    kryo.register(None.getClass)
    kryo.register(Nil.getClass)
    kryo.register(Utils.classForName("scala.collection.immutable.$colon$colon"))
    kryo.register(classOf[ArrayBuffer[Any]])

    kryo.setClassLoader(classLoader)
    kryo
  }

  override def newInstance(): SerializerInstance = {
    new KryoSerializerInstance(this)
  }

  private[spark] override lazy val supportsRelocationOfSerializedObjects: Boolean = {
    // If auto-reset is disabled, then Kryo may store references to duplicate occurrences of objects
    // in the stream rather than writing those objects' serialized bytes, breaking relocation. See
    // https://groups.google.com/d/msg/kryo-users/6ZUSyfjjtdo/FhGG1KHDXPgJ for more details.
    newInstance().asInstanceOf[KryoSerializerInstance].getAutoReset()
  }
}

private[spark]
class KryoSerializationStream(
    serInstance: KryoSerializerInstance,
    outStream: OutputStream) extends SerializationStream {

  private[this] var output: KryoOutput = new KryoOutput(outStream)
  private[this] var kryo: Kryo = serInstance.borrowKryo()

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    kryo.writeClassAndObject(output, t)
    this
  }

  override def flush() {
    if (output == null) {
      throw new IOException("Stream is closed")
    }
    output.flush()
  }

  override def close() {
    if (output != null) {
      try {
        output.close()
      } finally {
        serInstance.releaseKryo(kryo)
        kryo = null
        output = null
      }
    }
  }
}

private[spark]
class KryoDeserializationStream(
    serInstance: KryoSerializerInstance,
    inStream: InputStream) extends DeserializationStream {

  private[this] var input: KryoInput = new KryoInput(inStream)
  private[this] var kryo: Kryo = serInstance.borrowKryo()

  override def readObject[T: ClassTag](): T = {
    try {
      kryo.readClassAndObject(input).asInstanceOf[T]
    } catch {
      // DeserializationStream uses the EOF exception to indicate stopping condition.
      case e: KryoException if e.getMessage.toLowerCase.contains("buffer underflow") =>
        throw new EOFException
    }
  }

  override def close() {
    if (input != null) {
      try {
        // Kryo's Input automatically closes the input stream it is using.
        input.close()
      } finally {
        serInstance.releaseKryo(kryo)
        kryo = null
        input = null
      }
    }
  }
}

private[spark] class KryoSerializerInstance(ks: KryoSerializer) extends SerializerInstance {

  /**
   * A re-used [[Kryo]] instance. Methods will borrow this instance by calling `borrowKryo()`, do
   * their work, then release the instance by calling `releaseKryo()`. Logically, this is a caching
   * pool of size one. SerializerInstances are not thread-safe, hence accesses to this field are
   * not synchronized.
   */
  @Nullable private[this] var cachedKryo: Kryo = borrowKryo()

  /**
   * Borrows a [[Kryo]] instance. If possible, this tries to re-use a cached Kryo instance;
   * otherwise, it allocates a new instance.
   */
  private[serializer] def borrowKryo(): Kryo = {
    if (cachedKryo != null) {
      val kryo = cachedKryo
      // As a defensive measure, call reset() to clear any Kryo state that might have been modified
      // by the last operation to borrow this instance (see SPARK-7766 for discussion of this issue)
      kryo.reset()
      cachedKryo = null
      kryo
    } else {
      ks.newKryo()
    }
  }

  /**
   * Release a borrowed [[Kryo]] instance. If this serializer instance already has a cached Kryo
   * instance, then the given Kryo instance is discarded; otherwise, the Kryo is stored for later
   * re-use.
   */
  private[serializer] def releaseKryo(kryo: Kryo): Unit = {
    if (cachedKryo == null) {
      cachedKryo = kryo
    }
  }

  // Make these lazy vals to avoid creating a buffer unless we use them.
  private lazy val output = ks.newKryoOutput()
  private lazy val input = new KryoInput()

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    output.clear()
    val kryo = borrowKryo()
    try {
      kryo.writeClassAndObject(output, t)
    } catch {
      case e: KryoException if e.getMessage.startsWith("Buffer overflow") =>
        throw new SparkException(s"Kryo serialization failed: ${e.getMessage}. To avoid this, " +
          "increase spark.kryoserializer.buffer.max value.")
    } finally {
      releaseKryo(kryo)
    }
    ByteBuffer.wrap(output.toBytes)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val kryo = borrowKryo()
    try {
      input.setBuffer(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.remaining())
      kryo.readClassAndObject(input).asInstanceOf[T]
    } finally {
      releaseKryo(kryo)
    }
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val kryo = borrowKryo()
    val oldClassLoader = kryo.getClassLoader
    try {
      kryo.setClassLoader(loader)
      input.setBuffer(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.remaining())
      kryo.readClassAndObject(input).asInstanceOf[T]
    } finally {
      kryo.setClassLoader(oldClassLoader)
      releaseKryo(kryo)
    }
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new KryoSerializationStream(this, s)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new KryoDeserializationStream(this, s)
  }

  /**
   * Returns true if auto-reset is on. The only reason this would be false is if the user-supplied
   * registrator explicitly turns auto-reset off.
   */
  def getAutoReset(): Boolean = {
    val field = classOf[Kryo].getDeclaredField("autoReset")
    field.setAccessible(true)
    val kryo = borrowKryo()
    try {
      field.get(kryo).asInstanceOf[Boolean]
    } finally {
      releaseKryo(kryo)
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

private[serializer] object KryoSerializer {
  // Commonly used classes.
  private val toRegister: Seq[Class[_]] = Seq(
    ByteBuffer.allocate(1).getClass,
    classOf[StorageLevel],
    classOf[CompressedMapStatus],
    classOf[HighlyCompressedMapStatus],
    classOf[CompactBuffer[_]],
    classOf[BlockManagerId],
    classOf[Array[Byte]],
    classOf[Array[Short]],
    classOf[Array[Long]],
    classOf[BoundedPriorityQueue[_]],
    classOf[SparkConf]
  )

  private val toRegisterSerializer = Map[Class[_], KryoClassSerializer[_]](
    classOf[RoaringBitmap] -> new KryoClassSerializer[RoaringBitmap]() {
      override def write(kryo: Kryo, output: KryoOutput, bitmap: RoaringBitmap): Unit = {
        bitmap.serialize(new KryoOutputObjectOutputBridge(kryo, output))
      }
      override def read(kryo: Kryo, input: KryoInput, cls: Class[RoaringBitmap]): RoaringBitmap = {
        val ret = new RoaringBitmap
        ret.deserialize(new KryoInputObjectInputBridge(kryo, input))
        ret
      }
    }
  )
}

/**
 * This is a bridge class to wrap KryoInput as an InputStream and ObjectInput. It forwards all
 * methods of InputStream and ObjectInput to KryoInput. It's usually helpful when an API expects
 * an InputStream or ObjectInput but you want to use Kryo.
 */
private[spark] class KryoInputObjectInputBridge(
    kryo: Kryo, input: KryoInput) extends FilterInputStream(input) with ObjectInput {
  override def readLong(): Long = input.readLong()
  override def readChar(): Char = input.readChar()
  override def readFloat(): Float = input.readFloat()
  override def readByte(): Byte = input.readByte()
  override def readShort(): Short = input.readShort()
  override def readUTF(): String = input.readString() // readString in kryo does utf8
  override def readInt(): Int = input.readInt()
  override def readUnsignedShort(): Int = input.readShortUnsigned()
  override def skipBytes(n: Int): Int = {
    input.skip(n)
    n
  }
  override def readFully(b: Array[Byte]): Unit = input.read(b)
  override def readFully(b: Array[Byte], off: Int, len: Int): Unit = input.read(b, off, len)
  override def readLine(): String = throw new UnsupportedOperationException("readLine")
  override def readBoolean(): Boolean = input.readBoolean()
  override def readUnsignedByte(): Int = input.readByteUnsigned()
  override def readDouble(): Double = input.readDouble()
  override def readObject(): AnyRef = kryo.readClassAndObject(input)
}

/**
 * This is a bridge class to wrap KryoOutput as an OutputStream and ObjectOutput. It forwards all
 * methods of OutputStream and ObjectOutput to KryoOutput. It's usually helpful when an API expects
 * an OutputStream or ObjectOutput but you want to use Kryo.
 */
private[spark] class KryoOutputObjectOutputBridge(
    kryo: Kryo, output: KryoOutput) extends FilterOutputStream(output) with ObjectOutput  {
  override def writeFloat(v: Float): Unit = output.writeFloat(v)
  // There is no "readChars" counterpart, except maybe "readLine", which is not supported
  override def writeChars(s: String): Unit = throw new UnsupportedOperationException("writeChars")
  override def writeDouble(v: Double): Unit = output.writeDouble(v)
  override def writeUTF(s: String): Unit = output.writeString(s) // writeString in kryo does UTF8
  override def writeShort(v: Int): Unit = output.writeShort(v)
  override def writeInt(v: Int): Unit = output.writeInt(v)
  override def writeBoolean(v: Boolean): Unit = output.writeBoolean(v)
  override def write(b: Int): Unit = output.write(b)
  override def write(b: Array[Byte]): Unit = output.write(b)
  override def write(b: Array[Byte], off: Int, len: Int): Unit = output.write(b, off, len)
  override def writeBytes(s: String): Unit = output.writeString(s)
  override def writeChar(v: Int): Unit = output.writeChar(v.toChar)
  override def writeLong(v: Long): Unit = output.writeLong(v)
  override def writeByte(v: Int): Unit = output.writeByte(v)
  override def writeObject(obj: AnyRef): Unit = kryo.writeClassAndObject(output, obj)
}

/**
 * A Kryo serializer for serializing results returned by asJavaIterable.
 *
 * The underlying object is scala.collection.convert.Wrappers$IterableWrapper.
 * Kryo deserializes this into an AbstractCollection, which unfortunately doesn't work.
 */
private class JavaIterableWrapperSerializer
  extends com.esotericsoftware.kryo.Serializer[java.lang.Iterable[_]] {

  import JavaIterableWrapperSerializer._

  override def write(kryo: Kryo, out: KryoOutput, obj: java.lang.Iterable[_]): Unit = {
    // If the object is the wrapper, simply serialize the underlying Scala Iterable object.
    // Otherwise, serialize the object itself.
    if (obj.getClass == wrapperClass && underlyingMethodOpt.isDefined) {
      kryo.writeClassAndObject(out, underlyingMethodOpt.get.invoke(obj))
    } else {
      kryo.writeClassAndObject(out, obj)
    }
  }

  override def read(kryo: Kryo, in: KryoInput, clz: Class[java.lang.Iterable[_]])
    : java.lang.Iterable[_] = {
    kryo.readClassAndObject(in) match {
      case scalaIterable: Iterable[_] => scalaIterable.asJava
      case javaIterable: java.lang.Iterable[_] => javaIterable
    }
  }
}

private object JavaIterableWrapperSerializer extends Logging {
  // The class returned by JavaConverters.asJava
  // (scala.collection.convert.Wrappers$IterableWrapper).
  val wrapperClass =
    scala.collection.convert.WrapAsJava.asJavaIterable(Seq(1)).getClass

  // Get the underlying method so we can use it to get the Scala collection for serialization.
  private val underlyingMethodOpt = {
    try Some(wrapperClass.getDeclaredMethod("underlying")) catch {
      case e: Exception =>
        logError("Failed to find the underlying field in " + wrapperClass, e)
        None
    }
  }
}
