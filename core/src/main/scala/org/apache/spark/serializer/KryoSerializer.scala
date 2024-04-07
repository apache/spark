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
import java.lang.invoke.SerializedLambda
import java.nio.ByteBuffer
import java.util.Locale
import javax.annotation.Nullable

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.esotericsoftware.kryo.{Kryo, KryoException, Serializer => KryoClassSerializer}
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.io.{UnsafeInput => KryoUnsafeInput, UnsafeOutput => KryoUnsafeOutput}
import com.esotericsoftware.kryo.pool.{KryoCallback, KryoFactory, KryoPool}
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer}
import com.twitter.chill.{AllScalaRegistrar, EmptyScalaKryoInstantiator}
import org.apache.avro.generic.{GenericContainer, GenericData, GenericRecord}
import org.roaringbitmap.RoaringBitmap

import org.apache.spark._
import org.apache.spark.api.python.PythonBroadcast
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.CLASS_NAME
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.internal.io.FileCommitProtocol._
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.scheduler.{CompressedMapStatus, HighlyCompressedMapStatus}
import org.apache.spark.storage._
import org.apache.spark.util.{BoundedPriorityQueue, ByteBufferInputStream, NextIterator, SerializableConfiguration, SerializableJobConf, Utils}
import org.apache.spark.util.collection.{BitSet, CompactBuffer}
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * A Spark serializer that uses the <a href="https://code.google.com/p/kryo/">
 * Kryo serialization library</a>.
 *
 * @note This serializer is not guaranteed to be wire-compatible across different versions of
 * Spark. It is intended to be used to serialize/de-serialize data within a single
 * Spark application.
 */
class KryoSerializer(conf: SparkConf)
  extends org.apache.spark.serializer.Serializer
  with Logging
  with Serializable {

  private val bufferSizeKb = conf.get(KRYO_SERIALIZER_BUFFER_SIZE)

  if (bufferSizeKb >= ByteUnit.GiB.toKiB(2)) {
    throw new SparkIllegalArgumentException(
      errorClass = "INVALID_KRYO_SERIALIZER_BUFFER_SIZE",
      messageParameters = Map(
        "bufferSizeConfKey" -> KRYO_SERIALIZER_BUFFER_SIZE.key,
        "bufferSizeConfValue" -> ByteUnit.KiB.toMiB(bufferSizeKb).toString))
  }
  private val bufferSize = ByteUnit.KiB.toBytes(bufferSizeKb).toInt

  val maxBufferSizeMb = conf.get(KRYO_SERIALIZER_MAX_BUFFER_SIZE).toInt
  if (maxBufferSizeMb >= ByteUnit.GiB.toMiB(2)) {
    throw new SparkIllegalArgumentException(
      errorClass = "INVALID_KRYO_SERIALIZER_BUFFER_SIZE",
      messageParameters = Map(
        "bufferSizeConfKey" -> KRYO_SERIALIZER_MAX_BUFFER_SIZE.key,
        "bufferSizeConfValue" -> maxBufferSizeMb.toString))
  }
  private val maxBufferSize = ByteUnit.MiB.toBytes(maxBufferSizeMb).toInt

  private val referenceTracking = conf.get(KRYO_REFERENCE_TRACKING)
  private val registrationRequired = conf.get(KRYO_REGISTRATION_REQUIRED)
  private val userRegistrators = conf.get(KRYO_USER_REGISTRATORS)
    .map(_.trim)
    .filter(!_.isEmpty)
  private val classesToRegister = conf.get(KRYO_CLASSES_TO_REGISTER)
    .map(_.trim)
    .filter(!_.isEmpty)

  private val avroSchemas = conf.getAvroSchema
  // whether to use unsafe based IO for serialization
  private val useUnsafe = conf.get(KRYO_USE_UNSAFE)
  private val usePool = conf.get(KRYO_USE_POOL)

  def newKryoOutput(): KryoOutput =
    if (useUnsafe) {
      new KryoUnsafeOutput(bufferSize, math.max(bufferSize, maxBufferSize))
    } else {
      new KryoOutput(bufferSize, math.max(bufferSize, maxBufferSize))
    }

  @transient
  private lazy val factory: KryoFactory = new KryoFactory() {
    override def create: Kryo = {
      newKryo()
    }
  }

  private class PoolWrapper extends KryoPool {
    private var pool: KryoPool = getPool

    override def borrow(): Kryo = pool.borrow()

    override def release(kryo: Kryo): Unit = pool.release(kryo)

    override def run[T](kryoCallback: KryoCallback[T]): T = pool.run(kryoCallback)

    def reset(): Unit = {
      pool = getPool
    }

    private def getPool: KryoPool = {
      new KryoPool.Builder(factory).softReferences.build
    }
  }

  @transient
  private lazy val internalPool = new PoolWrapper

  def pool: KryoPool = internalPool

  def newKryo(): Kryo = {
    val instantiator = new EmptyScalaKryoInstantiator
    val kryo = instantiator.newKryo()
    kryo.setRegistrationRequired(registrationRequired)

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

    // Register serializers for Avro GenericContainer classes
    // We do not handle SpecificRecordBase and SpecificFixed here. They are abstract classes and
    // we will need to register serializers for their concrete implementations individually.
    // Also, their serialization requires the use of SpecificDatum(Reader|Writer) instead of
    // GenericDatum(Reader|Writer).
    def registerAvro[T <: GenericContainer]()(implicit ct: ClassTag[T]): Unit =
      kryo.register(ct.runtimeClass, new GenericAvroSerializer[T](avroSchemas))
    registerAvro[GenericRecord]()
    registerAvro[GenericData.Record]()
    registerAvro[GenericData.Array[_]]()
    registerAvro[GenericData.EnumSymbol]()
    registerAvro[GenericData.Fixed]()

    // Use the default classloader when calling the user registrator.
    Utils.withContextClassLoader(classLoader) {
      try {
        // Register classes given through spark.kryo.classesToRegister.
        classesToRegister.foreach { className =>
          kryo.register(Utils.classForName(className, noSparkClassLoader = true))
        }
        // Allow the user to register their own classes by setting spark.kryo.registrator.
        userRegistrators
          .map(Utils.classForName[KryoRegistrator](_, noSparkClassLoader = true).
            getConstructor().newInstance())
          .foreach { reg => reg.registerClasses(kryo) }
      } catch {
        case e: Exception =>
          throw new SparkException(
            errorClass = "FAILED_REGISTER_CLASS_WITH_KRYO",
            messageParameters = Map.empty,
            cause = e)
      }
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
    kryo.register(Utils.classForName("scala.collection.immutable.ArraySeq$ofRef"))
    kryo.register(Utils.classForName("scala.collection.immutable.$colon$colon"))
    kryo.register(Utils.classForName("scala.collection.immutable.Map$EmptyMap$"))
    kryo.register(Utils.classForName("scala.math.Ordering$Reverse"))
    kryo.register(Utils.classForName("scala.reflect.ClassTag$GenericClassTag"))
    kryo.register(classOf[ArrayBuffer[Any]])
    kryo.register(classOf[Array[Array[Byte]]])

    // We can't load those class directly in order to avoid unnecessary jar dependencies.
    // We load them safely, ignore it if the class not found.
    KryoSerializer.loadableSparkClasses.foreach { clazz =>
      try {
        kryo.register(clazz)
      } catch {
        case NonFatal(_) => // do nothing
        case _: NoClassDefFoundError if Utils.isTesting => // See SPARK-23422.
      }
    }

    kryo.setClassLoader(classLoader)
    kryo
  }

  override def setDefaultClassLoader(classLoader: ClassLoader): Serializer = {
    super.setDefaultClassLoader(classLoader)
    internalPool.reset()
    this
  }

  override def newInstance(): SerializerInstance = {
    new KryoSerializerInstance(this, useUnsafe, usePool)
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
    outStream: OutputStream,
    useUnsafe: Boolean) extends SerializationStream {

  private[this] var output: KryoOutput =
    if (useUnsafe) new KryoUnsafeOutput(outStream) else new KryoOutput(outStream)

  private[this] var kryo: Kryo = serInstance.borrowKryo()

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    kryo.writeClassAndObject(output, t)
    this
  }

  override def flush(): Unit = {
    if (output == null) {
      throw new IOException("Stream is closed")
    }
    output.flush()
  }

  override def close(): Unit = {
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
    inStream: InputStream,
    useUnsafe: Boolean) extends DeserializationStream {

  private[this] var input: KryoInput =
    if (useUnsafe) new KryoUnsafeInput(inStream) else new KryoInput(inStream)

  private[this] var kryo: Kryo = serInstance.borrowKryo()

  private[this] def hasNext: Boolean = {
    if (input == null) {
      return false
    }

    val eof = input.eof()
    if (eof) close()
    !eof
  }

  override def readObject[T: ClassTag](): T = {
    try {
      kryo.readClassAndObject(input).asInstanceOf[T]
    } catch {
      // DeserializationStream uses the EOF exception to indicate stopping condition.
      case e: KryoException
        if e.getMessage.toLowerCase(Locale.ROOT).contains("buffer underflow") =>
        throw new EOFException
    }
  }

  override def close(): Unit = {
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

  final override def asIterator: Iterator[Any] = new NextIterator[Any] {
    override protected def getNext(): Any = {
      if (KryoDeserializationStream.this.hasNext) {
        try {
          return readObject[Any]()
        } catch {
          case eof: EOFException =>
        }
      }
      finished = true
      null
    }

    override protected def close(): Unit = {
      KryoDeserializationStream.this.close()
    }
  }

  final override def asKeyValueIterator: Iterator[(Any, Any)] = new NextIterator[(Any, Any)] {
    override protected def getNext(): (Any, Any) = {
      if (KryoDeserializationStream.this.hasNext) {
        try {
          return (readKey[Any](), readValue[Any]())
        } catch {
          case eof: EOFException =>
        }
      }
      finished = true
      null
    }

    override protected def close(): Unit = {
      KryoDeserializationStream.this.close()
    }
  }
}

private[spark] class KryoSerializerInstance(
   ks: KryoSerializer, useUnsafe: Boolean, usePool: Boolean)
  extends SerializerInstance {
  /**
   * A re-used [[Kryo]] instance. Methods will borrow this instance by calling `borrowKryo()`, do
   * their work, then release the instance by calling `releaseKryo()`. Logically, this is a caching
   * pool of size one. SerializerInstances are not thread-safe, hence accesses to this field are
   * not synchronized.
   */
  @Nullable private[this] var cachedKryo: Kryo = if (usePool) null else borrowKryo()

  /**
   * Borrows a [[Kryo]] instance. If possible, this tries to re-use a cached Kryo instance;
   * otherwise, it allocates a new instance.
   */
  private[serializer] def borrowKryo(): Kryo = {
    if (usePool) {
      val kryo = ks.pool.borrow()
      kryo.reset()
      kryo
    } else {
      if (cachedKryo != null) {
        val kryo = cachedKryo
        // As a defensive measure, call reset() to clear any Kryo state that might have
        // been modified by the last operation to borrow this instance
        // (see SPARK-7766 for discussion of this issue)
        kryo.reset()
        cachedKryo = null
        kryo
      } else {
        ks.newKryo()
      }
    }
  }

  /**
   * Release a borrowed [[Kryo]] instance. If this serializer instance already has a cached Kryo
   * instance, then the given Kryo instance is discarded; otherwise, the Kryo is stored for later
   * re-use.
   */
  private[serializer] def releaseKryo(kryo: Kryo): Unit = {
    if (usePool) {
      ks.pool.release(kryo)
    } else {
      if (cachedKryo == null) {
        cachedKryo = kryo
      }
    }
  }

  // Make these lazy vals to avoid creating a buffer unless we use them.
  private lazy val output = ks.newKryoOutput()
  private lazy val input = if (useUnsafe) new KryoUnsafeInput() else new KryoInput()

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    output.clear()
    val kryo = borrowKryo()
    try {
      kryo.writeClassAndObject(output, t)
    } catch {
      case e: KryoException if e.getMessage.startsWith("Buffer overflow") =>
        throw new SparkException(
          errorClass = "KRYO_BUFFER_OVERFLOW",
          messageParameters = Map(
            "exceptionMsg" -> e.getMessage,
            "bufferSizeConfKey" -> KRYO_SERIALIZER_MAX_BUFFER_SIZE.key),
          cause = e)
    } finally {
      releaseKryo(kryo)
    }
    ByteBuffer.wrap(output.toBytes)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val kryo = borrowKryo()
    try {
      if (bytes.hasArray) {
        input.setBuffer(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.remaining())
      } else {
        input.setBuffer(new Array[Byte](4096))
        input.setInputStream(new ByteBufferInputStream(bytes))
      }
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
      if (bytes.hasArray) {
        input.setBuffer(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.remaining())
      } else {
        input.setBuffer(new Array[Byte](4096))
        input.setInputStream(new ByteBufferInputStream(bytes))
      }
      kryo.readClassAndObject(input).asInstanceOf[T]
    } finally {
      kryo.setClassLoader(oldClassLoader)
      releaseKryo(kryo)
    }
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new KryoSerializationStream(this, s, useUnsafe)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new KryoDeserializationStream(this, s, useUnsafe)
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
    classOf[Array[ByteBuffer]],
    classOf[StorageLevel],
    classOf[CompressedMapStatus],
    classOf[HighlyCompressedMapStatus],
    classOf[ChunkedByteBuffer],
    classOf[CompactBuffer[_]],
    classOf[BlockManagerId],
    classOf[Array[Boolean]],
    classOf[Array[Byte]],
    classOf[Array[Short]],
    classOf[Array[Int]],
    classOf[Array[Long]],
    classOf[Array[Float]],
    classOf[Array[Double]],
    classOf[Array[Char]],
    classOf[Array[String]],
    classOf[Array[Array[String]]],
    classOf[BoundedPriorityQueue[_]],
    classOf[SparkConf],
    classOf[TaskCommitMessage],
    classOf[SerializedLambda],
    classOf[BitSet]
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

  // classForName() is expensive in case the class is not found, so we filter the list of
  // SQL / ML / MLlib classes once and then re-use that filtered list in newInstance() calls.
  private lazy val loadableSparkClasses: Seq[Class[_]] = {
    Seq(
      "org.apache.spark.sql.catalyst.expressions.BoundReference",
      "org.apache.spark.sql.catalyst.expressions.SortOrder",
      "[Lorg.apache.spark.sql.catalyst.expressions.SortOrder;",
      "org.apache.spark.sql.catalyst.InternalRow",
      "org.apache.spark.sql.catalyst.InternalRow$",
      "[Lorg.apache.spark.sql.catalyst.InternalRow;",
      "org.apache.spark.sql.catalyst.expressions.UnsafeRow",
      "org.apache.spark.sql.catalyst.expressions.UnsafeArrayData",
      "org.apache.spark.sql.catalyst.expressions.UnsafeMapData",
      "org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering",
      "org.apache.spark.sql.catalyst.expressions.Ascending$",
      "org.apache.spark.sql.catalyst.expressions.NullsFirst$",
      "org.apache.spark.sql.catalyst.trees.Origin",
      "org.apache.spark.sql.types.IntegerType",
      "org.apache.spark.sql.types.IntegerType$",
      "org.apache.spark.sql.types.LongType$",
      "org.apache.spark.sql.types.DoubleType",
      "org.apache.spark.sql.types.DoubleType$",
      "org.apache.spark.sql.types.Metadata",
      "org.apache.spark.sql.types.StringType$",
      "org.apache.spark.sql.types.StructField",
      "[Lorg.apache.spark.sql.types.StructField;",
      "org.apache.spark.sql.types.StructType",
      "[Lorg.apache.spark.sql.types.StructType;",
      "org.apache.spark.sql.types.DateType$",
      "org.apache.spark.sql.types.DecimalType",
      "org.apache.spark.sql.types.Decimal$DecimalAsIfIntegral$",
      "org.apache.spark.sql.types.Decimal$DecimalIsFractional$",
      "org.apache.spark.sql.execution.command.PartitionStatistics",
      "org.apache.spark.sql.execution.datasources.BasicWriteTaskStats",
      "org.apache.spark.sql.execution.datasources.ExecutedWriteSummary",
      "org.apache.spark.sql.execution.datasources.WriteTaskResult",
      "org.apache.spark.sql.execution.datasources.v2.DataWritingSparkTaskResult",
      "org.apache.spark.sql.execution.joins.EmptyHashedRelation$",
      "org.apache.spark.sql.execution.joins.LongHashedRelation",
      "org.apache.spark.sql.execution.joins.LongToUnsafeRowMap",
      "org.apache.spark.sql.execution.joins.UnsafeHashedRelation",

      "org.apache.spark.ml.attribute.Attribute",
      "org.apache.spark.ml.attribute.AttributeGroup",
      "org.apache.spark.ml.attribute.BinaryAttribute",
      "org.apache.spark.ml.attribute.NominalAttribute",
      "org.apache.spark.ml.attribute.NumericAttribute",

      "org.apache.spark.ml.feature.Instance",
      "org.apache.spark.ml.feature.InstanceBlock",
      "org.apache.spark.ml.feature.LabeledPoint",
      "org.apache.spark.ml.feature.OffsetInstance",
      "org.apache.spark.ml.linalg.DenseMatrix",
      "org.apache.spark.ml.linalg.DenseVector",
      "org.apache.spark.ml.linalg.Matrix",
      "org.apache.spark.ml.linalg.SparseMatrix",
      "org.apache.spark.ml.linalg.SparseVector",
      "org.apache.spark.ml.linalg.Vector",
      "org.apache.spark.ml.stat.distribution.MultivariateGaussian",
      "org.apache.spark.ml.tree.impl.TreePoint",
      "org.apache.spark.mllib.clustering.VectorWithNorm",
      "org.apache.spark.mllib.linalg.DenseMatrix",
      "org.apache.spark.mllib.linalg.DenseVector",
      "org.apache.spark.mllib.linalg.Matrix",
      "org.apache.spark.mllib.linalg.SparseMatrix",
      "org.apache.spark.mllib.linalg.SparseVector",
      "org.apache.spark.mllib.linalg.Vector",
      "org.apache.spark.mllib.regression.LabeledPoint",
      "org.apache.spark.mllib.stat.distribution.MultivariateGaussian"
    ).flatMap { name =>
      try {
        Some[Class[_]](Utils.classForName(name))
      } catch {
        case NonFatal(_) => None // do nothing
        case _: NoClassDefFoundError if Utils.isTesting => None // See SPARK-23422.
      }
    }
  }
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
  // The class returned by CollectionConverters.asJava
  // (scala.collection.convert.Wrappers$IterableWrapper).
  import scala.jdk.CollectionConverters._
  val wrapperClass = Seq(1).asJava.getClass

  // Get the underlying method so we can use it to get the Scala collection for serialization.
  private val underlyingMethodOpt = {
    try Some(wrapperClass.getDeclaredMethod("underlying")) catch {
      case e: Exception =>
        logError(log"Failed to find the underlying field in ${MDC(CLASS_NAME, wrapperClass)}", e)
        None
    }
  }
}
