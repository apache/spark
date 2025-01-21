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
package org.apache.spark.sql.connect.client.arrow

import java.io.{ByteArrayInputStream, IOException}
import java.lang.invoke.{MethodHandles, MethodType}
import java.lang.reflect.Modifier
import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInteger}
import java.time._
import java.util
import java.util.{List => JList, Locale, Map => JMap}

import scala.collection.immutable
import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.ipc.ArrowReader

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.connect.client.CloseableIterator
import org.apache.spark.sql.errors.{CompilationErrors, ExecutionErrors}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.VariantVal

/**
 * Helper class for converting arrow batches into user objects.
 */
object ArrowDeserializers {
  import ArrowEncoderUtils._
  import org.apache.spark.util.ArrayImplicits._

  /**
   * Create an Iterator of `T`. This iterator takes an Iterator of Arrow IPC Streams, and
   * deserializes these streams into one or more instances of `T`
   */
  def deserializeFromArrow[T](
      input: Iterator[Array[Byte]],
      encoder: AgnosticEncoder[T],
      allocator: BufferAllocator,
      timeZoneId: String): CloseableIterator[T] = {
    try {
      val reader = new ConcatenatingArrowStreamReader(
        allocator,
        input.map(bytes => new MessageIterator(new ByteArrayInputStream(bytes), allocator)),
        destructive = true)
      new ArrowDeserializingIterator(encoder, reader, timeZoneId)
    } catch {
      case _: IOException =>
        new EmptyDeserializingIterator(encoder)
    }
  }

  /**
   * Create a deserializer of `T` on top of the given `root`.
   */
  private[arrow] def deserializerFor[T](
      encoder: AgnosticEncoder[T],
      root: VectorSchemaRoot,
      timeZoneId: String): Deserializer[T] = {
    val data: AnyRef = if (encoder.isStruct) {
      root
    } else {
      // The input schema is allowed to have multiple columns,
      // by convention we bind to the first one.
      root.getVector(0)
    }
    deserializerFor(encoder, data, timeZoneId).asInstanceOf[Deserializer[T]]
  }

  private[arrow] def deserializerFor(
      encoder: AgnosticEncoder[_],
      data: AnyRef,
      timeZoneId: String): Deserializer[Any] = {
    (encoder, data) match {
      case (PrimitiveBooleanEncoder | BoxedBooleanEncoder, v: FieldVector) =>
        new LeafFieldDeserializer[Boolean](encoder, v, timeZoneId) {
          override def value(i: Int): Boolean = reader.getBoolean(i)
        }
      case (PrimitiveByteEncoder | BoxedByteEncoder, v: FieldVector) =>
        new LeafFieldDeserializer[Byte](encoder, v, timeZoneId) {
          override def value(i: Int): Byte = reader.getByte(i)
        }
      case (PrimitiveShortEncoder | BoxedShortEncoder, v: FieldVector) =>
        new LeafFieldDeserializer[Short](encoder, v, timeZoneId) {
          override def value(i: Int): Short = reader.getShort(i)
        }
      case (PrimitiveIntEncoder | BoxedIntEncoder, v: FieldVector) =>
        new LeafFieldDeserializer[Int](encoder, v, timeZoneId) {
          override def value(i: Int): Int = reader.getInt(i)
        }
      case (PrimitiveLongEncoder | BoxedLongEncoder, v: FieldVector) =>
        new LeafFieldDeserializer[Long](encoder, v, timeZoneId) {
          override def value(i: Int): Long = reader.getLong(i)
        }
      case (PrimitiveFloatEncoder | BoxedFloatEncoder, v: FieldVector) =>
        new LeafFieldDeserializer[Float](encoder, v, timeZoneId) {
          override def value(i: Int): Float = reader.getFloat(i)
        }
      case (PrimitiveDoubleEncoder | BoxedDoubleEncoder, v: FieldVector) =>
        new LeafFieldDeserializer[Double](encoder, v, timeZoneId) {
          override def value(i: Int): Double = reader.getDouble(i)
        }
      case (NullEncoder, _: FieldVector) =>
        new Deserializer[Any] {
          def get(i: Int): Any = null
        }
      case (StringEncoder, v: FieldVector) =>
        new LeafFieldDeserializer[String](encoder, v, timeZoneId) {
          override def value(i: Int): String = reader.getString(i)
        }
      case (JavaEnumEncoder(tag), v: FieldVector) =>
        // It would be nice if we can get Enum.valueOf working...
        val valueOf = methodLookup.findStatic(
          tag.runtimeClass,
          "valueOf",
          MethodType.methodType(tag.runtimeClass, classOf[String]))
        new LeafFieldDeserializer[Enum[_]](encoder, v, timeZoneId) {
          override def value(i: Int): Enum[_] = {
            valueOf.invoke(reader.getString(i)).asInstanceOf[Enum[_]]
          }
        }
      case (ScalaEnumEncoder(parent, _), v: FieldVector) =>
        val mirror = scala.reflect.runtime.currentMirror
        val module = mirror.classSymbol(parent).module.asModule
        val enumeration = mirror.reflectModule(module).instance.asInstanceOf[Enumeration]
        new LeafFieldDeserializer[Enumeration#Value](encoder, v, timeZoneId) {
          override def value(i: Int): Enumeration#Value = {
            enumeration.withName(reader.getString(i))
          }
        }
      case (BinaryEncoder, v: FieldVector) =>
        new LeafFieldDeserializer[Array[Byte]](encoder, v, timeZoneId) {
          override def value(i: Int): Array[Byte] = reader.getBytes(i)
        }
      case (SparkDecimalEncoder(_), v: FieldVector) =>
        new LeafFieldDeserializer[Decimal](encoder, v, timeZoneId) {
          override def value(i: Int): Decimal = reader.getDecimal(i)
        }
      case (ScalaDecimalEncoder(_), v: FieldVector) =>
        new LeafFieldDeserializer[BigDecimal](encoder, v, timeZoneId) {
          override def value(i: Int): BigDecimal = reader.getScalaDecimal(i)
        }
      case (JavaDecimalEncoder(_, _), v: FieldVector) =>
        new LeafFieldDeserializer[JBigDecimal](encoder, v, timeZoneId) {
          override def value(i: Int): JBigDecimal = reader.getJavaDecimal(i)
        }
      case (ScalaBigIntEncoder, v: FieldVector) =>
        new LeafFieldDeserializer[BigInt](encoder, v, timeZoneId) {
          override def value(i: Int): BigInt = reader.getScalaBigInt(i)
        }
      case (JavaBigIntEncoder, v: FieldVector) =>
        new LeafFieldDeserializer[JBigInteger](encoder, v, timeZoneId) {
          override def value(i: Int): JBigInteger = reader.getJavaBigInt(i)
        }
      case (DayTimeIntervalEncoder, v: FieldVector) =>
        new LeafFieldDeserializer[Duration](encoder, v, timeZoneId) {
          override def value(i: Int): Duration = reader.getDuration(i)
        }
      case (YearMonthIntervalEncoder, v: FieldVector) =>
        new LeafFieldDeserializer[Period](encoder, v, timeZoneId) {
          override def value(i: Int): Period = reader.getPeriod(i)
        }
      case (DateEncoder(_), v: FieldVector) =>
        new LeafFieldDeserializer[java.sql.Date](encoder, v, timeZoneId) {
          override def value(i: Int): java.sql.Date = reader.getDate(i)
        }
      case (LocalDateEncoder(_), v: FieldVector) =>
        new LeafFieldDeserializer[LocalDate](encoder, v, timeZoneId) {
          override def value(i: Int): LocalDate = reader.getLocalDate(i)
        }
      case (TimestampEncoder(_), v: FieldVector) =>
        new LeafFieldDeserializer[java.sql.Timestamp](encoder, v, timeZoneId) {
          override def value(i: Int): java.sql.Timestamp = reader.getTimestamp(i)
        }
      case (InstantEncoder(_), v: FieldVector) =>
        new LeafFieldDeserializer[Instant](encoder, v, timeZoneId) {
          override def value(i: Int): Instant = reader.getInstant(i)
        }
      case (LocalDateTimeEncoder, v: FieldVector) =>
        new LeafFieldDeserializer[LocalDateTime](encoder, v, timeZoneId) {
          override def value(i: Int): LocalDateTime = reader.getLocalDateTime(i)
        }

      case (OptionEncoder(value), v) =>
        val deserializer = deserializerFor(value, v, timeZoneId)
        new Deserializer[Any] {
          override def get(i: Int): Any = Option(deserializer.get(i))
        }

      case (ArrayEncoder(element, _), v: ListVector) =>
        val deserializer = deserializerFor(element, v.getDataVector, timeZoneId)
        new VectorFieldDeserializer[AnyRef, ListVector](v) {
          def value(i: Int): AnyRef = getArray(vector, i, deserializer)(element.clsTag)
        }

      case (IterableEncoder(tag, element, _, _), v: ListVector) =>
        val deserializer = deserializerFor(element, v.getDataVector, timeZoneId)
        if (isSubClass(Classes.MUTABLE_ARRAY_SEQ, tag)) {
          // mutable ArraySeq is a bit special because we need to use an array of the element type.
          // Some parts of our codebase (unfortunately) rely on this for type inference on results.
          new VectorFieldDeserializer[mutable.ArraySeq[Any], ListVector](v) {
            def value(i: Int): mutable.ArraySeq[Any] = {
              val array = getArray(vector, i, deserializer)(element.clsTag)
              ScalaCollectionUtils.wrap(array)
            }
          }
        } else if (isSubClass(Classes.IMMUTABLE_ARRAY_SEQ, tag)) {
          new VectorFieldDeserializer[immutable.ArraySeq[Any], ListVector](v) {
            def value(i: Int): immutable.ArraySeq[Any] = {
              val array = getArray(vector, i, deserializer)(element.clsTag)
              array.asInstanceOf[Array[_]].toImmutableArraySeq
            }
          }
        } else if (isSubClass(Classes.ITERABLE, tag)) {
          val companion = ScalaCollectionUtils.getIterableCompanion(tag)
          new VectorFieldDeserializer[Iterable[Any], ListVector](v) {
            def value(i: Int): Iterable[Any] = {
              val builder = companion.newBuilder[Any]
              loadListIntoBuilder(vector, i, deserializer, builder)
              builder.result()
            }
          }
        } else if (isSubClass(Classes.JLIST, tag)) {
          val newInstance = resolveJavaListCreator(tag)
          new VectorFieldDeserializer[JList[Any], ListVector](v) {
            def value(i: Int): JList[Any] = {
              var index = v.getElementStartIndex(i)
              val end = v.getElementEndIndex(i)
              val list = newInstance(end - index)
              while (index < end) {
                list.add(deserializer.get(index))
                index += 1
              }
              list
            }
          }
        } else {
          throw unsupportedCollectionType(tag.runtimeClass)
        }

      case (MapEncoder(tag, key, value, _), v: MapVector) =>
        val structVector = v.getDataVector.asInstanceOf[StructVector]
        val keyDeserializer =
          deserializerFor(key, structVector.getChild(MapVector.KEY_NAME), timeZoneId)
        val valueDeserializer =
          deserializerFor(value, structVector.getChild(MapVector.VALUE_NAME), timeZoneId)
        if (isSubClass(Classes.MAP, tag)) {
          val companion = ScalaCollectionUtils.getMapCompanion(tag)
          new VectorFieldDeserializer[Map[Any, Any], MapVector](v) {
            def value(i: Int): Map[Any, Any] = {
              val builder = companion.newBuilder[Any, Any]
              var index = v.getElementStartIndex(i)
              val end = v.getElementEndIndex(i)
              builder.sizeHint(end - index)
              while (index < end) {
                builder += (keyDeserializer.get(index) -> valueDeserializer.get(index))
                index += 1
              }
              builder.result()
            }
          }
        } else if (isSubClass(Classes.JMAP, tag)) {
          val newInstance = resolveJavaMapCreator(tag)
          new VectorFieldDeserializer[JMap[Any, Any], MapVector](v) {
            def value(i: Int): JMap[Any, Any] = {
              val map = newInstance()
              var index = v.getElementStartIndex(i)
              val end = v.getElementEndIndex(i)
              while (index < end) {
                map.put(keyDeserializer.get(index), valueDeserializer.get(index))
                index += 1
              }
              map
            }
          }
        } else {
          throw unsupportedCollectionType(tag.runtimeClass)
        }

      case (ProductEncoder(tag, fields, outerPointerGetter), StructVectors(struct, vectors)) =>
        val outer = outerPointerGetter.map(_()).toSeq
        // We should try to make this work with MethodHandles.
        val Some(constructor) =
          ScalaReflection.findConstructor(
            tag.runtimeClass,
            outer.map(_.getClass) ++ fields.map(_.enc.clsTag.runtimeClass))
        val deserializers = if (isTuple(tag.runtimeClass)) {
          fields.zip(vectors).map { case (field, vector) =>
            deserializerFor(field.enc, vector, timeZoneId)
          }
        } else {
          val outerDeserializer = outer.map { value =>
            new Deserializer[Any] {
              override def get(i: Int): Any = value
            }
          }
          val lookup = createFieldLookup(vectors)
          outerDeserializer ++ fields.map { field =>
            deserializerFor(field.enc, lookup(field.name), timeZoneId)
          }
        }
        new StructFieldSerializer[Any](struct) {
          def value(i: Int): Any = {
            constructor(deserializers.map(_.get(i).asInstanceOf[AnyRef]))
          }
        }

      case (r @ RowEncoder(fields), StructVectors(struct, vectors)) =>
        val lookup = createFieldLookup(vectors)
        val deserializers = fields.toArray.map { field =>
          deserializerFor(field.enc, lookup(field.name), timeZoneId)
        }
        new StructFieldSerializer[Any](struct) {
          def value(i: Int): Any = {
            val values = deserializers.map(_.get(i))
            new GenericRowWithSchema(values, r.schema)
          }
        }

      case (VariantEncoder, StructVectors(struct, vectors)) =>
        assert(vectors.exists(_.getName == "value"))
        assert(
          vectors.exists(field =>
            field.getName == "metadata" && field.getField.getMetadata
              .containsKey("variant") && field.getField.getMetadata.get("variant") == "true"))
        val valueDecoder =
          deserializerFor(
            BinaryEncoder,
            vectors
              .find(_.getName == "value")
              .getOrElse(throw CompilationErrors.columnNotFoundError("value")),
            timeZoneId)
        val metadataDecoder =
          deserializerFor(
            BinaryEncoder,
            vectors
              .find(_.getName == "metadata")
              .getOrElse(throw CompilationErrors.columnNotFoundError("metadata")),
            timeZoneId)
        new StructFieldSerializer[VariantVal](struct) {
          def value(i: Int): VariantVal = {
            new VariantVal(
              valueDecoder.get(i).asInstanceOf[Array[Byte]],
              metadataDecoder.get(i).asInstanceOf[Array[Byte]])
          }
        }

      case (JavaBeanEncoder(tag, fields), StructVectors(struct, vectors)) =>
        val constructor =
          methodLookup.findConstructor(tag.runtimeClass, MethodType.methodType(classOf[Unit]))
        val lookup = createFieldLookup(vectors)
        val setters = fields
          .filter(_.writeMethod.isDefined)
          .map { field =>
            val vector = lookup(field.name)
            val deserializer = deserializerFor(field.enc, vector, timeZoneId)
            val setter = methodLookup.findVirtual(
              tag.runtimeClass,
              field.writeMethod.get,
              MethodType.methodType(classOf[Unit], field.enc.clsTag.runtimeClass))
            (bean: Any, i: Int) => setter.invoke(bean, deserializer.get(i))
          }
        new StructFieldSerializer[Any](struct) {
          def value(i: Int): Any = {
            val instance = constructor.invoke()
            setters.foreach(_(instance, i))
            instance
          }
        }

      case (TransformingEncoder(_, encoder, provider), v) =>
        new Deserializer[Any] {
          private[this] val codec = provider()
          private[this] val deserializer = deserializerFor(encoder, v, timeZoneId)
          override def get(i: Int): Any = codec.decode(deserializer.get(i))
        }

      case (CalendarIntervalEncoder | _: UDTEncoder[_], _) =>
        throw ExecutionErrors.unsupportedDataTypeError(encoder.dataType)

      case _ =>
        throw new RuntimeException(
          s"Unsupported Encoder($encoder)/Vector(${data.getClass}) combination.")
    }
  }

  private val methodLookup = MethodHandles.lookup()

  /**
   * Resolve the companion object for a scala class. In our particular case the class we pass in
   * is a Scala collection. We use the companion to create a builder for that collection.
   */
  private[arrow] def resolveCompanion[T](tag: ClassTag[_]): T = {
    val mirror = scala.reflect.runtime.currentMirror
    val module = mirror.classSymbol(tag.runtimeClass).companion.asModule
    mirror.reflectModule(module).instance.asInstanceOf[T]
  }

  /**
   * Create a function that creates a [[util.List]] instance. The int parameter of the creator
   * function is a size hint.
   *
   * If the [[ClassTag]] `tag` points to an interface instead of a concrete class we try to use
   * [[util.ArrayList]]. For concrete classes we try to use a constructor that takes a single
   * [[Int]] argument, it is assumed this is a size hint. If no such constructor exists we
   * fallback to a no-args constructor.
   */
  private def resolveJavaListCreator(tag: ClassTag[_]): Int => JList[Any] = {
    val cls = tag.runtimeClass
    val modifiers = cls.getModifiers
    if (Modifier.isInterface(modifiers) || Modifier.isAbstract(modifiers)) {
      // Abstract class or interface; we try to use ArrayList.
      if (!cls.isAssignableFrom(classOf[util.ArrayList[_]])) {
        unsupportedCollectionType(cls)
      }
      (size: Int) => new util.ArrayList[Any](size)
    } else {
      try {
        // Try to use a constructor that (hopefully) takes a size argument.
        val ctor = methodLookup.findConstructor(
          tag.runtimeClass,
          MethodType.methodType(classOf[Unit], Integer.TYPE))
        size => ctor.invoke(size).asInstanceOf[JList[Any]]
      } catch {
        case _: java.lang.NoSuchMethodException =>
          // Use a no-args constructor.
          val ctor =
            methodLookup.findConstructor(tag.runtimeClass, MethodType.methodType(classOf[Unit]))
          _ => ctor.invoke().asInstanceOf[JList[Any]]
      }
    }
  }

  /**
   * Create a function that creates a [[util.Map]] instance.
   *
   * If the [[ClassTag]] `tag` points to an interface instead of a concrete class we try to use
   * [[util.HashMap]]. For concrete classes we try to use a no-args constructor.
   */
  private def resolveJavaMapCreator(tag: ClassTag[_]): () => JMap[Any, Any] = {
    val cls = tag.runtimeClass
    val modifiers = cls.getModifiers
    if (Modifier.isInterface(modifiers) || Modifier.isAbstract(modifiers)) {
      // Abstract class or interface; we try to use HashMap.
      if (!cls.isAssignableFrom(classOf[java.util.HashMap[_, _]])) {
        unsupportedCollectionType(cls)
      }
      () => new util.HashMap[Any, Any]()
    } else {
      // Use a no-args constructor.
      val ctor =
        methodLookup.findConstructor(tag.runtimeClass, MethodType.methodType(classOf[Unit]))
      () => ctor.invoke().asInstanceOf[JMap[Any, Any]]
    }
  }

  /**
   * Create a function that can lookup one [[FieldVector vectors]] in `fields` by name. This
   * lookup is case insensitive. If the schema contains fields with duplicate (with
   * case-insensitive resolution) names an exception is thrown. The returned function will throw
   * an exception when no column can be found for a name.
   *
   * A small note on the binding process in general. Over complete schemas are currently allowed,
   * meaning that the data can have more column than the encoder. In this the over complete
   * (unbound) columns are ignored.
   */
  private def createFieldLookup(fields: Seq[FieldVector]): String => FieldVector = {
    def toKey(k: String): String = k.toLowerCase(Locale.ROOT)
    val lookup = mutable.Map.empty[String, FieldVector]
    fields.foreach { field =>
      val key = toKey(field.getName)
      val old = lookup.put(key, field)
      if (old.isDefined) {
        throw CompilationErrors.ambiguousColumnOrFieldError(
          field.getName :: Nil,
          fields.count(f => toKey(f.getName) == key))
      }
    }
    name => {
      lookup.getOrElse(toKey(name), throw CompilationErrors.columnNotFoundError(name))
    }
  }

  private def isTuple(cls: Class[_]): Boolean = cls.getName.startsWith("scala.Tuple")

  private def loadListIntoBuilder(
      v: ListVector,
      i: Int,
      deserializer: Deserializer[Any],
      builder: mutable.Builder[Any, _]): Unit = {
    var index = v.getElementStartIndex(i)
    val end = v.getElementEndIndex(i)
    builder.sizeHint(end - index)
    while (index < end) {
      builder += deserializer.get(index)
      index += 1
    }
  }

  private def getArray(v: ListVector, i: Int, deserializer: Deserializer[Any])(implicit
      tag: ClassTag[Any]): AnyRef = {
    val builder = mutable.ArrayBuilder.make[Any]
    loadListIntoBuilder(v, i, deserializer, builder)
    builder.result()
  }

  abstract class Deserializer[+E] {
    def get(i: Int): E
  }

  abstract class FieldDeserializer[E] extends Deserializer[E] {
    def value(i: Int): E
    def isNull(i: Int): Boolean
    override def get(i: Int): E = {
      if (!isNull(i)) {
        value(i)
      } else {
        null.asInstanceOf[E]
      }
    }
  }

  abstract class LeafFieldDeserializer[E](val reader: ArrowVectorReader)
      extends FieldDeserializer[E] {
    def this(encoder: AgnosticEncoder[_], vector: FieldVector, timeZoneId: String) = {
      this(ArrowVectorReader(encoder.dataType, vector, timeZoneId))
    }
    def value(i: Int): E
    def isNull(i: Int): Boolean = reader.isNull(i)
  }

  abstract class VectorFieldDeserializer[E, V <: FieldVector](val vector: V)
      extends FieldDeserializer[E] {
    def value(i: Int): E
    def isNull(i: Int): Boolean = vector.isNull(i)
  }

  abstract class StructFieldSerializer[E](v: StructVector)
      extends VectorFieldDeserializer[E, StructVector](v) {
    override def isNull(i: Int): Boolean = vector != null && vector.isNull(i)
  }
}

class EmptyDeserializingIterator[E](val encoder: AgnosticEncoder[E])
    extends CloseableIterator[E] {
  override def close(): Unit = ()
  override def hasNext: Boolean = false
  override def next(): E = throw new NoSuchElementException()
}

class ArrowDeserializingIterator[E](
    val encoder: AgnosticEncoder[E],
    private[this] val reader: ArrowReader,
    timeZoneId: String)
    extends CloseableIterator[E] {
  private[this] var index = 0
  private[this] val root = reader.getVectorSchemaRoot
  private[this] val deserializer = ArrowDeserializers.deserializerFor(encoder, root, timeZoneId)

  override def hasNext: Boolean = {
    if (index >= root.getRowCount) {
      if (reader.loadNextBatch()) {
        index = 0
      }
    }
    index < root.getRowCount
  }

  override def next(): E = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    val result = deserializer.get(index)
    index += 1
    result
  }

  override def close(): Unit = reader.close()
}
