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

import java.io.IOException
import java.lang.invoke.{MethodHandles, MethodType}
import java.lang.reflect.Modifier
import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInteger}
import java.time._
import java.util
import java.util.{List => JList, Locale, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.generic.{GenericCompanion, GenMapFactory}
import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BigIntVector, BitVector, DateDayVector, DecimalVector, DurationVector, FieldVector, Float4Vector, Float8Vector, IntervalYearVector, IntVector, NullVector, SmallIntVector, TimeStampMicroTZVector, TimeStampMicroVector, TinyIntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.util.Text

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.types.{Decimal, StructType}
import org.apache.spark.sql.util.ArrowUtils

/**
 * Helper class for converting arrow batches into user objects.
 */
object ArrowDeserializers {
  import ArrowEncoderUtils._
  /**
   * Create an Iterator of `T`. This iterator takes an Iterator of Arrow IPC Streams,
   * and deserializes these streams into one or more instances of `T`
   */
  def deserializeFromArrow[T](
      input: Iterator[Array[Byte]],
      encoder: AgnosticEncoder[T],
      allocator: BufferAllocator): TypedDeserializingIterator[T] = {
    try {
      val reader = new ConcatenatingArrowStreamReader(allocator, input)
      new ArrowDeserializingIterator(encoder, reader)
    } catch {
      case _: IOException =>
        new EmptyDeserializingIterator(encoder)
    }
  }

  /**
   * Create an Iterator of [[Row]]. This iterator takes an Iterator of Arrow IPC Streams,
   * and deserializes these streams into one or more instances of [[T]].
   *
   * The schema of the contained in the first IPC stream is used to construct the Row encoder.
   * All subsequent streams must have the same schema.
   */
  def deserializeFromArrow(
      input: Iterator[Array[Byte]],
      allocator: BufferAllocator): TypedDeserializingIterator[Row] = {
    try {
      val reader = new ConcatenatingArrowStreamReader(allocator, input)
      val schema = ArrowUtils.fromArrowSchema(reader.getVectorSchemaRoot.getSchema)
      val encoder = org.apache.spark.sql.catalyst.encoders.RowEncoder.encoderFor(schema)
      new ArrowDeserializingIterator(encoder, reader)
    } catch {
      case e: IOException =>
        new EmptyDeserializingIterator(RowEncoder(Nil))
    }
  }

  /**
   * Create a deserializer of `T` on top of the given `root`.
   */
  private[arrow] def deserializerFor[T](
      encoder: AgnosticEncoder[T],
      root: VectorSchemaRoot): Deserializer[T] = {
    val data: AnyRef = if (encoder.schema == encoder.dataType) {
      root
    } else {
      // The input schema is allowed to have multiple columns,
      // by convention we bind to the first one.
      root.getVector(0)
    }
    deserializerFor(encoder, data).asInstanceOf[Deserializer[T]]
  }

  private[arrow] def deserializerFor(
      encoder: AgnosticEncoder[_],
      data: AnyRef): Deserializer[Any] = {
    (encoder, data) match {
      case (PrimitiveBooleanEncoder | BoxedBooleanEncoder, v: BitVector) =>
        new FieldDeserializer[Boolean, BitVector](v) {
          def value(i: Int): Boolean = vector.get(i) != 0
        }
      case (PrimitiveByteEncoder | BoxedByteEncoder, v: TinyIntVector) =>
        new FieldDeserializer[Byte, TinyIntVector](v) {
          def value(i: Int): Byte = vector.get(i)
        }
      case (PrimitiveShortEncoder | BoxedShortEncoder, v: SmallIntVector) =>
        new FieldDeserializer[Short, SmallIntVector](v) {
          def value(i: Int): Short = vector.get(i)
        }
      case (PrimitiveIntEncoder | BoxedIntEncoder, v: IntVector) =>
        new FieldDeserializer[Int, IntVector](v) {
          def value(i: Int): Int = vector.get(i)
        }
      case (PrimitiveLongEncoder | BoxedLongEncoder, v: BigIntVector) =>
        new FieldDeserializer[Long, BigIntVector](v) {
          def value(i: Int): Long = vector.get(i)
        }
      case (PrimitiveFloatEncoder | BoxedFloatEncoder, v: Float4Vector) =>
        new FieldDeserializer[Float, Float4Vector](v) {
          def value(i: Int): Float = vector.get(i)
        }
      case (PrimitiveDoubleEncoder | BoxedDoubleEncoder, v: Float8Vector) =>
        new FieldDeserializer[Double, Float8Vector](v) {
          def value(i: Int): Double = vector.get(i)
        }
      case (NullEncoder, v: NullVector) =>
        new FieldDeserializer[Any, NullVector](v) {
          def value(i: Int): Any = null
        }
      case (StringEncoder, v: VarCharVector) =>
        new FieldDeserializer[String, VarCharVector](v) {
          def value(i: Int): String = getString(vector, i)
        }
      case (JavaEnumEncoder(tag), v: VarCharVector) =>
        // TODO  see if we can get Enum.valueOf working...
        val valueOf = methodLookup.findStatic(
          tag.runtimeClass,
          "valueOf",
          MethodType.methodType(tag.runtimeClass, classOf[String]))
        new FieldDeserializer[Enum[_], VarCharVector](v) {
          def value(i: Int): Enum[_] = {
            valueOf.invoke(getString(vector, i)).asInstanceOf[Enum[_]]
          }
        }
      case (ScalaEnumEncoder(parent, _), v: VarCharVector) =>
        val mirror = scala.reflect.runtime.currentMirror
        val module = mirror.classSymbol(parent).module.asModule
        val enumeration = mirror.reflectModule(module).instance.asInstanceOf[Enumeration]
        new FieldDeserializer[Enumeration#Value, VarCharVector](v) {
          def value(i: Int): Enumeration#Value = enumeration.withName(getString(vector, i))
        }
      case (BinaryEncoder, v: VarBinaryVector) =>
        new FieldDeserializer[Array[Byte], VarBinaryVector](v) {
          def value(i: Int): Array[Byte] = vector.get(i)
        }
      case (SparkDecimalEncoder(_), v: DecimalVector) =>
        new FieldDeserializer[Decimal, DecimalVector](v) {
          def value(i: Int): Decimal = Decimal(vector.getObject(i))
        }
      case (ScalaDecimalEncoder(_), v: DecimalVector) =>
        new FieldDeserializer[BigDecimal, DecimalVector](v) {
          def value(i: Int): BigDecimal = BigDecimal(vector.getObject(i))
        }
      case (JavaDecimalEncoder(_, _), v: DecimalVector) =>
        new FieldDeserializer[JBigDecimal, DecimalVector](v) {
          def value(i: Int): JBigDecimal = vector.getObject(i)
        }
      case (ScalaBigIntEncoder, v: DecimalVector) =>
        new FieldDeserializer[BigInt, DecimalVector](v) {
          def value(i: Int): BigInt = new BigInt(vector.getObject(i).toBigInteger)
        }
      case (JavaBigIntEncoder, v: DecimalVector) =>
        new FieldDeserializer[JBigInteger, DecimalVector](v) {
          def value(i: Int): JBigInteger = vector.getObject(i).toBigInteger
        }
      case (DayTimeIntervalEncoder, v: DurationVector) =>
        new FieldDeserializer[Duration, DurationVector](v) {
          def value(i: Int): Duration = vector.getObject(i)
        }
      case (YearMonthIntervalEncoder, v: IntervalYearVector) =>
        new FieldDeserializer[Period, IntervalYearVector](v) {
          def value(i: Int): Period = vector.getObject(i).normalized()
        }
      case (DateEncoder(_), v: DateDayVector) =>
        new FieldDeserializer[java.sql.Date, DateDayVector](v) {
          def value(i: Int): java.sql.Date = DateTimeUtils.toJavaDate(vector.get(i))
        }
      case (LocalDateEncoder(_), v: DateDayVector) =>
        new FieldDeserializer[LocalDate, DateDayVector](v) {
          def value(i: Int): LocalDate = DateTimeUtils.daysToLocalDate(vector.get(i))
        }
      case (TimestampEncoder(_), v: TimeStampMicroTZVector) =>
        new FieldDeserializer[java.sql.Timestamp, TimeStampMicroTZVector](v) {
          def value(i: Int): java.sql.Timestamp = DateTimeUtils.toJavaTimestamp(vector.get(i))
        }
      case (InstantEncoder(_), v: TimeStampMicroTZVector) =>
        new FieldDeserializer[Instant, TimeStampMicroTZVector](v) {
          def value(i: Int): Instant = DateTimeUtils.microsToInstant(vector.get(i))
        }
      case (LocalDateTimeEncoder, v: TimeStampMicroVector) =>
        new FieldDeserializer[LocalDateTime, TimeStampMicroVector](v) {
          def value(i: Int): LocalDateTime = DateTimeUtils.microsToLocalDateTime(vector.get(i))
        }

      case (OptionEncoder(value), v) =>
        val deserializer = deserializerFor(value, v)
        new Deserializer[Any] {
          override def get(i: Int): Any = Option(deserializer.get(i))
        }

      case (ArrayEncoder(element, _), v: ListVector) =>
        val deserializer = deserializerFor(element, v.getDataVector)
        new FieldDeserializer[AnyRef, ListVector](v) {
          def value(i: Int): AnyRef = getArray(vector, i, deserializer)(element.clsTag)
        }

      case (IterableEncoder(tag, element, _, _), v: ListVector) =>
        val deserializer = deserializerFor(element, v.getDataVector)
        if (isSubClass(Classes.WRAPPED_ARRAY, tag)) {
          // Wrapped array is a bit special because we need to use an array of the element type.
          // Some parts of our codebase (unfortunately) rely on this for type inference on results.
          new FieldDeserializer[mutable.WrappedArray[Any], ListVector](v) {
            def value(i: Int): mutable.WrappedArray[Any] = {
              val array = getArray(vector, i, deserializer)(element.clsTag)
              mutable.WrappedArray.make(array)
            }
          }
        } else if (isSubClass(Classes.ITERABLE, tag)) {
          val companion = resolveCompanion[GenericCompanion[Iterable]](tag)
          new FieldDeserializer[Iterable[Any], ListVector](v) {
            def value(i: Int): Iterable[Any] = {
              val builder = companion.newBuilder[Any]
              loadListIntoBuilder(vector, i, deserializer, builder)
              builder.result()
            }
          }
        } else if (isSubClass(Classes.JLIST, tag)) {
          val newInstance = resolveJavaListCreator(tag)
          new FieldDeserializer[JList[Any], ListVector](v) {
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
        val keyDeserializer = deserializerFor(key, structVector.getChild(MapVector.KEY_NAME))
        val valueDeserializer = deserializerFor(value, structVector.getChild(MapVector.VALUE_NAME))
        if (isSubClass(Classes.MAP, tag)) {
          val companion = resolveCompanion[GenMapFactory[Map]](tag)
          new FieldDeserializer[Map[Any, Any], MapVector](v) {
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
          new FieldDeserializer[JMap[Any, Any], MapVector](v) {
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

      case (ProductEncoder(tag, fields), StructVectors(struct, vectors)) =>
        val methodType = MethodType.methodType(
          classOf[Unit],
          fields.map(_.enc.clsTag.runtimeClass).asJava)
        val constructor = methodLookup.findConstructor(tag.runtimeClass, methodType)
          .asSpreader(0, classOf[Array[Any]], fields.size)
        val deserializers = if (isTuple(tag.runtimeClass)) {
          fields.zip(vectors).map {
            case (field, vector) =>
              deserializerFor(field.enc, vector)
          }
        } else {
          val lookup = createFieldLookup(vectors)
          fields.toArray.map { field =>
            deserializerFor(field.enc, lookup(field.name))
          }
        }
        new StructFieldSerializer[Any](struct) {
          def value(i: Int): Any = {
            val parameters = deserializers.map(_.get(i))
            constructor.invoke(parameters) // See if we can get invokeExact to work here.
          }
        }

      case (r @ RowEncoder(fields), StructVectors(struct, vectors)) =>
        val lookup = createFieldLookup(vectors)
        val deserializers = fields.toArray.map { field =>
          deserializerFor(field.enc, lookup(field.name))
        }
        new StructFieldSerializer[Any](struct) {
          def value(i: Int): Any = {
            val values = deserializers.map(_.get(i))
            new GenericRowWithSchema(values, r.schema)
          }
        }

      case (JavaBeanEncoder(tag, fields), StructVectors(struct, vectors)) =>
        val constructor = methodLookup.findConstructor(
          tag.runtimeClass,
          MethodType.methodType(classOf[Unit]))
        val lookup = createFieldLookup(vectors)
        val setters = fields.map { field =>
          val vector = lookup(field.name)
          val deserializer = deserializerFor(field.enc, vector)
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

      case (CalendarIntervalEncoder | _: UDTEncoder[_], _) =>
        throw QueryExecutionErrors.unsupportedDataTypeError(encoder.dataType)

      case _ =>
        throw new RuntimeException(s"Unsupported Encoder($encoder)/Vector($data) combination.")
    }
  }

  private val methodLookup = MethodHandles.lookup()

  /**
   * Resolve the companion object for a scala class. In our particular case the class we pass in
   * is a Scala collection. We use the companion to create a builder for that collection.
   */
  private def resolveCompanion[T](tag: ClassTag[_]): T = {
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
   * [[Int]] argument, it is assumed this is a size hint. If no such constructor exists we fallback
   * to a no-args constructor.
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
          val ctor = methodLookup.findConstructor(
            tag.runtimeClass,
            MethodType.methodType(classOf[Unit]))
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
      val ctor = methodLookup.findConstructor(
        tag.runtimeClass,
        MethodType.methodType(classOf[Unit]))
      () => ctor.invoke().asInstanceOf[JMap[Any, Any]]
    }
  }

  /**
   * Create a function that can lookup one [[FieldVector vectors]] in `fields` by name.
   * This lookup is case insensitive. If the schema contains fields with duplicate (with
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
        throw QueryCompilationErrors.ambiguousColumnOrFieldError(
          field.getName :: Nil,
          fields.count(f => toKey(f.getName) == key))
      }
    }
    name => {
      lookup.getOrElse(toKey(name), throw QueryCompilationErrors.columnNotFoundError(name))
    }
  }

  private def isTuple(cls: Class[_]): Boolean = cls.getName.startsWith("scala.Tuple")

  private def getString(v: VarCharVector, i: Int): String = {
    // This is currently a bit heavy on allocations:
    // - byte array created in VarCharVector.get
    // - CharBuffer created CharSetEncoder
    // - char array in String
    // By using direct buffers and reusing the char buffer
    // we could get rid of the first two allocations.
    Text.decode(v.get(i))
  }

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

  private def getArray(
                        v: ListVector,
                        i: Int,
                        deserializer: Deserializer[Any])(
                        implicit tag: ClassTag[Any]): AnyRef = {
    val builder = mutable.ArrayBuilder.make[Any]
    loadListIntoBuilder(v, i, deserializer, builder)
    builder.result()
  }

  abstract class Deserializer[+E] {
    def get(i: Int): E
  }

  abstract class FieldDeserializer[E, V <: FieldVector](val vector: V) extends Deserializer[E] {
    def value(i: Int): E
    def isNull(i: Int): Boolean = vector.isNull(i)
    override def get(i: Int): E = {
      if (!isNull(i)) {
        value(i)
      } else {
        null.asInstanceOf[E]
      }
    }
  }

  abstract class StructFieldSerializer[E](v: StructVector)
    extends FieldDeserializer[E, StructVector](v) {
    override def isNull(i: Int): Boolean = vector != null && vector.isNull(i)
  }
}

trait TypedDeserializingIterator[E] extends CloseableIterator[E] {
  def encoder: AgnosticEncoder[E]
  def schema: StructType = encoder.schema
}

class EmptyDeserializingIterator[E](override val encoder: AgnosticEncoder[E])
  extends TypedDeserializingIterator[E] {
  override def close(): Unit = ()
  override def hasNext: Boolean = false
  override def next(): E = throw new NoSuchElementException()
}

class ArrowDeserializingIterator[E](
    override val encoder: AgnosticEncoder[E],
    private[this] val reader: ConcatenatingArrowStreamReader)
  extends TypedDeserializingIterator[E] {
  private[this] var index = 0
  private[this] val root = reader.getVectorSchemaRoot
  private[this] val deserializer = ArrowDeserializers.deserializerFor(encoder, root)

  override def hasNext: Boolean = {
    if (index >= root.getRowCount) {
      reader.loadNextBatch()
      index = 0
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

