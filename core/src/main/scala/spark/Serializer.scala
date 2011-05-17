package spark

import java.io.{InputStream, OutputStream}

/**
 * A serializer. Because some serialization libraries are not thread safe,
 * this class is used to create SerializerInstances that do the actual
 * serialization.
 */
trait Serializer {
  def newInstance(): SerializerInstance
}

/**
 * An instance of the serializer, for use by one thread at a time.
 */
trait SerializerInstance {
  def serialize[T](t: T): Array[Byte]
  def deserialize[T](bytes: Array[Byte]): T
  def outputStream(s: OutputStream): SerializationStream
  def inputStream(s: InputStream): DeserializationStream
}

/**
 * A stream for writing serialized objects.
 */
trait SerializationStream {
  def writeObject[T](t: T): Unit
  def flush(): Unit
  def close(): Unit
}

/**
 * A stream for reading serialized objects.
 */
trait DeserializationStream {
  def readObject[T](): T
  def close(): Unit
}
