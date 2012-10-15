package spark.serializer

import java.nio.ByteBuffer
import java.io.{EOFException, InputStream, OutputStream}
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream
import spark.util.ByteBufferInputStream

/**
 * A serializer. Because some serialization libraries are not thread safe, this class is used to
 * create [[spark.serializer.SerializerInstance]] objects that do the actual serialization and are
 * guaranteed to only be called from one thread at a time.
 */
trait Serializer {
  def newInstance(): SerializerInstance
}

/**
 * An instance of a serializer, for use by one thread at a time.
 */
trait SerializerInstance {
  def serialize[T](t: T): ByteBuffer

  def deserialize[T](bytes: ByteBuffer): T

  def deserialize[T](bytes: ByteBuffer, loader: ClassLoader): T

  def serializeStream(s: OutputStream): SerializationStream

  def deserializeStream(s: InputStream): DeserializationStream

  def serializeMany[T](iterator: Iterator[T]): ByteBuffer = {
    // Default implementation uses serializeStream
    val stream = new FastByteArrayOutputStream()
    serializeStream(stream).writeAll(iterator)
    val buffer = ByteBuffer.allocate(stream.position.toInt)
    buffer.put(stream.array, 0, stream.position.toInt)
    buffer.flip()
    buffer
  }

  def deserializeMany(buffer: ByteBuffer): Iterator[Any] = {
    // Default implementation uses deserializeStream
    buffer.rewind()
    deserializeStream(new ByteBufferInputStream(buffer)).asIterator
  }
}

/**
 * A stream for writing serialized objects.
 */
trait SerializationStream {
  def writeObject[T](t: T): SerializationStream
  def flush(): Unit
  def close(): Unit

  def writeAll[T](iter: Iterator[T]): SerializationStream = {
    while (iter.hasNext) {
      writeObject(iter.next())
    }
    this
  }
}

/**
 * A stream for reading serialized objects.
 */
trait DeserializationStream {
  def readObject[T](): T
  def close(): Unit

  /**
   * Read the elements of this stream through an iterator. This can only be called once, as
   * reading each element will consume data from the input source.
   */
  def asIterator: Iterator[Any] = new Iterator[Any] {
    var gotNext = false
    var finished = false
    var nextValue: Any = null

    private def getNext() {
      try {
        nextValue = readObject[Any]()
      } catch {
        case eof: EOFException =>
          finished = true
      }
      gotNext = true
    }

    override def hasNext: Boolean = {
      if (!gotNext) {
        getNext()
      }
      if (finished) {
        close()
      }
      !finished
    }

    override def next(): Any = {
      if (!gotNext) {
        getNext()
      }
      if (finished) {
        throw new NoSuchElementException("End of stream")
      }
      gotNext = false
      nextValue
    }
  }
}
