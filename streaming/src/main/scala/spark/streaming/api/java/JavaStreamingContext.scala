package spark.streaming.api.java

import scala.collection.JavaConversions._
import java.util.{List => JList}

import spark.streaming._
import dstream._
import spark.storage.StorageLevel
import spark.api.java.function.{Function => JFunction, Function2 => JFunction2}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import java.io.InputStream

class JavaStreamingContext(val ssc: StreamingContext) {
  def this(master: String, frameworkName: String, batchDuration: Time) =
    this(new StreamingContext(master, frameworkName, batchDuration))

  // TODOs:
  // - Test StreamingContext functions
  // - Test to/from Hadoop functions
  // - Add checkpoint()/remember()
  // - Support creating your own streams
  // - Add Kafka Stream

  /**
   * Create a input stream from network source hostname:port. Data is received using
   * a TCP socket and the receive bytes is interpreted as UTF8 encoded \n delimited
   * lines.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @param storageLevel  Storage level to use for storing the received objects
   *                      (default: StorageLevel.MEMORY_AND_DISK_SER_2)
   */
  def networkTextStream(hostname: String, port: Int, storageLevel: StorageLevel)
  : JavaDStream[String] = {
    ssc.networkTextStream(hostname, port, storageLevel)
  }

  /**
   * Create a input stream from network source hostname:port. Data is received using
   * a TCP socket and the receive bytes is interpreted as UTF8 encoded \n delimited
   * lines.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   */
  def networkTextStream(hostname: String, port: Int): JavaDStream[String] = {
    ssc.networkTextStream(hostname, port)
  }

  /**
   * Create a input stream from network source hostname:port. Data is received using
   * a TCP socket and the receive bytes it interepreted as object using the given
   * converter.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @param converter     Function to convert the byte stream to objects
   * @param storageLevel  Storage level to use for storing the received objects
   * @tparam T            Type of the objects received (after converting bytes to objects)
   */
  def networkStream[T](
      hostname: String,
      port: Int,
      converter: JFunction[InputStream, java.lang.Iterable[T]],
      storageLevel: StorageLevel)
  : JavaDStream[T] = {
    import scala.collection.JavaConverters._
    def fn = (x: InputStream) => converter.apply(x).toIterator
    implicit val cmt: ClassManifest[T] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[T]]
    ssc.networkStream(hostname, port, fn, storageLevel)
  }

  /**
   * Creates a input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them as text files (using key as LongWritable, value
   * as Text and input format as TextInputFormat). File names starting with . are ignored.
   * @param directory HDFS directory to monitor for new file
   */
  def textFileStream(directory: String): JavaDStream[String] = {
    ssc.textFileStream(directory)
  }

  /**
   * Create a input stream from network source hostname:port, where data is received
   * as serialized blocks (serialized using the Spark's serializer) that can be directly
   * pushed into the block manager without deserializing them. This is the most efficient
   * way to receive data.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @param storageLevel  Storage level to use for storing the received objects
   * @tparam T            Type of the objects in the received blocks
   */
  def rawNetworkStream[T](
      hostname: String,
      port: Int,
      storageLevel: StorageLevel): JavaDStream[T] = {
    implicit val cmt: ClassManifest[T] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[T]]
    JavaDStream.fromDStream(ssc.rawNetworkStream(hostname, port, storageLevel))
  }

  /**
   * Create a input stream from network source hostname:port, where data is received
   * as serialized blocks (serialized using the Spark's serializer) that can be directly
   * pushed into the block manager without deserializing them. This is the most efficient
   * way to receive data.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @tparam T            Type of the objects in the received blocks
   */
  def rawNetworkStream[T](hostname: String, port: Int): JavaDStream[T] = {
    implicit val cmt: ClassManifest[T] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[T]]
    JavaDStream.fromDStream(ssc.rawNetworkStream(hostname, port))
  }

  /**
   * Creates a input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them using the given key-value types and input format.
   * File names starting with . are ignored.
   * @param directory HDFS directory to monitor for new file
   * @tparam K Key type for reading HDFS file
   * @tparam V Value type for reading HDFS file
   * @tparam F Input format for reading HDFS file
   */
  def fileStream[K, V, F <: NewInputFormat[K, V]](directory: String): JavaPairDStream[K, V] = {
    implicit val cmk: ClassManifest[K] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[K]]
    implicit val cmv: ClassManifest[V] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[V]]
    implicit val cmf: ClassManifest[F] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[F]]
    ssc.fileStream[K, V, F](directory);
  }

  /**
   * Creates a input stream from a Flume source.
   * @param hostname Hostname of the slave machine to which the flume data will be sent
   * @param port     Port of the slave machine to which the flume data will be sent
   * @param storageLevel  Storage level to use for storing the received objects
   */
  def flumeStream(hostname: String, port: Int, storageLevel: StorageLevel):
    JavaDStream[SparkFlumeEvent] = {
    ssc.flumeStream(hostname, port, storageLevel)
  }


  /**
   * Creates a input stream from a Flume source.
   * @param hostname Hostname of the slave machine to which the flume data will be sent
   * @param port     Port of the slave machine to which the flume data will be sent
   */
  def flumeStream(hostname: String, port: Int):
  JavaDStream[SparkFlumeEvent] = {
    ssc.flumeStream(hostname, port)
  }

  // NOT SUPPORTED: registerInputStream

  /**
   * Registers an output stream that will be computed every interval
   */
  def registerOutputStream(outputStream: JavaDStreamLike[_, _]) {
    ssc.registerOutputStream(outputStream.dstream)
  }

  /**
   * Starts the execution of the streams.
   */
  def start() = ssc.start()

  /**
   * Sstops the execution of the streams.
   */
  def stop() = ssc.stop()

}
