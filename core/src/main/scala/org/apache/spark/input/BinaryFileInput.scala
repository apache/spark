package org.apache.spark.input

import scala.collection.JavaConversions._
import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader


/**
 *  The new (Hadoop 2.0) InputFormat for while binary files (not be to be confused with the recordreader itself)
 */
@serializable abstract class BinaryFileInputFormat[T]
  extends CombineFileInputFormat[String,T]  {
  override protected def isSplitable(context: JobContext, file: Path): Boolean = false
  /**
   * Allow minPartitions set by end-user in order to keep compatibility with old Hadoop API.
   */
  def setMaxSplitSize(context: JobContext, minPartitions: Int) {
    val files = listStatus(context)
    val totalLen = files.map { file =>
      if (file.isDir) 0L else file.getLen
    }.sum

    /** val maxSplitSize = Math.ceil(totalLen * 1.0 /
      (if (minPartitions == 0) 1 else minPartitions)).toLong **/
    val maxSplitSize = Math.ceil(totalLen*1.0/files.length).toLong
    super.setMaxSplitSize(maxSplitSize)
  }

  def createRecordReader(split: InputSplit, taContext: TaskAttemptContext): RecordReader[String,T]

}

/**
 * A [[org.apache.hadoop.mapreduce.RecordReader RecordReader]] for reading a single whole tiff file
 * out in a key-value pair, where the key is the file path and the value is the entire content of
 * the file as a TSliceReader (to keep the size information
 */
@serializable abstract class BinaryRecordReader[T](
                                                    split: CombineFileSplit,
                                                    context: TaskAttemptContext,
                                                    index: Integer)
  extends RecordReader[String, T] {

  private val path = split.getPath(index)
  private val fs = path.getFileSystem(context.getConfiguration)

  // True means the current file has been processed, then skip it.
  private var processed = false

  private val key = path.toString
  private var value: T = null.asInstanceOf[T]
  override def initialize(split: InputSplit, context: TaskAttemptContext) = {}
  override def close() = {}

  override def getProgress = if (processed) 1.0f else 0.0f

  override def getCurrentKey = key

  override def getCurrentValue = value

  override def nextKeyValue = {
    if (!processed) {
      val fileIn = fs.open(path)
      val innerBuffer = ByteStreams.toByteArray(fileIn)
      value = parseByteArray(innerBuffer)
      Closeables.close(fileIn, false)

      processed = true
      true
    } else {
      false
    }
  }
  def parseByteArray(inArray: Array[Byte]): T
}

/**
 * A demo class for extracting just the byte array itself
 */

@serializable class ByteInputFormat extends BinaryFileInputFormat[Array[Byte]] {
  override def createRecordReader(split: InputSplit, taContext: TaskAttemptContext)=
  {
    new CombineFileRecordReader[String,Array[Byte]](split.asInstanceOf[CombineFileSplit],taContext,classOf[ByteRecordReader])
  }
}

@serializable class ByteRecordReader(
                                      split: CombineFileSplit,
                                      context: TaskAttemptContext,
                                      index: Integer)
  extends BinaryRecordReader[Array[Byte]](split,context,index) {

  def parseByteArray(inArray: Array[Byte]) = inArray
}
