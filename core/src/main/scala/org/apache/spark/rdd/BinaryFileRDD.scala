package org.apache.spark.rdd

/** Allows better control of the partitioning
  *
  */
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.input.WholeTextFileInputFormat
import org.apache.spark.InterruptibleIterator
import org.apache.spark.Logging
import org.apache.spark.Partition
import org.apache.spark.SerializableWritable
import org.apache.spark.{SparkContext, TaskContext}

import org.apache.spark.input.BinaryFileInputFormat

private[spark] class BinaryFileRDD[T](
                                       sc : SparkContext,
                                       inputFormatClass: Class[_ <: BinaryFileInputFormat[T]],
                                       keyClass: Class[String],
                                       valueClass: Class[T],
                                       @transient conf: Configuration,
                                       minPartitions: Int)
  extends NewHadoopRDD[String, T](sc, inputFormatClass, keyClass, valueClass, conf) {

  override def getPartitions: Array[Partition] = {
    val inputFormat = inputFormatClass.newInstance
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(conf)
      case _ =>
    }
    val jobContext = newJobContext(conf, jobId)
    inputFormat.setMaxSplitSize(jobContext, minPartitions)
    val rawSplits = inputFormat.getSplits(jobContext).toArray
    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }
}
