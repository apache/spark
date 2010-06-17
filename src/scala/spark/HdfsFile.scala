package spark

import nexus.SlaveOffer

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter

class HdfsSplit(@transient s: InputSplit)
extends SerializableWritable[InputSplit](s) {}

class HdfsTextFile(sc: SparkContext, path: String)
extends RDD[String, HdfsSplit](sc) {
  @transient val conf = new JobConf()
  @transient val inputFormat = new TextInputFormat()

  FileInputFormat.setInputPaths(conf, path)
  ConfigureLock.synchronized { inputFormat.configure(conf) }

  @transient val splits_ =
    inputFormat.getSplits(conf, 2).map(new HdfsSplit(_)).toArray

  override def splits = splits_
  
  override def iterator(split: HdfsSplit) = new Iterator[String] {
    var reader: RecordReader[LongWritable, Text] = null
    ConfigureLock.synchronized {
      val conf = new JobConf()
      conf.set("io.file.buffer.size",
          System.getProperty("spark.buffer.size", "65536"))
      val tif = new TextInputFormat()
      tif.configure(conf) 
      reader = tif.getRecordReader(split.value, conf, Reporter.NULL)
    }
    val lineNum = new LongWritable()
    val text = new Text()
    var gotNext = false
    var finished = false

    override def hasNext: Boolean = {
      if (!gotNext) {
        finished = !reader.next(lineNum, text)
        gotNext = true
      }
      !finished
    }

    override def next: String = {
      if (!gotNext)
        finished = !reader.next(lineNum, text)
      if (finished)
        throw new java.util.NoSuchElementException("end of stream")
      gotNext = false
      text.toString
    }
  }

  override def prefers(split: HdfsSplit, slot: SlaveOffer) =
    split.value.getLocations().contains(slot.getHost)
}

object ConfigureLock {}
