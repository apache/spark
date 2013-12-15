package org.apache.spark.api.python

import org.apache.spark.SparkContext
import org.apache.hadoop.io._
import scala.Array
import java.io.{DataOutput, DataInput}

case class TestWritable(var str: String, var numi: Int, var numd: Double) extends Writable {
  def this() = this("", 0, 0.0)

  def write(p1: DataOutput) = {
    p1.writeUTF(str)
    p1.writeInt(numi)
    p1.writeDouble(numd)
  }

  def readFields(p1: DataInput) = {
    str = p1.readUTF()
    numi = p1.readInt()
    numd = p1.readDouble()
  }
}

object WriteInputFormatTests extends App {
  import SparkContext._

  val sc = new SparkContext("local[2]", "test")

  val textPath = "../python/test_support/data/sftext/"
  val intPath = "../python/test_support/data/sfint/"
  val doublePath = "../python/test_support/data/sfdouble/"
  val arrPath = "../python/test_support/data/sfarray/"
  val classPath = "../python/test_support/data/sfclass/"

  val intKeys = Seq((1.0, "aa"), (2.0, "bb"), (2.0, "aa"), (3.0, "cc"), (2.0, "bb"), (1.0, "aa"))
  sc.parallelize(intKeys).saveAsSequenceFile(intPath)
  sc.parallelize(intKeys.map{ case (k, v) => (k.toDouble, v) }).saveAsSequenceFile(doublePath)
  sc.parallelize(intKeys.map{ case (k, v) => (k.toString, v) }).saveAsSequenceFile(textPath)

  val data = Seq(
    (1, Array(1.0, 2.0, 3.0)),
    (2, Array(3.0, 4.0, 5.0)),
    (3, Array(4.0, 5.0, 6.0))
  )
  sc.parallelize(data, numSlices = 2)
    .map{ case (k, v) =>
      (new IntWritable(k), new ArrayWritable(classOf[DoubleWritable], v.map(new DoubleWritable(_))))
    }
    .saveAsNewAPIHadoopFile[org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat[IntWritable, ArrayWritable]](arrPath)

  val testClass = Seq(
    ("1", TestWritable("test1", 123, 54.0)),
    ("2", TestWritable("test2", 456, 8762.3)),
    ("1", TestWritable("test3", 123, 423.1)),
    ("3", TestWritable("test56", 456, 423.5)),
    ("2", TestWritable("test2", 123, 5435.2))
  )
  val rdd = sc.parallelize(testClass, numSlices = 2).map{ case (k, v) => (new Text(k), v) }
  rdd.saveAsNewAPIHadoopFile(classPath,
                             classOf[Text], classOf[TestWritable],
                             classOf[org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat[Text, TestWritable]])


}