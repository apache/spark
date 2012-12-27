package spark.rdd

import spark._
import org.apache.hadoop.mapred.{FileInputFormat, SequenceFileInputFormat, JobConf, Reporter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{NullWritable, BytesWritable}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.fs.Path
import java.io.{File, IOException, EOFException}
import java.text.NumberFormat

private[spark] class CheckpointRDDSplit(idx: Int, val splitFile: String) extends Split {
  override val index: Int = idx
}

class CheckpointRDD[T: ClassManifest](sc: SparkContext, checkpointPath: String)
  extends RDD[T](sc, Nil) {

  @transient val path = new Path(checkpointPath)
  @transient val fs = path.getFileSystem(new Configuration())

  @transient val splits_ : Array[Split] = {
    val splitFiles = fs.listStatus(path).map(_.getPath.toString).filter(_.contains("part-")).sorted
    splitFiles.zipWithIndex.map(x => new CheckpointRDDSplit(x._2, x._1)).toArray
  }

  checkpointData = Some(new RDDCheckpointData[T](this))
  checkpointData.get.cpFile = Some(checkpointPath)

  override def getSplits = splits_

  override def getPreferredLocations(split: Split): Seq[String] = {
    val status = fs.getFileStatus(path)
    val locations = fs.getFileBlockLocations(status, 0, status.getLen)
    locations.firstOption.toList.flatMap(_.getHosts).filter(_ != "localhost")
  }

  override def compute(split: Split, context: TaskContext): Iterator[T] = {
    CheckpointRDD.readFromFile(split.asInstanceOf[CheckpointRDDSplit].splitFile, context)
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }
}

private[spark] object CheckpointRDD extends Logging {

  def splitIdToFileName(splitId: Int): String = {
    val numfmt = NumberFormat.getInstance()
    numfmt.setMinimumIntegerDigits(5)
    numfmt.setGroupingUsed(false)
    "part-"  + numfmt.format(splitId)
  }

  def writeToFile[T](path: String, blockSize: Int = -1)(context: TaskContext, iterator: Iterator[T]) {
    val outputDir = new Path(path)
    val fs = outputDir.getFileSystem(new Configuration())

    val finalOutputName = splitIdToFileName(context.splitId)
    val finalOutputPath = new Path(outputDir, finalOutputName)
    val tempOutputPath = new Path(outputDir, "." + finalOutputName + "-attempt-" + context.attemptId)

    if (fs.exists(tempOutputPath)) {
      throw new IOException("Checkpoint failed: temporary path " +
        tempOutputPath + " already exists")
    }
    val bufferSize = System.getProperty("spark.buffer.size", "65536").toInt

    val fileOutputStream = if (blockSize < 0) {
      fs.create(tempOutputPath, false, bufferSize)
    } else {
      // This is mainly for testing purpose
      fs.create(tempOutputPath, false, bufferSize, fs.getDefaultReplication, blockSize)
    }
    val serializer = SparkEnv.get.serializer.newInstance()
    val serializeStream = serializer.serializeStream(fileOutputStream)
    serializeStream.writeAll(iterator)
    fileOutputStream.close()

    if (!fs.rename(tempOutputPath, finalOutputPath)) {
      if (!fs.delete(finalOutputPath, true)) {
        throw new IOException("Checkpoint failed: failed to delete earlier output of task "
          + context.attemptId);
      }
      if (!fs.rename(tempOutputPath, finalOutputPath)) {
        throw new IOException("Checkpoint failed: failed to save output of task: "
          + context.attemptId)
      }
    }
  }

  def readFromFile[T](path: String, context: TaskContext): Iterator[T] = {
    val inputPath = new Path(path)
    val fs = inputPath.getFileSystem(new Configuration())
    val bufferSize = System.getProperty("spark.buffer.size", "65536").toInt
    val fileInputStream = fs.open(inputPath, bufferSize)
    val serializer = SparkEnv.get.serializer.newInstance()
    val deserializeStream = serializer.deserializeStream(fileInputStream)

    // Register an on-task-completion callback to close the input stream.
    context.addOnCompleteCallback(() => deserializeStream.close())

    deserializeStream.asIterator.asInstanceOf[Iterator[T]]
  }

  // Test whether CheckpointRDD generate expected number of splits despite
  // each split file having multiple blocks. This needs to be run on a
  // cluster (mesos or standalone) using HDFS.
  def main(args: Array[String]) {
    import spark._

    val Array(cluster, hdfsPath) = args
    val sc = new SparkContext(cluster, "CheckpointRDD Test")
    val rdd = sc.makeRDD(1 to 10, 10).flatMap(x => 1 to 10000)
    val path = new Path(hdfsPath, "temp")
    val fs = path.getFileSystem(new Configuration())
    sc.runJob(rdd, CheckpointRDD.writeToFile(path.toString, 10) _)
    val cpRDD = new CheckpointRDD[Int](sc, path.toString)
    assert(cpRDD.splits.length == rdd.splits.length, "Number of splits is not the same")
    assert(cpRDD.collect.toList == rdd.collect.toList, "Data of splits not the same")
    fs.delete(path)
  }
}
