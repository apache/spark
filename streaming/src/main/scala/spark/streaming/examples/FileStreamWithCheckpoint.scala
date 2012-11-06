package spark.streaming.examples

import spark.streaming._
import spark.streaming.StreamingContext._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

object FileStreamWithCheckpoint {

  def main(args: Array[String]) {

    if (args.size != 3) {
      println("FileStreamWithCheckpoint <master> <directory> <checkpoint dir>")
      println("FileStreamWithCheckpoint restart <directory> <checkpoint dir>")
      System.exit(-1)
    }

    val directory = new Path(args(1))
    val checkpointDir = args(2)

    val ssc: StreamingContext = {

      if (args(0) == "restart") {

        // Recreated streaming context from specified checkpoint file
        new StreamingContext(checkpointDir)

      } else {

        // Create directory if it does not exist
        val fs = directory.getFileSystem(new Configuration())
        if (!fs.exists(directory)) fs.mkdirs(directory)

        // Create new streaming context
        val ssc_ = new StreamingContext(args(0), "FileStreamWithCheckpoint")
        ssc_.setBatchDuration(Seconds(1))
        ssc_.checkpoint(checkpointDir, Seconds(1))

        // Setup the streaming computation
        val inputStream = ssc_.textFileStream(directory.toString)
        val words = inputStream.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
        wordCounts.print()

        ssc_
      }
    }

    // Start the stream computation
    startFileWritingThread(directory.toString)
    ssc.start()
  }

  def startFileWritingThread(directory: String) {

    val fs = new Path(directory).getFileSystem(new Configuration())

    val fileWritingThread = new Thread() {
      override def run() {
        val r = new scala.util.Random()
        val text = "This is a sample text file with a random number "
        while(true) {
          val number = r.nextInt()
          val file = new Path(directory, number.toString)
          val fos = fs.create(file)
          fos.writeChars(text + number)
          fos.close()
          println("Created text file " + file)
          Thread.sleep(1000)
        }
      }
    }
    fileWritingThread.start()
  }

}
