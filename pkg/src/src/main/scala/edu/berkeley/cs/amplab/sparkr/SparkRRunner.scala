package edu.berkeley.cs.amplab.sparkr

import java.io._
import java.net.URI
import java.util.concurrent.Semaphore

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import org.apache.hadoop.fs.Path

/**
 * Main class used to launch SparkR applications using spark-submit. It executes R as a
 * subprocess and then has it connect back to the JVM to access system properties etc.
 */
object SparkRRunner {
  def main(args: Array[String]) {
    val rFile = args(0)

    val otherArgs = args.slice(1, args.length)

    // TODO: Can we get this from SparkConf ?
    val sparkRBackendPort = sys.env.getOrElse("SPARKR_BACKEND_PORT", "12345").toInt
    val rCommand = "Rscript"

    // Check if the file path exists.
    // If not, change directory to current working directory for YARN cluster mode
    val rF = new File(rFile)
    val rFileNormalized = if (!rF.exists()) {
      new Path(rFile).getName
    } else {
      rFile
    }


    // Launch a SparkR backend server for the R process to connect to; this will let it see our
    // Java system properties etc.
    val sparkRBackend = new SparkRBackend()
    val sparkRBackendThread = new Thread() {
      val finishedInit = new Semaphore(0)

      override def run() {
        sparkRBackend.init(sparkRBackendPort)
        finishedInit.release()
        sparkRBackend.run()
      }

      def stopBackend() {
        sparkRBackend.close()
      }
    }

    sparkRBackendThread.start()
    // Wait for SparkRBackend initialization to finish
    sparkRBackendThread.finishedInit.acquire()

    // Launch R
    val builder = new ProcessBuilder(Seq(rCommand, rFileNormalized) ++ otherArgs)
    val env = builder.environment()
    env.put("EXISTING_SPARKR_BACKEND_PORT", sparkRBackendPort.toString)
    builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
    val process = builder.start()

    new RedirectThread(process.getInputStream, System.out, "redirect output").start()

    val returnCode = process.waitFor()
    sparkRBackendThread.stopBackend()
    System.exit(returnCode)
  }

  private class RedirectThread(
      in: InputStream,
      out: OutputStream,
      name: String,
      propagateEof: Boolean = false)
    extends Thread(name) {

    setDaemon(true)
    override def run() {
      // FIXME: We copy the stream on the level of bytes to avoid encoding problems.
      try {
        val buf = new Array[Byte](1024)
        var len = in.read(buf)
        while (len != -1) {
          out.write(buf, 0, len)
          out.flush()
          len = in.read(buf)
        }
      } finally {
        if (propagateEof) {
          out.close()
        }
      }
    }
  }
}
