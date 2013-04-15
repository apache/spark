package spark.repl

import java.io.BufferedReader
import java.io.PrintWriter
import java.io.StringReader
import java.io.StringWriter
import java.net.URLClassLoader

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.future

import spark.deploy.master.Master
import spark.deploy.worker.Worker

trait ReplSuiteMixin {
  def setupStandaloneCluster() {
    future { Master.main(Array("-i", "127.0.1.2", "-p", "7089")) }
    Thread.sleep(2000)
    future { Worker.main(Array("spark://127.0.1.2:7089", "--webui-port", "0")) }
  }
  
  def runInterpreter(master: String, input: String): String = {
    val in = new BufferedReader(new StringReader(input + "\n"))
    val out = new StringWriter()
    val cl = getClass.getClassLoader
    var paths = new ArrayBuffer[String]
    if (cl.isInstanceOf[URLClassLoader]) {
      val urlLoader = cl.asInstanceOf[URLClassLoader]
      for (url <- urlLoader.getURLs) {
        if (url.getProtocol == "file") {
          paths += url.getFile
        }
      }
    }
    val interp = new SparkILoop(in, new PrintWriter(out), master)
    spark.repl.Main.interp = interp
    val separator = System.getProperty("path.separator")
    interp.process(Array("-classpath", paths.mkString(separator)))
    if (interp != null)
      interp.closeInterpreter();
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port")
    return out.toString
  }

  def assertContains(message: String, output: String) {
    assert(output contains message,
      "Interpreter output did not contain '" + message + "':\n" + output)
  }

  def assertDoesNotContain(message: String, output: String) {
    assert(!(output contains message),
      "Interpreter output contained '" + message + "':\n" + output)
  }
}