package spark

import com.google.common.io.Files
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import java.io.{File, PrintWriter}
import SparkContext._

class FileServerSuite extends FunSuite with BeforeAndAfter {
  
  var sc: SparkContext = _
  var tmpFile : File = _ 
  var testJarFile : File = _ 
  
  before {
    // Create a sample text file
    val tmpdir = new File(Files.createTempDir(), "test")
    tmpdir.mkdir()
    tmpFile = new File(tmpdir, "FileServerSuite.txt")
    val pw = new PrintWriter(tmpFile)
    pw.println("100")
    pw.close()
  }
  
  after {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    // Clean up downloaded file
    if (tmpFile.exists) {
      tmpFile.delete()
    }
  }
  
  test("Distributing files") {
    sc = new SparkContext("local[4]", "test")
    sc.addFile(tmpFile.toString)
    val testData = Array((1,1), (1,1), (2,1), (3,5), (2,2), (3,0))
    val result = sc.parallelize(testData).reduceByKey {
      val in  = new java.io.BufferedReader(new java.io.FileReader(tmpFile))
      val fileVal = in.readLine().toInt
      in.close()
      _ * fileVal + _ * fileVal
    }.collect
    println(result)
    assert(result.toSet === Set((1,200), (2,300), (3,500)))
  }

  test ("Dynamically adding JARS") {
    sc = new SparkContext("local[4]", "test")
    val sampleJarFile = getClass().getClassLoader().getResource("uncommons-maths-1.2.2.jar").getFile()
    sc.addJar(sampleJarFile)
    val testData = Array((1,1), (1,1), (2,1), (3,5), (2,3), (3,0))
    val result = sc.parallelize(testData).reduceByKey { (x,y) => 
      val fac = Thread.currentThread.getContextClassLoader().loadClass("org.uncommons.maths.Maths").getDeclaredMethod("factorial", classOf[Int])
      val a = fac.invoke(null, x.asInstanceOf[java.lang.Integer]).asInstanceOf[Long].toInt
      val b = fac.invoke(null, y.asInstanceOf[java.lang.Integer]).asInstanceOf[Long].toInt
      a + b
    }.collect()
    assert(result.toSet === Set((1,2), (2,7), (3,121)))
  }
  
}