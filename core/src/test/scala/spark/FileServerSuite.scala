package spark

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import java.io.{File, PrintWriter}

class FileServerSuite extends FunSuite with BeforeAndAfter {
  
  var sc: SparkContext = _
  
  before {
    // Create a sample text file
    val pw = new PrintWriter(System.getProperty("java.io.tmpdir") + "FileServerSuite.txt")
    pw.println("100")
    pw.close()
  }
  
  after {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    // Clean up downloaded file
    val tmpFile = new File("FileServerSuite.txt")
    if (tmpFile.exists) {
      tmpFile.delete()
    }
  }
  
  test("Distributing files") {
    sc = new SparkContext("local[4]", "test")
    sc.addFile(System.getProperty("java.io.tmpdir") + "FileServerSuite.txt")
    val testRdd = sc.parallelize(List(1,2,3,4))
    val result = testRdd.map { x => 
      val in  = new java.io.BufferedReader(new java.io.FileReader("FileServerSuite.txt"))
      val fileVal = in.readLine().toInt
      in.close()
      fileVal
    }.reduce(_ + _)
    assert(result == 400)
  }
  
}