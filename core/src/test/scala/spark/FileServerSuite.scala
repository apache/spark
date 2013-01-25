package spark

import com.google.common.io.Files
import org.scalatest.FunSuite
import java.io.{File, PrintWriter, FileReader, BufferedReader}
import SparkContext._

class FileServerSuite extends FunSuite with LocalSparkContext {

  @transient var tmpFile: File = _
  @transient var testJarFile: File = _

  override def beforeEach() {
    super.beforeEach()
    // Create a sample text file
    val tmpdir = new File(Files.createTempDir(), "test")
    tmpdir.mkdir()
    tmpFile = new File(tmpdir, "FileServerSuite.txt")
    val pw = new PrintWriter(tmpFile)
    pw.println("100")
    pw.close()
  }

  override def afterEach() {
    super.afterEach()
    // Clean up downloaded file
    if (tmpFile.exists) {
      tmpFile.delete()
    }
  }

  test("Distributing files locally") {
    sc = new SparkContext("local[4]", "test")
    sc.addFile(tmpFile.toString)
    val testData = Array((1,1), (1,1), (2,1), (3,5), (2,2), (3,0))
    val result = sc.parallelize(testData).reduceByKey {
      val path = SparkFiles.get("FileServerSuite.txt")
      val in  = new BufferedReader(new FileReader(path))
      val fileVal = in.readLine().toInt
      in.close()
      _ * fileVal + _ * fileVal
    }.collect
    assert(result.toSet === Set((1,200), (2,300), (3,500)))
  }

  test("Distributing files locally using URL as input") {
    // addFile("file:///....")
    sc = new SparkContext("local[4]", "test")
    sc.addFile((new File(tmpFile.toString)).toURL.toString)
    val testData = Array((1,1), (1,1), (2,1), (3,5), (2,2), (3,0))
    val result = sc.parallelize(testData).reduceByKey {
      val path = SparkFiles.get("FileServerSuite.txt")
      val in  = new BufferedReader(new FileReader(path))
      val fileVal = in.readLine().toInt
      in.close()
      _ * fileVal + _ * fileVal
    }.collect
    assert(result.toSet === Set((1,200), (2,300), (3,500)))
  }

  test ("Dynamically adding JARS locally") {
    sc = new SparkContext("local[4]", "test")
    val sampleJarFile = getClass.getClassLoader.getResource("uncommons-maths-1.2.2.jar").getFile()
    sc.addJar(sampleJarFile)
    val testData = Array((1,1), (1,1), (2,1), (3,5), (2,3), (3,0))
    val result = sc.parallelize(testData).reduceByKey { (x,y) =>
      val fac = Thread.currentThread.getContextClassLoader()
                                    .loadClass("org.uncommons.maths.Maths")
                                    .getDeclaredMethod("factorial", classOf[Int])
      val a = fac.invoke(null, x.asInstanceOf[java.lang.Integer]).asInstanceOf[Long].toInt
      val b = fac.invoke(null, y.asInstanceOf[java.lang.Integer]).asInstanceOf[Long].toInt
      a + b
    }.collect()
    assert(result.toSet === Set((1,2), (2,7), (3,121)))
  }

  test("Distributing files on a standalone cluster") {
    sc = new SparkContext("local-cluster[1,1,512]", "test")
    sc.addFile(tmpFile.toString)
    val testData = Array((1,1), (1,1), (2,1), (3,5), (2,2), (3,0))
    val result = sc.parallelize(testData).reduceByKey {
      val path = SparkFiles.get("FileServerSuite.txt")
      val in  = new BufferedReader(new FileReader(path))
      val fileVal = in.readLine().toInt
      in.close()
      _ * fileVal + _ * fileVal
    }.collect
    println(result)
    assert(result.toSet === Set((1,200), (2,300), (3,500)))
  }

  test ("Dynamically adding JARS on a standalone cluster") {
    sc = new SparkContext("local-cluster[1,1,512]", "test")
    val sampleJarFile = getClass.getClassLoader.getResource("uncommons-maths-1.2.2.jar").getFile()
    sc.addJar(sampleJarFile)
    val testData = Array((1,1), (1,1), (2,1), (3,5), (2,3), (3,0))
    val result = sc.parallelize(testData).reduceByKey { (x,y) =>
      val fac = Thread.currentThread.getContextClassLoader()
                                    .loadClass("org.uncommons.maths.Maths")
                                    .getDeclaredMethod("factorial", classOf[Int])
      val a = fac.invoke(null, x.asInstanceOf[java.lang.Integer]).asInstanceOf[Long].toInt
      val b = fac.invoke(null, y.asInstanceOf[java.lang.Integer]).asInstanceOf[Long].toInt
      a + b
    }.collect()
    assert(result.toSet === Set((1,2), (2,7), (3,121)))
  }
}
