package spark.streaming

import spark.Utils

import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.conf.Configuration

import java.io.{InputStream, ObjectStreamClass, ObjectInputStream, ObjectOutputStream}


class Checkpoint(@transient ssc: StreamingContext, val checkpointTime: Time) extends Serializable {
  val master = ssc.sc.master
  val framework = ssc.sc.frameworkName
  val sparkHome = ssc.sc.sparkHome
  val jars = ssc.sc.jars
  val graph = ssc.graph
  val checkpointFile = ssc.checkpointFile
  val checkpointInterval = ssc.checkpointInterval

  validate()

  def validate() {
    assert(master != null, "Checkpoint.master is null")
    assert(framework != null, "Checkpoint.framework is null")
    assert(graph != null, "Checkpoint.graph is null")
    assert(checkpointTime != null, "Checkpoint.checkpointTime is null")
  }

  def saveToFile(file: String = checkpointFile) {
    val path = new Path(file)
    val conf = new Configuration()
    val fs = path.getFileSystem(conf)
    if (fs.exists(path)) {
      val bkPath = new Path(path.getParent, path.getName + ".bk")
      FileUtil.copy(fs, path, fs, bkPath, true, true, conf)
      //logInfo("Moved existing checkpoint file to " + bkPath)
    }
    val fos = fs.create(path)
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(this)
    oos.close()
    fs.close()
  }

  def toBytes(): Array[Byte] = {
    val bytes = Utils.serialize(this)
    bytes
  }
}

object Checkpoint {

  def loadFromFile(file: String): Checkpoint = {
    try {
      val path = new Path(file)
      val conf = new Configuration()
      val fs = path.getFileSystem(conf)
      if (!fs.exists(path)) {
        throw new Exception("Checkpoint file '" + file + "' does not exist")
      }
      val fis = fs.open(path)
      // ObjectInputStream uses the last defined user-defined class loader in the stack
      // to find classes, which maybe the wrong class loader. Hence, a inherited version
      // of ObjectInputStream is used to explicitly use the current thread's default class
      // loader to find and load classes. This is a well know Java issue and has popped up
      // in other places (e.g., http://jira.codehaus.org/browse/GROOVY-1627)
      val ois = new ObjectInputStreamWithLoader(fis, Thread.currentThread().getContextClassLoader)
      val cp = ois.readObject.asInstanceOf[Checkpoint]
      ois.close()
      fs.close()
      cp.validate()
      cp
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new Exception("Could not load checkpoint file '" + file + "'", e)
    }
  }

  def fromBytes(bytes: Array[Byte]): Checkpoint = {
    val cp = Utils.deserialize[Checkpoint](bytes)
    cp.validate()
    cp
  }
}

class ObjectInputStreamWithLoader(inputStream_ : InputStream, loader: ClassLoader) extends ObjectInputStream(inputStream_) {
  override def resolveClass(desc: ObjectStreamClass): Class[_] = {
    try {
      return loader.loadClass(desc.getName())
    } catch {
      case e: Exception =>
    }
    return super.resolveClass(desc)
  }
}
