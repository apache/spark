package spark.streaming

import spark.Utils

import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.conf.Configuration

import java.io.{ObjectInputStream, ObjectOutputStream}

class Checkpoint(@transient ssc: StreamingContext) extends Serializable {
  val master = ssc.sc.master
  val frameworkName = ssc.sc.frameworkName
  val sparkHome = ssc.sc.sparkHome
  val jars = ssc.sc.jars
  val graph = ssc.graph
  val batchDuration = ssc.batchDuration
  val checkpointFile = ssc.checkpointFile
  val checkpointInterval = ssc.checkpointInterval

  def saveToFile(file: String) {
    val path = new Path(file)
    val conf = new Configuration()
    val fs = path.getFileSystem(conf)
    if (fs.exists(path)) {
      val bkPath = new Path(path.getParent, path.getName + ".bk")
      FileUtil.copy(fs, path, fs, bkPath, true, true, conf)
      println("Moved existing checkpoint file to " + bkPath)
    }
    val fos = fs.create(path)
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(this)
    oos.close()
    fs.close()
  }

  def toBytes(): Array[Byte] = {
    val cp = new Checkpoint(ssc)
    val bytes = Utils.serialize(cp)
    bytes
  }
}

object Checkpoint {

  def loadFromFile(file: String): Checkpoint = {
    val path = new Path(file)
    val conf = new Configuration()
    val fs = path.getFileSystem(conf)
    if (!fs.exists(path)) {
      throw new Exception("Could not read checkpoint file " + path)
    }
    val fis = fs.open(path)
    val ois = new ObjectInputStream(fis)
    val cp = ois.readObject.asInstanceOf[Checkpoint]
    ois.close()
    fs.close()
    cp
  }

  def fromBytes(bytes: Array[Byte]): Checkpoint = {
    Utils.deserialize[Checkpoint](bytes)
  }

  /*def toBytes(ssc: StreamingContext): Array[Byte] = {
    val cp = new Checkpoint(ssc)
    val bytes = Utils.serialize(cp)
    bytes
  }


  def saveContext(ssc: StreamingContext, file: String) {
    val cp = new Checkpoint(ssc)
    val path = new Path(file)
    val conf = new Configuration()
    val fs = path.getFileSystem(conf)
    if (fs.exists(path)) {
      val bkPath = new Path(path.getParent, path.getName + ".bk")
      FileUtil.copy(fs, path, fs, bkPath, true, true, conf)
      println("Moved existing checkpoint file to " + bkPath)
    }
    val fos = fs.create(path)
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(cp)
    oos.close()
    fs.close()
  }

  def loadContext(file: String): StreamingContext = {
    loadCheckpoint(file).createNewContext()
  }
  */
}
