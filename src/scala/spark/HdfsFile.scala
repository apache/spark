package spark

import java.io._
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import java.util.HashSet

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

import nexus._

import com.google.common.collect.MapMaker

import org.apache.hadoop.io.ObjectWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter

@serializable
abstract class DistributedFile[T, Split](@transient sc: SparkContext) {
  def splits: Array[Split]
  def iterator(split: Split): Iterator[T]
  def prefers(split: Split, slot: SlaveOffer): Boolean

  def taskStarted(split: Split, slot: SlaveOffer) {}

  def sparkContext = sc

  def foreach(f: T => Unit) {
    val cleanF = sc.clean(f)
    val tasks = splits.map(s => new ForeachTask(this, s, cleanF)).toArray
    sc.runTaskObjects(tasks)
  }

  def toArray: Array[T] = {
    val tasks = splits.map(s => new GetTask(this, s))
    val results = sc.runTaskObjects(tasks)
    Array.concat(results: _*)
  }

  def reduce(f: (T, T) => T): T = {
    val cleanF = sc.clean(f)
    val tasks = splits.map(s => new ReduceTask(this, s, f))
    val results = new ArrayBuffer[T]
    for (option <- sc.runTaskObjects(tasks); elem <- option)
      results += elem
    if (results.size == 0)
      throw new UnsupportedOperationException("empty collection")
    else
      return results.reduceLeft(f)
  }

  def take(num: Int): Array[T] = {
    if (num == 0)
      return new Array[T](0)
    val buf = new ArrayBuffer[T]
    for (split <- splits; elem <- iterator(split)) {
      buf += elem
      if (buf.length == num)
        return buf.toArray
    }
    return buf.toArray
  }

  def first: T = take(1) match {
    case Array(t) => t
    case _ => throw new UnsupportedOperationException("empty collection")
  }

  def map[U](f: T => U) = new MappedFile(this, sc.clean(f))
  def filter(f: T => Boolean) = new FilteredFile(this, sc.clean(f))
  def cache() = new CachedFile(this)

  def count(): Long = 
    try { map(x => 1L).reduce(_+_) }
    catch { case e: UnsupportedOperationException => 0L }
}

@serializable
abstract class FileTask[U, T, Split](val file: DistributedFile[T, Split],
  val split: Split)
extends Task[U] {
  override def prefers(slot: SlaveOffer) = file.prefers(split, slot)
  override def markStarted(slot: SlaveOffer) { file.taskStarted(split, slot) }
}

class ForeachTask[T, Split](file: DistributedFile[T, Split],
  split: Split, func: T => Unit)
extends FileTask[Unit, T, Split](file, split) {
  override def run() {
    println("Processing " + split)
    file.iterator(split).foreach(func)
  }
}

class GetTask[T, Split](file: DistributedFile[T, Split], split: Split)
extends FileTask[Array[T], T, Split](file, split) {
  override def run(): Array[T] = {
    println("Processing " + split)
    file.iterator(split).collect.toArray
  }
}

class ReduceTask[T, Split](file: DistributedFile[T, Split],
  split: Split, f: (T, T) => T)
extends FileTask[Option[T], T, Split](file, split) {
  override def run(): Option[T] = {
    println("Processing " + split)
    val iter = file.iterator(split)
    if (iter.hasNext)
      Some(iter.reduceLeft(f))
    else
      None
  }
}

class MappedFile[U, T, Split](prev: DistributedFile[T, Split], f: T => U) 
extends DistributedFile[U, Split](prev.sparkContext) {
  override def splits = prev.splits
  override def prefers(split: Split, slot: SlaveOffer) = prev.prefers(split, slot)
  override def iterator(split: Split) = prev.iterator(split).map(f)
  override def taskStarted(split: Split, slot: SlaveOffer) = prev.taskStarted(split, slot)
}

class FilteredFile[T, Split](prev: DistributedFile[T, Split], f: T => Boolean) 
extends DistributedFile[T, Split](prev.sparkContext) {
  override def splits = prev.splits
  override def prefers(split: Split, slot: SlaveOffer) = prev.prefers(split, slot)
  override def iterator(split: Split) = prev.iterator(split).filter(f)
  override def taskStarted(split: Split, slot: SlaveOffer) = prev.taskStarted(split, slot)
}

class CachedFile[T, Split](prev: DistributedFile[T, Split])
extends DistributedFile[T, Split](prev.sparkContext) {
  val id = CachedFile.newId()
  @transient val cacheLocs = Map[Split, List[Int]]()

  override def splits = prev.splits

  override def prefers(split: Split, slot: SlaveOffer): Boolean = {
    if (cacheLocs.contains(split))
      cacheLocs(split).contains(slot.getSlaveId)
    else
      prev.prefers(split, slot)
  }
  
  override def iterator(split: Split): Iterator[T] = {
    val key = id + "::" + split.toString
    val cache = CachedFile.cache
    val loading = CachedFile.loading
    val cachedVal = cache.get(key)
    if (cachedVal != null) {
      // Split is in cache, so just return its values
      return Iterator.fromArray(cachedVal.asInstanceOf[Array[T]])
    } else {
      // Mark the split as loading (unless someone else marks it first)
      loading.synchronized {
        if (loading.contains(key)) {
          while (loading.contains(key)) {
            try {loading.wait()} catch {case _ =>}
          }
          return Iterator.fromArray(cache.get(key).asInstanceOf[Array[T]])
        } else {
          loading.add(key)
        }
      }
      // If we got here, we have to load the split
      println("Loading and caching " + split)
      val array = prev.iterator(split).collect.toArray
      cache.put(key, array)
      loading.synchronized {
        loading.remove(key)
        loading.notifyAll()
      }
      return Iterator.fromArray(array)
    }
  }

  override def taskStarted(split: Split, slot: SlaveOffer) {
    val oldList = cacheLocs.getOrElse(split, Nil)
    val slaveId = slot.getSlaveId
    if (!oldList.contains(slaveId))
      cacheLocs(split) = slaveId :: oldList
  }
}

private object CachedFile {
  val nextId = new AtomicLong(0) // Generates IDs for mapped files (on master)
  def newId() = nextId.getAndIncrement()

  // Stores map results for various splits locally (on workers)
  val cache = new MapMaker().softValues().makeMap[String, AnyRef]()

  // Remembers which splits are currently being loaded (on workers)
  val loading = new HashSet[String]
}

class HdfsSplit(@transient s: InputSplit)
extends SerializableWritable[InputSplit](s)

class HdfsTextFile(sc: SparkContext, path: String)
extends DistributedFile[String, HdfsSplit](sc) {
  @transient val conf = new JobConf()
  @transient val inputFormat = new TextInputFormat()

  FileInputFormat.setInputPaths(conf, path)
  ConfigureLock.synchronized { inputFormat.configure(conf) }

  @transient val splits_ =
    inputFormat.getSplits(conf, 2).map(new HdfsSplit(_)).toArray

  override def splits = splits_
  
  override def iterator(split: HdfsSplit) = new Iterator[String] {
    var reader: RecordReader[LongWritable, Text] = null
    ConfigureLock.synchronized {
      val conf = new JobConf()
      conf.set("io.file.buffer.size",
          System.getProperty("spark.buffer.size", "65536"))
      val tif = new TextInputFormat()
      tif.configure(conf) 
      reader = tif.getRecordReader(split.value, conf, Reporter.NULL)
    }
    val lineNum = new LongWritable()
    val text = new Text()
    var gotNext = false
    var finished = false

    override def hasNext: Boolean = {
      if (!gotNext) {
        finished = !reader.next(lineNum, text)
        gotNext = true
      }
      !finished
    }

    override def next: String = {
      if (!gotNext)
        finished = !reader.next(lineNum, text)
      if (finished)
        throw new java.util.NoSuchElementException("end of stream")
      gotNext = false
      text.toString
    }
  }

  override def prefers(split: HdfsSplit, slot: SlaveOffer) =
    split.value.getLocations().contains(slot.getHost)
}

object ConfigureLock {}

@serializable
class SerializableWritable[T <: Writable](@transient var t: T) {
  def value = t
  override def toString = t.toString

  private def writeObject(out: ObjectOutputStream) {
    out.defaultWriteObject()
    new ObjectWritable(t).write(out)
  }

  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    val ow = new ObjectWritable()
    ow.setConf(new JobConf())
    ow.readFields(in)
    t = ow.get().asInstanceOf[T]
  }
}
