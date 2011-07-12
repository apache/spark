package spark

import java.io.EOFException
import java.net.URL
import java.io.ObjectInputStream
import java.util.concurrent.atomic.AtomicLong
import java.util.HashSet
import java.util.Random
import java.util.Date

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.HadoopFileWriter
import org.apache.hadoop.mapred.OutputFormat
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.hadoop.mapred.OutputCommitter
import org.apache.hadoop.mapred.FileOutputCommitter
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.Text

import SparkContext._

/**
 * Extra functions available on RDDs of (key, value) pairs through an implicit conversion.
 */
@serializable
class PairRDDFunctions[K: ClassManifest, V: ClassManifest](self: RDD[(K, V)]) extends Logging {
  def reduceByKeyToDriver(func: (V, V) => V): Map[K, V] = {
    def mergeMaps(m1: HashMap[K, V], m2: HashMap[K, V]): HashMap[K, V] = {
      for ((k, v) <- m2) {
        m1.get(k) match {
          case None => m1(k) = v
          case Some(w) => m1(k) = func(w, v)
        }
      }
      return m1
    }
    self.map(pair => HashMap(pair)).reduce(mergeMaps)
  }

  def combineByKey[C](createCombiner: V => C,
                      mergeValue: (C, V) => C,
                      mergeCombiners: (C, C) => C,
                      numSplits: Int)
  : RDD[(K, C)] =
  {
    val aggregator = new Aggregator[K, V, C](createCombiner, mergeValue, mergeCombiners)
    val partitioner = new HashPartitioner(numSplits)
    new ShuffledRDD(self, aggregator, partitioner)
  }

  def reduceByKey(func: (V, V) => V, numSplits: Int): RDD[(K, V)] = {
    combineByKey[V]((v: V) => v, func, func, numSplits)
  }

  def groupByKey(numSplits: Int): RDD[(K, Seq[V])] = {
    def createCombiner(v: V) = ArrayBuffer(v)
    def mergeValue(buf: ArrayBuffer[V], v: V) = buf += v
    def mergeCombiners(b1: ArrayBuffer[V], b2: ArrayBuffer[V]) = b1 ++= b2
    val bufs = combineByKey[ArrayBuffer[V]](
      createCombiner _, mergeValue _, mergeCombiners _, numSplits)
    bufs.asInstanceOf[RDD[(K, Seq[V])]]
  }

  def join[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (V, W))] = {
    val vs: RDD[(K, Either[V, W])] = self.map { case (k, v) => (k, Left(v)) }
    val ws: RDD[(K, Either[V, W])] = other.map { case (k, w) => (k, Right(w)) }
    (vs ++ ws).groupByKey(numSplits).flatMap {
      case (k, seq) => {
        val vbuf = new ArrayBuffer[V]
        val wbuf = new ArrayBuffer[W]
        seq.foreach(_ match {
          case Left(v) => vbuf += v
          case Right(w) => wbuf += w
        })
        for (v <- vbuf; w <- wbuf) yield (k, (v, w))
      }
    }
  }

  def leftOuterJoin[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (V, Option[W]))] = {
    val vs: RDD[(K, Either[V, W])] = self.map { case (k, v) => (k, Left(v)) }
    val ws: RDD[(K, Either[V, W])] = other.map { case (k, w) => (k, Right(w)) }
    (vs ++ ws).groupByKey(numSplits).flatMap {
      case (k, seq) => {
        val vbuf = new ArrayBuffer[V]
        val wbuf = new ArrayBuffer[Option[W]]
        seq.foreach(_ match {
          case Left(v) => vbuf += v
          case Right(w) => wbuf += Some(w)
        })
        if (wbuf.isEmpty) {
          wbuf += None
        }
        for (v <- vbuf; w <- wbuf) yield (k, (v, w))
      }
    }
  }

  def rightOuterJoin[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (Option[V], W))] = {
    val vs: RDD[(K, Either[V, W])] = self.map { case (k, v) => (k, Left(v)) }
    val ws: RDD[(K, Either[V, W])] = other.map { case (k, w) => (k, Right(w)) }
    (vs ++ ws).groupByKey(numSplits).flatMap {
      case (k, seq) => {
        val vbuf = new ArrayBuffer[Option[V]]
        val wbuf = new ArrayBuffer[W]
        seq.foreach(_ match {
          case Left(v) => vbuf += Some(v)
          case Right(w) => wbuf += w
        })
        if (vbuf.isEmpty) {
          vbuf += None
        }
        for (v <- vbuf; w <- wbuf) yield (k, (v, w))
      }
    }
  }

  def combineByKey[C](createCombiner: V => C,
                      mergeValue: (C, V) => C,
                      mergeCombiners: (C, C) => C)
  : RDD[(K, C)] = {
    combineByKey(createCombiner, mergeValue, mergeCombiners, numCores)
  }

  def reduceByKey(func: (V, V) => V): RDD[(K, V)] = {
    reduceByKey(func, numCores)
  }

  def groupByKey(): RDD[(K, Seq[V])] = {
    groupByKey(numCores)
  }

  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = {
    join(other, numCores)
  }

  def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = {
    leftOuterJoin(other, numCores)
  }

  def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = {
    rightOuterJoin(other, numCores)
  }

  def numCores = self.context.numCores

  def collectAsMap(): Map[K, V] = HashMap(self.collect(): _*)
  
  def mapValues[U](f: V => U): RDD[(K, U)] = {
    val cleanF = self.context.clean(f)
    new MappedValuesRDD(self, cleanF)
  }
  
  def flatMapValues[U](f: V => Traversable[U]): RDD[(K, U)] = {
    val cleanF = self.context.clean(f)
    new FlatMappedValuesRDD(self, cleanF)
  }
  
  def groupWith[W](other: RDD[(K, W)]): RDD[(K, (Seq[V], Seq[W]))] = {
    val part = self.partitioner match {
      case Some(p) => p
      case None => new HashPartitioner(numCores)
    }
    new CoGroupedRDD[K](Seq(self.asInstanceOf[RDD[(_, _)]], other.asInstanceOf[RDD[(_, _)]]), part).map {
      case (k, Seq(vs, ws)) =>
        (k, (vs.asInstanceOf[Seq[V]], ws.asInstanceOf[Seq[W]]))
    }
  }
  
  def groupWith[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
      : RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    val part = self.partitioner match {
      case Some(p) => p
      case None => new HashPartitioner(numCores)
    }
    new CoGroupedRDD[K](
        Seq(self.asInstanceOf[RDD[(_, _)]], 
            other1.asInstanceOf[RDD[(_, _)]], 
            other2.asInstanceOf[RDD[(_, _)]]),
        part).map {
      case (k, Seq(vs, w1s, w2s)) =>
        (k, (vs.asInstanceOf[Seq[V]], w1s.asInstanceOf[Seq[W1]], w2s.asInstanceOf[Seq[W2]]))
    }
  }
  
  def saveAsHadoopFile (path: String, jobConf: JobConf) {
    saveAsHadoopFile(path, jobConf.getOutputKeyClass, jobConf.getOutputValueClass, jobConf.getOutputFormat().getClass.asInstanceOf[Class[OutputFormat[AnyRef,AnyRef]]], jobConf.getOutputCommitter().getClass.asInstanceOf[Class[OutputCommitter]], jobConf)
  }

  def saveAsHadoopFile [F <: OutputFormat[K,V], C <: OutputCommitter] (path: String) (implicit fm: ClassManifest[F], cm: ClassManifest[C]) {
    saveAsHadoopFile(path, fm.erasure.asInstanceOf[Class[F]], cm.erasure.asInstanceOf[Class[C]])
  }
  
  def saveAsHadoopFile(path: String, outputFormatClass: Class[_ <: OutputFormat[K,V]], outputCommitterClass: Class[_ <: OutputCommitter]) {
    saveAsHadoopFile(path,  implicitly[ClassManifest[K]].erasure, implicitly[ClassManifest[V]].erasure, outputFormatClass, outputCommitterClass)
  }

  def saveAsHadoopFile(path: String, keyClass: Class[_], valueClass: Class[_], outputFormatClass: Class[_ <: OutputFormat[_,_]], outputCommitterClass: Class[_ <: OutputCommitter]) {
    saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass, outputCommitterClass, null)
  }
  
  private def saveAsHadoopFile(path: String, keyClass: Class[_], valueClass: Class[_], outputFormatClass: Class[_ <: OutputFormat[_,_]], outputCommitterClass: Class[_ <: OutputCommitter], jobConf: JobConf) {
    logInfo ("Saving as hadoop file of type (" + keyClass.getSimpleName+ "," +valueClass.getSimpleName+ ")" ) 
    val writer = new HadoopFileWriter(path, 
                                      keyClass, 
                                      valueClass, 
                                      outputFormatClass.asInstanceOf[Class[OutputFormat[AnyRef,AnyRef]]],
                                      outputCommitterClass.asInstanceOf[Class[OutputCommitter]],
                                      null)
    writer.preSetup()

    def writeToFile (context: TaskContext, iter: Iterator[(K,V)]): HadoopFileWriter = {
      writer.setup(context.stageId, context.splitId, context.attemptId)
      writer.open()
      
      var count = 0
      while(iter.hasNext) {
        val record = iter.next
        count += 1
        writer.write(record._1.asInstanceOf[AnyRef], record._2.asInstanceOf[AnyRef])
      }
    
      writer.close()
      return writer
    }

    self.context.runJob(self, writeToFile _ ).foreach(_.commit())
    writer.cleanup()
  }

  def getKeyClass() = implicitly[ClassManifest[K]].erasure

  def getValueClass() = implicitly[ClassManifest[V]].erasure
}

class MappedValuesRDD[K, V, U](
  prev: RDD[(K, V)], f: V => U)
extends RDD[(K, U)](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override val partitioner = prev.partitioner
  override def compute(split: Split) =
    prev.iterator(split).map{case (k, v) => (k, f(v))}
}

class FlatMappedValuesRDD[K, V, U](
  prev: RDD[(K, V)], f: V => Traversable[U])
extends RDD[(K, U)](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override val partitioner = prev.partitioner
  override def compute(split: Split) = {
    prev.iterator(split).toStream.flatMap { 
      case (k, v) => f(v).map(x => (k, x))
    }.iterator
  }
}
