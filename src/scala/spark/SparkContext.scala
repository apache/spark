package spark

import java.io._
import java.util.UUID

import scala.collection.mutable.ArrayBuffer

class SparkContext(master: String, frameworkName: String) {
  Broadcast.initialize(true)

  def parallelize[T: ClassManifest](seq: Seq[T], numSlices: Int) =
    new ParallelArray[T](this, seq, numSlices)

  def parallelize[T: ClassManifest](seq: Seq[T]): ParallelArray[T] =
    parallelize(seq, scheduler.numCores)

  def accumulator[T](initialValue: T)(implicit param: AccumulatorParam[T]) =
    new Accumulator(initialValue, param)

  // TODO: Keep around a weak hash map of values to Cached versions?
  def broadcast[T](value: T) = new CentralizedHDFSBroadcast(value, local)
  //def broadcast[T](value: T) = new ChainedStreamingBroadcast(value, local)

  def textFile(path: String) = new HdfsTextFile(this, path)

  val LOCAL_REGEX = """local\[([0-9]+)\]""".r

  private var scheduler: Scheduler = master match {
    case "local" => new LocalScheduler(1)
    case LOCAL_REGEX(threads) => new LocalScheduler(threads.toInt)
    case _ => { System.loadLibrary("mesos");
                new MesosScheduler(master, frameworkName, createExecArg()) }
  }

  private val local = scheduler.isInstanceOf[LocalScheduler]  

  scheduler.start()

  private def createExecArg(): Array[Byte] = {
    // Our executor arg is an array containing all the spark.* system properties
    val props = new ArrayBuffer[(String, String)]
    val iter = System.getProperties.entrySet.iterator
    while (iter.hasNext) {
      val entry = iter.next
      val (key, value) = (entry.getKey.toString, entry.getValue.toString)
      if (key.startsWith("spark."))
        props += key -> value
    }
    return Utils.serialize(props.toArray)
  }

  def runTasks[T: ClassManifest](tasks: Array[() => T]): Array[T] = {
    runTaskObjects(tasks.map(f => new FunctionTask(f)))
  }

  private[spark] def runTaskObjects[T: ClassManifest](tasks: Seq[Task[T]])
      : Array[T] = {
    println("Running " + tasks.length + " tasks in parallel")
    val start = System.nanoTime
    val result = scheduler.runTasks(tasks.toArray)
    println("Tasks finished in " + (System.nanoTime - start) / 1e9 + " s")
    return result
  }

  def stop() { 
     scheduler.stop()
     scheduler = null
  }
  
  def waitForRegister() {
    scheduler.waitForRegister()
  }
  
  // Clean a closure to make it ready to serialized and send to tasks
  // (removes unreferenced variables in $outer's, updates REPL variables)
  private[spark] def clean[F <: AnyRef](f: F): F = {
    ClosureCleaner.clean(f)
    return f
  }
}

object SparkContext {
  implicit object DoubleAccumulatorParam extends AccumulatorParam[Double] {
    def add(t1: Double, t2: Double): Double = t1 + t2
    def zero(initialValue: Double) = 0.0
  }
  implicit object IntAccumulatorParam extends AccumulatorParam[Int] {
    def add(t1: Int, t2: Int): Int = t1 + t2
    def zero(initialValue: Int) = 0
  }
  // TODO: Add AccumulatorParams for other types, e.g. lists and strings
}
