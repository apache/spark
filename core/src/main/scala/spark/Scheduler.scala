package spark

// Scheduler trait, implemented by both NexusScheduler and LocalScheduler.
private trait Scheduler {
  def start()
  def waitForRegister()
  //def runTasks[T](tasks: Array[Task[T]])(implicit m: ClassManifest[T]): Array[T]
  def runJob[T, U](rdd: RDD[T], func: Iterator[T] => U)(implicit m: ClassManifest[U]): Array[U]
  def stop()
  def numCores(): Int
}
