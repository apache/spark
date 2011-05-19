package spark

// Scheduler trait, implemented by both NexusScheduler and LocalScheduler.
private trait Scheduler {
  def start()

  def waitForRegister()

  // Run a function on some partitions of an RDD, returning an array of results.
  def runJob[T, U](rdd: RDD[T], func: Iterator[T] => U, partitions: Seq[Int])
                  (implicit m: ClassManifest[U]): Array[U]

  def stop()

  def numCores(): Int
}
