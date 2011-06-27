package spark

// Scheduler trait, implemented by both MesosScheduler and LocalScheduler.
private trait Scheduler {
  def start()

  // Wait for registration with Mesos.
  def waitForRegister()

  // Run a function on some partitions of an RDD, returning an array of results.
  def runJob[T, U](rdd: RDD[T], func: Iterator[T] => U, partitions: Seq[Int])
                  (implicit m: ClassManifest[U]): Array[U] = 
                  runJob(rdd, (context: TaskContext, iter: Iterator[T]) => func(iter), partitions)

  // Run a function on some partitions of an RDD, returning an array of results.
  def runJob[T, U](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U, partitions: Seq[Int])
                  (implicit m: ClassManifest[U]): Array[U]

  def stop()

  // Get the number of cores in the cluster, as a hint for sizing jobs.
  def numCores(): Int
}
