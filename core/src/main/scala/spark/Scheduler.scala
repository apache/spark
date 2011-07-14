package spark

// Scheduler trait, implemented by both MesosScheduler and LocalScheduler.
private trait Scheduler {
  def start()

  // Wait for registration with Mesos.
  def waitForRegister()

  // Run a function on some partitions of an RDD, returning an array of results. The allowLocal flag specifies
  // whether the scheduler is allowed to run the job on the master machine rather than shipping it to the cluster,
  // for actions that create short jobs such as first() and take().
  def runJob[T, U: ClassManifest](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U,
                                  partitions: Seq[Int], allowLocal: Boolean): Array[U]

  def stop()

  // Get the default level of parallelism to use in the cluster, as a hint for sizing jobs.
  def defaultParallelism(): Int
}
