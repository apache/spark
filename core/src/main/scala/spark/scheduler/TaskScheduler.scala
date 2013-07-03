package spark.scheduler

/**
 * Low-level task scheduler interface, implemented by both ClusterScheduler and LocalScheduler.
 * These schedulers get sets of tasks submitted to them from the DAGScheduler for each stage,
 * and are responsible for sending the tasks to the cluster, running them, retrying if there
 * are failures, and mitigating stragglers. They return events to the DAGScheduler through
 * the TaskSchedulerListener interface.
 */
private[spark] trait TaskScheduler {
  def start(): Unit

  // Invoked after system has successfully initialized (typically in spark context).
  // Yarn uses this to bootstrap allocation of resources based on preferred locations, wait for slave registerations, etc.
  def postStartHook() { }

  // Disconnect from the cluster.
  def stop(): Unit

  // Submit a sequence of tasks to run.
  def submitTasks(taskSet: TaskSet): Unit

  // Set a listener for upcalls. This is guaranteed to be set before submitTasks is called.
  def setListener(listener: TaskSchedulerListener): Unit

  // Get the default level of parallelism to use in the cluster, as a hint for sizing jobs.
  def defaultParallelism(): Int
}
