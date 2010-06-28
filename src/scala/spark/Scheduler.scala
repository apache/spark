package spark

// Scheduler trait, implemented by both NexusScheduler and LocalScheduler.
private trait Scheduler {
  def start()
  def waitForRegister()
  def runTasks[T](tasks: Array[Task[T]])(implicit m: ClassManifest[T]): Array[T]
  def stop()
}
