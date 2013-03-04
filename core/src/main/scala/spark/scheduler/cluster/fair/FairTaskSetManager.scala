package spark.scheduler.cluster.fair

import scala.collection.mutable.ArrayBuffer

import spark._
import spark.scheduler._
import spark.scheduler.cluster._
import spark.TaskState.TaskState
import java.nio.ByteBuffer

/**
 * Schedules the tasks within a single TaskSet in the FairClusterScheduler.
 */
private[spark] class FairTaskSetManager(sched: FairClusterScheduler, override val taskSet: TaskSet) extends TaskSetManager(sched, taskSet) with Logging {

  // Add a task to all the pending-task lists that it should be on.
  private def addPendingTask(index: Int) {
    val locations = tasks(index).preferredLocations.toSet & sched.hostsAlive
    if (locations.size == 0) {
      pendingTasksWithNoPrefs += index
    } else {
      for (host <- locations) {
        val list = pendingTasksForHost.getOrElseUpdate(host, ArrayBuffer())
        list += index
      }
    }
    allPendingTasks += index
  }  
  
  override def taskFinished(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    val info = taskInfos(tid)
    if (info.failed) {
      // We might get two task-lost messages for the same task in coarse-grained Mesos mode,
      // or even from Mesos itself when acks get delayed.
      return
    }
    val index = info.index
    info.markSuccessful()
    sched.taskFinished(this)
    if (!finished(index)) {
      tasksFinished += 1
      logInfo("Finished TID %s in %d ms (progress: %d/%d)".format(
        tid, info.duration, tasksFinished, numTasks))
      // Deserialize task result and pass it to the scheduler
      val result = ser.deserialize[TaskResult[_]](serializedData, getClass.getClassLoader)
      result.metrics.resultSize = serializedData.limit()
      sched.listener.taskEnded(tasks(index), Success, result.value, result.accumUpdates, info, result.metrics)      
      // Mark finished and stop if we've finished all the tasks
      finished(index) = true
      if (tasksFinished == numTasks) {
        sched.taskSetFinished(this)
      }
    } else {
      logInfo("Ignoring task-finished event for TID " + tid +
        " because task " + index + " is already finished")
    }
  }

  override def taskLost(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    val info = taskInfos(tid)
    if (info.failed) {
      // We might get two task-lost messages for the same task in coarse-grained Mesos mode,
      // or even from Mesos itself when acks get delayed.
      return
    }
    val index = info.index
    info.markFailed()
    //Bookkeeping necessary for the pools in the scheduler
    sched.taskFinished(this)
    if (!finished(index)) {
      logInfo("Lost TID %s (task %s:%d)".format(tid, taskSet.id, index))
      copiesRunning(index) -= 1      
      // Check if the problem is a map output fetch failure. In that case, this
      // task will never succeed on any node, so tell the scheduler about it.
      if (serializedData != null && serializedData.limit() > 0) {
        val reason = ser.deserialize[TaskEndReason](serializedData, getClass.getClassLoader)
        reason match {
          case fetchFailed: FetchFailed =>
            logInfo("Loss was due to fetch failure from " + fetchFailed.bmAddress)
            sched.listener.taskEnded(tasks(index), fetchFailed, null, null, info, null)
            finished(index) = true
            tasksFinished += 1
            sched.taskSetFinished(this)
            return

          case ef: ExceptionFailure =>
            val key = ef.exception.toString
            val now = System.currentTimeMillis
            val (printFull, dupCount) = {
              if (recentExceptions.contains(key)) {
                val (dupCount, printTime) = recentExceptions(key)
                if (now - printTime > EXCEPTION_PRINT_INTERVAL) {
                  recentExceptions(key) = (0, now)
                  (true, 0)
                } else {
                  recentExceptions(key) = (dupCount + 1, printTime)
                  (false, dupCount + 1)
                }
              } else {
                recentExceptions(key) = (0, now)
                (true, 0)
              }
            }
            if (printFull) {
              val locs = ef.exception.getStackTrace.map(loc => "\tat %s".format(loc.toString))
              logInfo("Loss was due to %s\n%s".format(ef.exception.toString, locs.mkString("\n")))
            } else {
              logInfo("Loss was due to %s [duplicate %d]".format(ef.exception.toString, dupCount))
            }

          case _ => {}
        }
      }
      // On non-fetch failures, re-enqueue the task as pending for a max number of retries
      addPendingTask(index)
      // Count failed attempts only on FAILED and LOST state (not on KILLED)
      if (state == TaskState.FAILED || state == TaskState.LOST) {
        numFailures(index) += 1
        if (numFailures(index) > MAX_TASK_FAILURES) {
          logError("Task %s:%d failed more than %d times; aborting job".format(
            taskSet.id, index, MAX_TASK_FAILURES))
          abort("Task %s:%d failed more than %d times".format(taskSet.id, index, MAX_TASK_FAILURES))
        }
      }
    } else {
      logInfo("Ignoring task-lost event for TID " + tid +
        " because task " + index + " is already finished")
    }
  }  
}