package org.apache.spark.util

import java.util.EventListener

import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 *
 * Listener providing a callback function to invoke when a task's execution completes.
 */
@DeveloperApi
trait TaskCompletionListener extends EventListener {
  def onTaskCompletion(context: TaskContext)
}
