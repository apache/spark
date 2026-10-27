/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io.Closeable
import java.util.{Properties, Stack}
import java.util.concurrent.TimeUnit
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{LISTENER, TASK_ATTEMPT_ID, THREAD_NAME, TOTAL_TIME}
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util._


/**
 * A [[TaskContext]] implementation.
 *
 * A small note on thread safety. The interrupted & fetchFailed fields are volatile, this makes
 * sure that updates are always visible across threads. The complete & failed flags and their
 * callbacks are protected by locking on the context instance. For instance, this ensures
 * that you cannot add a completion listener in one thread while we are completing in another
 * thread. Other state is immutable, however the exposed `TaskMetrics` & `MetricsSystem` objects are
 * not thread safe.
 */
private[spark] class TaskContextImpl(
    override val stageId: Int,
    override val stageAttemptNumber: Int,
    override val partitionId: Int,
    override val taskAttemptId: Long,
    override val attemptNumber: Int,
    override val numPartitions: Int,
    override val taskMemoryManager: TaskMemoryManager,
    localProperties: Properties,
    @transient private val metricsSystem: MetricsSystem,
    // The default value is only used in tests.
    override val taskMetrics: TaskMetrics = TaskMetrics.empty,
    override val cpuAmount: BigDecimal = SparkEnv.get.conf.get(config.CPUS_PER_TASK),
    override val resources: Map[String, ResourceInformation] = Map.empty)
  extends TaskContext
  with Logging {

  import TaskContextImpl._

  /**
   * List of callback functions to execute when the task completes.
   *
   * Using a stack causes us to process listeners in reverse order of registration. As listeners are
   * invoked, they are popped from the stack.
   */
  @transient private val onCompleteCallbacks = new Stack[TaskCompletionListener]

  /**
   * List of callback functions to execute when the task completes, before any of
   * `onCompleteCallbacks`. `markTaskCompleted()` runs them on its own thread before it sets
   * `completed`, in reverse order of registration, as they are popped from the stack.
   */
  @transient private val onPreCompleteCallbacks = new Stack[TaskCompletionListener]

  /** List of callback functions to execute when the task fails. */
  @transient private val onFailureCallbacks = new Stack[TaskFailureListener]

  /** List of callback functions to execute when the task is interrupted. */
  @transient private val onInterruptCallbacks = new Stack[TaskInterruptListener]

  /**
   * List of callback functions to invoke after the task's status update has been sent
   * to the driver. Used for operations like push-based shuffle block push that should only
   * begin after the driver has been notified of the task result.
   */
  @transient private val onPostStatusUpdateCallbacks = new Stack[PostStatusUpdateListener]

  /**
   * The thread currently executing task completion or failure listeners, if any.
   *
   * `invokeListeners()` uses this to ensure listeners are called sequentially. Protected by the
   * TaskContext lock.
   */
  @transient private var listenerInvocationThread: Option[Thread] = None

  /**
   * A thread waiting in `markTaskCompleted()` for another thread to stop invoking listeners. That
   * thread hands the listener invocation directly to this one, so that no third thread can take
   * it over in between. Protected by the TaskContext lock.
   */
  @transient private var nextListenerInvocationThread: Option[Thread] = None

  /** How often `markTaskCompleted()` logs while it waits for another thread invoking listeners. */
  @transient @volatile private[spark] var listenerHandoverWarnIntervalMs: Long =
    DEFAULT_LISTENER_HANDOVER_WARN_INTERVAL_MS

  // If defined, the corresponding task has been killed and this option contains the reason.
  @volatile private var reasonIfKilled: Option[String] = None

  // The pending interruption request, which is blocked by uninterruptible resource creation.
  // Should be protected by `TaskContext.synchronized`.
  private var pendingInterruptRequest: Option[(Option[Thread], String)] = None

  // Whether this task is able to be interrupted. Should be protected by `TaskContext.synchronized`.
  private var _interruptible = true

  // Whether `markTaskCompleted()` has been called. It sets `completed` once the pre-completion
  // listeners have run.
  private var completionStarted: Boolean = false

  // Whether the task has completed.
  private var completed: Boolean = false

  // If defined, the task has failed and this option contains the Throwable that caused the task to
  // fail.
  private var failureCauseOpt: Option[Throwable] = None

  // If there was a fetch failure in the task, we store it here, to make sure user-code doesn't
  // hide the exception.  See SPARK-19276
  @volatile private var _fetchFailedException: Option[FetchFailedException] = None

  override def addTaskCompletionListener(listener: TaskCompletionListener): this.type = {
    val needToCallListener = synchronized {
      // If there is already a thread invoking listeners, the call to
      // `invokeTaskCompletionListeners()` below will be a no-op. If that thread runs the completion
      // listeners in `markTaskCompleted()`, or hands the invocation over to a thread waiting to do
      // so, the new listener runs there. Otherwise it is not run until a listener is added while no
      // thread is invoking listeners (see `invokeListeners()`).
      //
      // If there is no such thread, the call to `invokeTaskCompletionListeners()` below will
      // execute all listeners, including the new listener.
      onCompleteCallbacks.push(listener)
      completed
    }
    if (needToCallListener) {
      invokeTaskCompletionListeners(None, Claim)
    }
    this
  }

  override private[spark] def addTaskPreCompletionListener(
      listener: TaskCompletionListener): this.type = {
    val completedAlready = synchronized {
      // While `markTaskCompleted()` runs the pre-completion listeners, `completed` is not set yet,
      // so the new listener is queued and that thread runs it before it sets `completed`.
      if (!completed) {
        onPreCompleteCallbacks.push(listener)
      }
      completed
    }
    if (completedAlready) {
      // Too late to run before the completion listeners: run it like one added now, i.e. right
      // away, or next if the completion listeners are still running.
      addTaskCompletionListener(listener)
    }
    this
  }

  override def addTaskFailureListener(listener: TaskFailureListener): this.type = {
    synchronized {
      onFailureCallbacks.push(listener)
      failureCauseOpt
    }.foreach(invokeTaskFailureListeners(_, Claim))
    this
  }

  override def addTaskInterruptListener(listener: TaskInterruptListener): this.type = {
    synchronized {
      // If another thread is already running `invokeTaskInterruptListeners`, adding the new
      // listener to `onInterruptCallbacks` will cause that thread to execute it (the loop pops
      // listeners under the TaskContext lock).
      //
      // Otherwise, `invokeTaskInterruptListeners()` below will execute all listeners.
      onInterruptCallbacks.push(listener)
      reasonIfKilled
    }.foreach { reason =>
      invokeTaskInterruptListeners(reason, new TaskKilledException(reason))
    }
    this
  }

  override def addPostStatusUpdateListener(listener: PostStatusUpdateListener): this.type = {
    synchronized {
      onPostStatusUpdateCallbacks.push(listener)
    }
    this
  }

  private[spark] def invokePostStatusUpdateListeners(): Unit = {
    invokeListeners(onPostStatusUpdateCallbacks, "PostStatusUpdateListener", None, Claim) {
      _.onStatusUpdateSent(this)
    }
  }

  override def resourcesJMap(): java.util.Map[String, ResourceInformation] = {
    resources.asJava
  }

  private[spark] override def markTaskFailed(error: Throwable): Unit = {
    markTaskFailed(error, Claim)
  }

  /**
   * @param mode `Nested` if this thread is already invoking listeners, i.e. a listener failed. The
   *             failure listeners then run as part of that invocation, which this thread keeps.
   */
  private def markTaskFailed(error: Throwable, mode: InvocationMode): Unit = {
    synchronized {
      if (failureCauseOpt.isDefined) return
      failureCauseOpt = Some(error)
    }
    invokeTaskFailureListeners(error, mode)
  }

  private[spark] override def markTaskCompleted(error: Option[Throwable]): Unit = {
    synchronized {
      if (completionStarted) return
      completionStarted = true
    }
    // Run the pre-completion listeners on this thread before setting `completed`, outside the
    // listener invocation of `invokeListeners()`. Until `completed` is set, a listener that
    // another thread adds is only queued, so that thread cannot start running the listeners. That
    // thread can be one a pre-completion listener stops and waits for, e.g. a pipelined Python UDF
    // writer that adds listeners lazily while it pulls its input. And since the completion
    // listeners only run after `completed` is set, they run after every pre-completion listener,
    // even if another thread is invoking listeners at this point.
    //
    // The completion listeners then run on this thread too, and this method returns only after
    // they have run. If it returned while another thread is invoking listeners (e.g. a kill thread
    // running failure listeners), that thread would not run them, and the task would go on to
    // release its memory with them pending. So the listener invocation is claimed together with
    // setting `completed`, which also keeps a thread that adds a completion listener at that
    // moment from running them; if another thread holds it, that thread hands it over to this one
    // when done. Failure listeners that another thread leaves to this invocation run before the
    // next completion listener (see `invokeListeners()`). A completion listener added after this
    // method has run the others runs on the thread adding it, unless another thread is invoking
    // listeners at that moment: that thread does not run it (see `invokeListeners()`).
    //
    // The wait for the other thread has no time bound: if its listeners block, so does this task.
    // It logs a warning periodically, the first one with that thread's stack trace. The task waits
    // here before it releases its block locks and memory (the executor does that once `Task.run`
    // returns), so a failure listener on another thread, e.g. a kill thread, must not wait for
    // either. Nothing bounds a wait for a kill thread: with `spark.task.reaper.enabled`, every
    // kill runs on a reaper thread, which checks `spark.task.reaper.killTimeout` only after its
    // kill call returns. If another thread holds the invocation and the task is killed, the reaper
    // with a positive kill timeout bounds the wait by killing the executor JVM, except in local
    // mode, where it only logs an error.
    val currentThread = Thread.currentThread()
    var mode: InvocationMode = Claimed
    var awaitHandover = false
    // Whether this call registered this thread as the listener invocation thread or as the next
    // one, so that it must make sure not to leave it registered.
    var registered = false
    val preCompletionExceptions = new ArrayBuffer[Throwable](2)
    def nextPreCompletionListener(): Option[TaskCompletionListener] = synchronized {
      if (onPreCompleteCallbacks.empty()) {
        // Set together with finding no more pre-completion listeners, so that one added later
        // is handed to `addTaskCompletionListener` instead of being left in the stack. The
        // listener invocation is claimed in the same step (see above).
        completed = true
        if (listenerInvocationThread.isEmpty) {
          listenerInvocationThread = Some(currentThread)
          registered = true
        } else if (listenerInvocationThread.contains(currentThread)) {
          // Called from a listener on this thread: run the completion listeners as part of that
          // invocation.
          mode = Nested
        } else {
          nextListenerInvocationThread = Some(currentThread)
          awaitHandover = true
          registered = true
        }
        None
      } else {
        Some(onPreCompleteCallbacks.pop())
      }
    }
    var listener: Option[TaskCompletionListener] = None
    try {
      while ({listener = nextPreCompletionListener(); listener.nonEmpty}) {
        invokeTaskPreCompletionListener(listener.get, preCompletionExceptions)
      }
      try {
        val warningFailure =
          if (awaitHandover) awaitListenerInvocationHandover(currentThread) else None
        try {
          invokeTaskCompletionListeners(error, mode)
        } catch {
          // The failure to warn came first, so it is the one thrown.
          case e: Throwable if warningFailure.exists(_ ne e) => warningFailure.get.addSuppressed(e)
        }
        warningFailure.foreach(throw _)
      } catch {
        case e: TaskCompletionListenerException if preCompletionExceptions.nonEmpty =>
          // Merge the completion listeners' own failures, not the exception that wraps them. Any
          // other error comes from invokeListeners itself (e.g. an OutOfMemoryError) and is left
          // to propagate, so that the executor still sees a fatal one.
          preCompletionExceptions ++= e.getSuppressed.toSeq
      }
    } finally {
      if (registered) synchronized {
        // `invokeListeners()` releases the invocation itself once it has started; this covers an
        // error before that, e.g. an OutOfMemoryError. A stale waiter would get the invocation
        // handed to it, and a stale holder would keep every other thread from running listeners.
        if (nextListenerInvocationThread.contains(currentThread)) {
          nextListenerInvocationThread = None
        }
        if (listenerInvocationThread.contains(currentThread)) {
          releaseListenerInvocation()
        }
      }
    }
    if (preCompletionExceptions.nonEmpty) {
      throw newTaskCompletionListenerException(preCompletionExceptions.toSeq, error)
    }
  }

  private[spark] override def getTaskFailure: Option[Throwable] = {
    failureCauseOpt
  }

  /**
   * Runs a pre-completion listener. If it fails, marks the task failed, like a failing completion
   * listener does in `invokeListeners()`, and adds the failure to `listenerExceptions`.
   */
  private def invokeTaskPreCompletionListener(
      listener: TaskCompletionListener,
      listenerExceptions: ArrayBuffer[Throwable]): Unit = {
    try {
      listener.onTaskCompletion(this)
    } catch {
      case e: Throwable =>
        try {
          markTaskFailed(e)
        } catch {
          // Failures of the failure listeners. Any other error comes from the invocation itself
          // (e.g. an OutOfMemoryError) and propagates, so that the executor still sees a fatal
          // one.
          case t: TaskCompletionListenerException => e.addSuppressed(t)
        }
        listenerExceptions += e
        logError(log"Error in ${MDC(LISTENER, "TaskPreCompletionListener")}", e)
    }
  }

  private def newTaskCompletionListenerException(
      listenerExceptions: Seq[Throwable],
      error: Option[Throwable]): TaskCompletionListenerException = {
    val exception = new TaskCompletionListenerException(listenerExceptions.map(_.getMessage), error)
    listenerExceptions.foreach(exception.addSuppressed)
    exception
  }

  /**
   * Waits until the thread invoking listeners hands the invocation over to `currentThread`, which
   * has registered itself as `nextListenerInvocationThread`.
   *
   * Interrupts do not stop the wait, and are passed on once it is done. The wait of
   * `PipelinedWriterTask.awaitExit()` in `BasePythonRunner` drops them instead, because it is a
   * listener itself and the listeners right after it must still be able to clean up. This wait
   * is not a listener but part of `markTaskCompleted()`, so it leaves the interrupt status (e.g.
   * set by a task kill) as the completion listeners would see it without the wait.
   *
   * A failure to warn (e.g. an OutOfMemoryError while logging) does not end the wait either, like
   * in `PipelinedWriterTask.awaitExit()`: the other thread does not run the completion listeners,
   * and the task would release its memory with them pending. The first such failure is returned,
   * for `markTaskCompleted()` to throw once it has run the completion listeners. The wait itself
   * is not covered, so that a lasting failure does not spin.
   */
  private def awaitListenerInvocationHandover(currentThread: Thread): Option[Throwable] = {
    val startNs = System.nanoTime()
    var interrupted = false
    var warned = false
    var handedOver = false
    var failure: Option[Throwable] = None
    try {
      while (!handedOver) {
        val holder = synchronized {
          val deadlineNs = System.nanoTime() +
            TimeUnit.MILLISECONDS.toNanos(math.max(1L, listenerHandoverWarnIntervalMs))
          var remainingNs = deadlineNs - System.nanoTime()
          while (!listenerInvocationThread.contains(currentThread) && remainingNs > 0) {
            try {
              wait(math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNs)))
            } catch {
              case _: InterruptedException => interrupted = true
            }
            remainingNs = deadlineNs - System.nanoTime()
          }
          handedOver = listenerInvocationThread.contains(currentThread)
          if (handedOver) None else listenerInvocationThread
        }
        // Outside the lock: walking the stack of another thread stops it at a safepoint, and
        // logging may block, which would otherwise hold up every thread using this TaskContext.
        // The holder does not change during the wait, since it hands the invocation to this
        // thread, so its stack trace is attached to the first warning only.
        holder match {
          case Some(holder) =>
            try {
              warnStillWaiting(holder, startNs, withStackTrace = !warned)
              warned = true
            } catch {
              case t: Throwable => failure = failure.orElse(Some(t))
            }
          case None =>
        }
      }
    } finally {
      if (interrupted) {
        currentThread.interrupt()
      }
    }
    failure
  }

  private def warnStillWaiting(holder: Thread, startNs: Long, withStackTrace: Boolean): Unit = {
    val holderStack = if (withStackTrace) Some(holder.getStackTrace) else None
    // If the holder has handed over meanwhile, its stack shows whatever it does next.
    if (synchronized(listenerInvocationThread.contains(holder))) {
      val waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs)
      val message = log"Task ${MDC(TASK_ATTEMPT_ID, taskAttemptId)} has been waiting " +
        log"for ${MDC(TOTAL_TIME, waitedMs)} ms to run its completion listeners until " +
        log"thread ${MDC(THREAD_NAME, holder.getName)} finishes running its task listeners."
      holderStack match {
        case Some(trace) =>
          val stack = new Exception(s"Stack trace of thread ${holder.getName}")
          stack.setStackTrace(trace)
          logWarning(message, stack)
        case None =>
          logWarning(message)
      }
    }
  }

  /**
   * Deregisters this thread as the listener invocation thread, or hands the invocation over to a
   * thread waiting for it in `markTaskCompleted()`. Must hold the TaskContext lock.
   */
  private def releaseListenerInvocation(): Unit = {
    listenerInvocationThread = nextListenerInvocationThread
    nextListenerInvocationThread = None
    notifyAll()
  }

  private def invokeTaskCompletionListeners(
      error: Option[Throwable],
      mode: InvocationMode): Unit = {
    // It is safe to access the reference to `onCompleteCallbacks` without holding the TaskContext
    // lock. `invokeListeners()` acquires the lock before accessing the contents.
    invokeListeners(onCompleteCallbacks, "TaskCompletionListener", error, mode) {
      _.onTaskCompletion(this)
    }
  }

  private def invokeTaskFailureListeners(error: Throwable, mode: InvocationMode): Unit = {
    // It is safe to access the reference to `onFailureCallbacks` without holding the TaskContext
    // lock. `invokeListeners()` acquires the lock before accessing the contents.
    invokeListeners(onFailureCallbacks, "TaskFailureListener", Option(error), mode) {
      _.onTaskFailure(this, error)
    }
  }

  private def invokeTaskInterruptListeners(reason: String, error: Throwable): Unit = {
    // Do not use `invokeListeners()`. That method uses `listenerInvocationThread` to serialize
    // all listener invocations, so if the task completes or fails while an interrupt listener
    // runs, the task completion would wait for the interrupt listeners, and the failure listeners
    // would only run after them.
    //
    // Instead, directly execute the task interrupt listeners with independent serialization.
    // Exceptions are collected and thrown as a TaskCompletionListenerException so that
    // `markInterrupted` can catch and record the aggregate failure (SPARK-56330).
    def getNextListener(): Option[TaskInterruptListener] = synchronized {
      if (onInterruptCallbacks.empty()) {
        None
      } else {
        Some(onInterruptCallbacks.pop())
      }
    }
    val listenerExceptions = new ArrayBuffer[Throwable](2)
    var listenerOption: Option[TaskInterruptListener] = None
    while ({listenerOption = getNextListener(); listenerOption.nonEmpty}) {
      try {
        listenerOption.get.onTaskInterrupted(this, reason)
      } catch {
        case e: Throwable =>
          listenerExceptions += e
          logError(log"Error in TaskInterruptListener", e)
      }
    }
    if (listenerExceptions.nonEmpty) {
      val exception = new TaskCompletionListenerException(
        listenerExceptions.map(_.getMessage).toSeq, Option(error))
      listenerExceptions.foreach(exception.addSuppressed)
      throw exception
    }
  }

  /**
   * Invokes and removes the listeners in `listeners`, in reverse order of registration.
   *
   * Other threads cannot invoke listeners while this thread does, so they leave the listeners
   * they add, or whose invocation they start, meanwhile. If this is the completion listener
   * invocation of `markTaskCompleted()`, the failure listeners left to it (the task has failed)
   * run before the next completion listener, e.g. so that a failure listener that aborts a state
   * store update runs before the completion listener that releases the state store. Any other
   * invocation does not run the listeners left to it, since its thread may be one on which they
   * must not run, e.g. a kill thread, or the task thread once `Task.run` has unset its
   * `TaskContext` and the executor has released the task's block locks and memory. Unless `mode`
   * is `Nested`, this thread releases the invocation when it is done, or hands it over to a thread
   * waiting for it in `markTaskCompleted()`.
   *
   * Exceptions of the failure listeners run before the next completion listener are reported to
   * the caller of this method, together with those of `listeners`.
   */
  private def invokeListeners[T](
      listeners: Stack[T],
      name: String,
      error: Option[Throwable],
      mode: InvocationMode)(
      callback: T => Unit): Unit = {
    // This method is subject to two constraints:
    //
    // 1. Listeners must be run sequentially to uphold the guarantee provided by the TaskContext
    //    API.
    //
    // 2. Listeners may spawn threads that call methods on this TaskContext. To avoid deadlock, we
    //    cannot call listeners while holding the TaskContext lock.
    //
    // We meet these constraints by ensuring there is at most one thread invoking listeners at any
    // point in time.
    //
    // Allocated before the claim below, so that nothing can fail between the claim and the `try`
    // that releases it.
    //
    // Whether this is the completion listener invocation of `markTaskCompleted()`, the only caller
    // of `invokeTaskCompletionListeners()` that does not claim the invocation.
    val completesTask = (listeners eq onCompleteCallbacks) && mode != Claim
    val listenerExceptions = new ArrayBuffer[Throwable](2)
    var released = mode == Nested
    if (mode == Claim) synchronized {
      if (listenerInvocationThread.nonEmpty) {
        // If another thread is already invoking listeners, do nothing.
        return
      } else {
        // If no other thread is invoking listeners, register this thread as the listener invocation
        // thread. This prevents other threads from invoking listeners until this thread is
        // deregistered.
        listenerInvocationThread = Some(Thread.currentThread())
      }
    }

    def runListener(listener: T): Unit = {
      try {
        callback(listener)
      } catch {
        case e: Throwable =>
          // A listener failed. Call markTaskFailed, which runs the failure listeners as part of
          // this invocation: this thread stays registered as `listenerInvocationThread`, so that
          // no other thread takes over the listeners in the meantime.
          //
          // One of the following cases applies (#3 being the interesting one):
          //
          // 1. [[Task.doRunTask]] is currently calling [[markTaskFailed]] because the task body
          //    failed, and now a failure listener has failed here (not necessarily the first to
          //    fail). Then calling [[markTaskFailed]] again here is a no-op, and we simply resume
          //    running the remaining failure listeners. [[Task.doRunTask]] will then call
          //    [[markTaskCompleted]] after this method returns.
          //
          // 2. The task body failed, [[Task.doRunTask]] already called [[markTaskFailed]],
          //    [[Task.doRunTask]] is currently calling [[markTaskCompleted]], and now a completion
          //    listener has failed here (not necessarily the first one to fail). Then calling
          //    [[markTaskFailed]] it again here is a no-op, and we simply resume running the
          //    remaining completion listeners.
          //
          // 3. [[Task.doRunTask]] is currently calling [[markTaskCompleted]] because the task body
          //    succeeded, and now a completion listener has failed here (the first one to
          //    fail). Then our call to [[markTaskFailed]] here will run all failure listeners
          //    before returning, after which we will resume running the remaining completion
          //    listeners.
          //
          // 4. [[Task.doRunTask]] is currently calling [[markTaskCompleted]] because the task body
          //    succeeded, but [[markTaskFailed]] is currently running because a completion listener
          //    has failed, and now a failure listener has failed (not necessarily the first one to
          //    fail). Then calling [[markTaskFailed]] again here will have no effect, and we simply
          //    resume running the remaining failure listeners; we will resume running the remaining
          //    completion listeners after this call returns.
          //
          // 5. [[Task.doRunTask]] is currently calling [[markTaskCompleted]] because the task body
          //    succeeded, [[markTaskFailed]] already ran because a completion listener previously
          //    failed, and now another completion listener has failed. Then our call to
          //    [[markTaskFailed]] here will have no effect and we simply resume running the
          //    remaining completion handlers.
          try {
            markTaskFailed(e, Nested)
          } catch {
            // Failures of the failure listeners. Any other error comes from the nested invocation
            // itself (e.g. an OutOfMemoryError) and propagates, through the release below.
            case t: TaskCompletionListenerException => e.addSuppressed(t)
          }
          listenerExceptions += e
          logError(log"Error in ${MDC(LISTENER, name)}", e)
      }
    }

    def runLeftFailureListeners(cause: Throwable): Unit = {
      try {
        invokeTaskFailureListeners(cause, Nested)
      } catch {
        // Merge the listeners' own failures, which the nested invocation has logged, not the
        // exception that wraps them. Any other error comes from that invocation itself (e.g. an
        // OutOfMemoryError) and propagates, through the release below.
        case e: TaskCompletionListenerException => listenerExceptions ++= e.getSuppressed.toSeq
      }
    }

    // Chooses the next thing to do under the TaskContext lock, to be done outside of it.
    def nextStep(): Option[() => Unit] = synchronized {
      if (completesTask && failureCauseOpt.isDefined && !onFailureCallbacks.empty()) {
        val cause = failureCauseOpt.get
        Some(() => runLeftFailureListeners(cause))
      } else if (!listeners.empty()) {
        val listener = listeners.pop()
        Some(() => runListener(listener))
      } else {
        // We have executed all listeners that have been added so far. Deregister this thread as
        // the callback invocation thread, or hand the invocation over to a waiting thread.
        if (mode != Nested) {
          releaseListenerInvocation()
        }
        None
      }
    }

    try {
      var step: Option[() => Unit] = None
      while ({step = nextStep(); step.nonEmpty}) {
        step.get()
      }
      released = true
    } finally {
      if (!released) {
        // Something escaped the loop above. Do not leave this thread registered, which would
        // keep any other thread from running listeners, or a waiting thread waiting forever.
        synchronized {
          releaseListenerInvocation()
        }
      }
    }
    if (listenerExceptions.nonEmpty) {
      val exception = new TaskCompletionListenerException(
        listenerExceptions.map(_.getMessage).toSeq, error)
      listenerExceptions.foreach(exception.addSuppressed)
      throw exception
    }
  }

  private[spark] override def markInterrupted(reason: String): Unit = {
    reasonIfKilled = Some(reason)
    try {
      invokeTaskInterruptListeners(reason, new TaskKilledException(reason))
    } catch {
      case e: TaskCompletionListenerException =>
        // SPARK-56330: Do not propagate to the executor kill / RPC thread; record the aggregate
        // failure for the task (not the first listener exception only).
        logError(log"Exception(s) in TaskInterruptListener(s) after task was interrupted", e)
        markTaskFailed(e)
    }
  }

  private[spark] override def killTaskIfInterrupted(): Unit = {
    val reason = reasonIfKilled
    if (reason.isDefined) {
      throw new TaskKilledException(reason.get)
    }
  }

  private[spark] override def getKillReason(): Option[String] = {
    reasonIfKilled
  }

  @GuardedBy("this")
  override def isCompleted(): Boolean = synchronized(completed)

  override def isFailed(): Boolean = synchronized(failureCauseOpt.isDefined)

  override def isInterrupted(): Boolean = reasonIfKilled.isDefined

  override def getLocalProperty(key: String): String = localProperties.getProperty(key)

  override def getMetricsSources(sourceName: String): Seq[Source] =
    metricsSystem.getSourcesByName(sourceName)

  private[spark] override def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
    taskMetrics.registerAccumulator(a)
  }

  private[spark] override def setFetchFailed(fetchFailed: FetchFailedException): Unit = {
    this._fetchFailedException = Option(fetchFailed)
  }

  private[spark] override def fetchFailed: Option[FetchFailedException] = _fetchFailedException

  private[spark] override def getLocalProperties: Properties = localProperties


  override def interruptible(): Boolean = TaskContext.synchronized(_interruptible)

  override def pendingInterrupt(threadToInterrupt: Option[Thread], reason: String): Unit = {
    TaskContext.synchronized {
      pendingInterruptRequest = Some((threadToInterrupt, reason))
    }
  }

  def createResourceUninterruptibly[T <: Closeable](resourceBuilder: => T): T = {

    @inline def interruptIfRequired(): Unit = {
      pendingInterruptRequest.foreach { case (threadToInterrupt, reason) =>
        markInterrupted(reason)
        threadToInterrupt.foreach(_.interrupt())
      }
      killTaskIfInterrupted()
    }

    TaskContext.synchronized {
      interruptIfRequired()
      _interruptible = false
    }
    try {
      val resource = resourceBuilder
      addTaskCompletionListener[Unit](_ => resource.close())
      resource
    } finally {
      TaskContext.synchronized {
        _interruptible = true
        interruptIfRequired()
      }
    }
  }
}

private object TaskContextImpl {

  /** How an `invokeListeners()` call takes part in the listener invocation. */
  sealed trait InvocationMode

  /**
   * Registers this thread as `listenerInvocationThread` if no thread is registered; otherwise
   * leaves the listeners to the registered thread, which may be this one, and which runs them only
   * in some cases (see `invokeListeners()`).
   */
  case object Claim extends InvocationMode

  /** The caller has already registered this thread as `listenerInvocationThread`. */
  case object Claimed extends InvocationMode

  /** An outer invocation on this thread has registered it, and keeps it when this one is done. */
  case object Nested extends InvocationMode

  /** How often to log while `markTaskCompleted()` waits for another thread invoking listeners. */
  val DEFAULT_LISTENER_HANDOVER_WARN_INTERVAL_MS = 10000L
}
