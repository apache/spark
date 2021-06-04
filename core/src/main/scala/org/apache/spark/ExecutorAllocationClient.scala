
package org.apache.spark

/**
 * A client that communicates with the cluster manager to request or kill executors.
 * This is currently supported only in YARN mode.
 * 与集群管理器通信以请求或杀死Executor的客户端。目前仅在 YARN 模式下支持。
 */
private[spark] trait ExecutorAllocationClient {


  /** Get the list of currently active executors */
  private[spark] def getExecutorIds(): Seq[String]

  /**
   * Update the cluster manager on our scheduling needs. Three bits of information are included
   * to help it make decisions.
   * 根据我们的调度需求更新集群管理器。包含三个信息以帮助它做出决定。
   * @param numExecutors The total number of executors we'd like to have. The cluster manager
   *                     shouldn't kill any running executor to reach this number, but,
   *                     if all existing executors were to die, this is the number of executors
   *                     we'd want to be allocated.
   *                     我们希望拥有的执行者总数。集群管理器不应该杀死任何正在运行的执行器来达到这个数量，但是，如果所有现有的执行器都死了，这就是我们想要分配的执行器数量。
   * @param localityAwareTasks The number of tasks in all active stages that have a locality
   *                           preferences. This includes running, pending, and completed tasks.
   *                           具有位置偏好的所有活动阶段中的任务数量。这包括正在运行、挂起和已完成的任务。
   * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
   *                             that would like to like to run on that host.
   *                             This includes running, pending, and completed tasks.
   * @return whether the request is acknowledged by the cluster manager.
   */
  private[spark] def requestTotalExecutors(
      numExecutors: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int]): Boolean

  /**
   * Request an additional number of executors from the cluster manager.
   * @return whether the request is acknowledged by the cluster manager.
   */
  def requestExecutors(numAdditionalExecutors: Int): Boolean

  /**
   * Request that the cluster manager kill the specified executors.
   *
   * @param executorIds identifiers of executors to kill
   * @param adjustTargetNumExecutors whether the target number of executors will be adjusted down
   *                                 after these executors have been killed
   * @param countFailures if there are tasks running on the executors when they are killed, whether
    *                     to count those failures toward task failure limits
   * @param force whether to force kill busy executors, default false
   * @return the ids of the executors acknowledged by the cluster manager to be removed.
   */
  def killExecutors(
    executorIds: Seq[String],
    adjustTargetNumExecutors: Boolean,
    countFailures: Boolean,
    force: Boolean = false): Seq[String]

  /**
   * Request that the cluster manager kill every executor on the specified host.
   *
   * @return whether the request is acknowledged by the cluster manager.
   */
  def killExecutorsOnHost(host: String): Boolean

  /**
   * Request that the cluster manager kill the specified executor.
   * @return whether the request is acknowledged by the cluster manager.
   */
  def killExecutor(executorId: String): Boolean = {
    val killedExecutors = killExecutors(Seq(executorId), adjustTargetNumExecutors = true,
      countFailures = false)
    killedExecutors.nonEmpty && killedExecutors(0).equals(executorId)
  }
}
