package spark.scheduler.cluster

import spark.executor.ExecutorExitCode

/**
 * Represents an explanation for a executor or whole slave failing or exiting.
 */
private[spark]
class ExecutorLossReason(val message: String) {
  override def toString: String = message
}

private[spark]
case class ExecutorExited(val exitCode: Int)
  extends ExecutorLossReason(ExecutorExitCode.explainExitCode(exitCode)) {
}

private[spark]
case class SlaveLost(_message: String = "Slave lost")
  extends ExecutorLossReason(_message) {
}
