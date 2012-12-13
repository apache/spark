package spark.scheduler.cluster

import spark.executor.ExecutorExitCode

/**
 * Represents an explanation for a executor or whole slave failing or exiting.
 */
private[spark]
class ExecutorLostReason(val message: String) {
  override def toString: String = message
}

private[spark]
case class ExecutorExited(val exitCode: Int)
    extends ExecutorLostReason(ExecutorExitCode.explainExitCode(exitCode)) {
}

private[spark]
case class SlaveLost(_message: String = "Slave lost")
    extends ExecutorLostReason(_message) {
}
