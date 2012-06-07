package spark

class TaskContext(val stageId: Int, val splitId: Int, val attemptId: Int) extends Serializable
