package spark.scheduler.cluster

import java.nio.ByteBuffer

class TaskDescription(val taskId: Long, val name: String, val serializedTask: ByteBuffer) {}
