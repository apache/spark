package spark.deploy

import java.io.File

private[spark] class JobDescription(
    val name: String,
    val cores: Int,
    val memoryPerSlave: Int,
    val command: Command,
    val sparkHome: File)
  extends Serializable {

  val user = System.getProperty("user.name", "<unknown>")

  override def toString: String = "JobDescription(" + name + ")"
}
