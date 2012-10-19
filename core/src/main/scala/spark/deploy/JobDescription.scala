package spark.deploy

private[spark] class JobDescription(
    val name: String,
    val cores: Int,
    val memoryPerSlave: Int,
    val command: Command)
  extends Serializable {

  val user = System.getProperty("user.name", "<unknown>")

  override def toString: String = "JobDescription(" + name + ")"
}
