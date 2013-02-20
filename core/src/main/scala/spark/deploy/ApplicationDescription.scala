package spark.deploy

private[spark] class ApplicationDescription(
    val name: String,
    val cores: Int,
    val memoryPerSlave: Int,
    val command: Command,
    val sparkHome: String)
  extends Serializable {

  val user = System.getProperty("user.name", "<unknown>")

  override def toString: String = "ApplicationDescription(" + name + ")"
}
