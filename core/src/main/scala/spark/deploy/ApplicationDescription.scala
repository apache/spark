package spark.deploy

private[spark] class ApplicationDescription(
    val name: String,
    val maxCores: Int, /* Integer.MAX_VALUE denotes an unlimited number of cores */
    val memoryPerSlave: Int,
    val command: Command,
    val sparkHome: String,
    val appUiUrl: String)
  extends Serializable {

  val user = System.getProperty("user.name", "<unknown>")

  override def toString: String = "ApplicationDescription(" + name + ")"
}
