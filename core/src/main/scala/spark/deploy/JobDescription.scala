package spark.deploy

class JobDescription(
    val name: String,
    val memoryPerSlave: Int,
    val cores: Int,
    val resources: Seq[String],
    val command: Command)
  extends Serializable {

  val user = System.getProperty("user.name", "<unknown>")
}
