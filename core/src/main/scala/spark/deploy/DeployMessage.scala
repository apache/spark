package spark.deploy

sealed trait DeployMessage

case class RegisterSlave(host: String, port: Int, cores: Int, memory: Int) extends DeployMessage
case class RegisteredSlave(clusterId: String, slaveId: Int) extends DeployMessage
