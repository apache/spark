package ubiquifs

sealed case class Message()

case class RegisterSlave(host: String, port: Int) extends Message
case class RegisterSucceeded() extends Message

case class Create(path: String) extends Message
case class CreateSucceeded(host: String, port: Int) extends Message
case class CreateFailed(message: String) extends Message

case class Read(path: String) extends Message
case class ReadSucceeded(host: String, port: Int) extends Message
case class ReadFailed(message: String) extends Message
