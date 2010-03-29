package ubiquifs

private[ubiquifs] object Utils {
  private val HOST_PORT_RE = "([a-zA-Z0-9.-]+):([0-9]+)".r

  def parseHostPort(string: String): (String, Int) = {
    string match {
      case HOST_PORT_RE(host, port) => (host, port.toInt)
      case _ => throw new IllegalArgumentException(string)
    }
  }
}
