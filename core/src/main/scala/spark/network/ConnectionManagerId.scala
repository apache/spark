package spark.network

import java.net.InetSocketAddress

import spark.Utils


private[spark] case class ConnectionManagerId(host: String, port: Int) {
  // DEBUG code
  Utils.checkHost(host)
  assert (port > 0)

  def toSocketAddress() = new InetSocketAddress(host, port)
}


private[spark] object ConnectionManagerId {
  def fromSocketAddress(socketAddress: InetSocketAddress): ConnectionManagerId = {
    new ConnectionManagerId(socketAddress.getHostName(), socketAddress.getPort())
  }
}
