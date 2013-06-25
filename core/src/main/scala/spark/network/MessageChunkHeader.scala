package spark.network

import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer


private[spark] class MessageChunkHeader(
    val typ: Long,
    val id: Int,
    val totalSize: Int,
    val chunkSize: Int,
    val other: Int,
    val address: InetSocketAddress) {
  lazy val buffer = {
    // No need to change this, at 'use' time, we do a reverse lookup of the hostname.
    // Refer to network.Connection
    val ip = address.getAddress.getAddress()
    val port = address.getPort()
    ByteBuffer.
      allocate(MessageChunkHeader.HEADER_SIZE).
      putLong(typ).
      putInt(id).
      putInt(totalSize).
      putInt(chunkSize).
      putInt(other).
      putInt(ip.size).
      put(ip).
      putInt(port).
      position(MessageChunkHeader.HEADER_SIZE).
      flip.asInstanceOf[ByteBuffer]
  }

  override def toString = "" + this.getClass.getSimpleName + ":" + id + " of type " + typ +
      " and sizes " + totalSize + " / " + chunkSize + " bytes"
}


private[spark] object MessageChunkHeader {
  val HEADER_SIZE = 40

  def create(buffer: ByteBuffer): MessageChunkHeader = {
    if (buffer.remaining != HEADER_SIZE) {
      throw new IllegalArgumentException("Cannot convert buffer data to Message")
    }
    val typ = buffer.getLong()
    val id = buffer.getInt()
    val totalSize = buffer.getInt()
    val chunkSize = buffer.getInt()
    val other = buffer.getInt()
    val ipSize = buffer.getInt()
    val ipBytes = new Array[Byte](ipSize)
    buffer.get(ipBytes)
    val ip = InetAddress.getByAddress(ipBytes)
    val port = buffer.getInt()
    new MessageChunkHeader(typ, id, totalSize, chunkSize, other, new InetSocketAddress(ip, port))
  }
}
