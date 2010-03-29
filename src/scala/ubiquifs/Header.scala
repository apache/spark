package ubiquifs

import java.io.{DataInputStream, DataOutputStream}

object RequestType {
  val READ = 0
  val WRITE = 1
}

class Header(val requestType: Int, val path: String) {
  def write(out: DataOutputStream) {
    out.write(requestType)
    out.writeUTF(path)
  }
}

object Header {
  def read(in: DataInputStream): Header = {
    new Header(in.read(), in.readUTF())
  }
}
