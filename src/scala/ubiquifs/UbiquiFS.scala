package ubiquifs

import java.io.{InputStream, OutputStream}

class UbiquiFS(master: String) {
  private val (masterHost, masterPort) = Utils.parseHostPort(master)

  def create(path: String): OutputStream = null

  def open(path: String): InputStream = null
}
