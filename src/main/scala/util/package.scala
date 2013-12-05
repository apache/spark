package catalyst

import java.io.{PrintWriter, ByteArrayOutputStream, FileInputStream, File}

package object util {
  /**
   * Returns a path to a temporary file that probably does not exist.
   * Note, there is always the race condition that someone created this
   * file since the last time we checked.  Thus, this shouldn't be used
   * for anything security conscious.
   */
  def getTempFilePath(prefix: String, suffix: String = ""): File = {
    val tempFile = File.createTempFile(prefix, suffix)
    tempFile.delete()
    tempFile
  }

  def fileToString(file: File, encoding: String = "UTF-8") = {
    val inStream = new FileInputStream(file)
    val outStream = new ByteArrayOutputStream
    try {
      var reading = true
      while ( reading ) {
        inStream.read() match {
          case -1 => reading = false
          case c => outStream.write(c)
        }
      }
      outStream.flush()
    }
    finally {
      inStream.close()
    }
    new String(outStream.toByteArray(), encoding)
  }

  def stringToFile(file: File, str: String): File = {
    val out = new PrintWriter(file)
    out.write(str)
    out.close()
    file
  }
}