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

  def sideBySide(left: String, right: String): Seq[String] = sideBySide(left.split("\n"), right.split("\n"))

  def sideBySide(left: Seq[String], right: Seq[String]): Seq[String] = {
    val maxLeftSize = left.map(_.size).max
    val leftPadded = left ++ Seq.fill(if (left.size < right.size) right.size - left.size else 0)("")
    val rightPadded = right ++ Seq.fill(if (right.size < left.size) left.size - right.size else 0)("")

    leftPadded.zip(rightPadded).map {
      case (l,r) => (if (l == r) " " else "!") + l + (" " * ((maxLeftSize - l.size) + 3)) + r
    }
  }

  def stringOrNull(a: AnyRef) = if (a == null) null else a.toString

  implicit class debugLogging(a: AnyRef) {
    def debugLogging {
      org.apache.log4j.Logger.getLogger(a.getClass.getName).setLevel(org.apache.log4j.Level.DEBUG)
    }
  }
}