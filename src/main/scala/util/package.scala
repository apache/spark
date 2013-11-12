package catalyst

import java.io.File

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
}