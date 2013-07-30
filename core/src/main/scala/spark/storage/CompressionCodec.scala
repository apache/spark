package spark.storage

import java.io.{InputStream, OutputStream}


/**
 * CompressionCodec allows the customization of the compression codec
 */
trait CompressionCodec {
  def compressionOutputStream(s: OutputStream): OutputStream

  def compressionInputStream(s: InputStream): InputStream
}
