package spark.storage

import java.io.{InputStream, OutputStream}

import org.xerial.snappy.{SnappyInputStream, SnappyOutputStream}

/**
 * Snappy implementation of [[spark.storage.CompressionCodec]]
 * block size can be configured by spark.snappy.block.size
 */
class SnappyCompressionCodec extends CompressionCodec {
  def compressionOutputStream(s: OutputStream): OutputStream =
    new SnappyOutputStream(s, 
      System.getProperty("spark.snappy.block.size", "32768").toInt)

  def compressionInputStream(s: InputStream): InputStream =
    new SnappyInputStream(s)
}
