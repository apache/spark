package spark.storage

import java.io.{InputStream, OutputStream}

import com.ning.compress.lzf.{LZFInputStream, LZFOutputStream}

/**
 * LZF implementation of [[spark.storage.CompressionCodec]]
 */
class LZFCompressionCodec extends CompressionCodec {
  def compressionOutputStream(s: OutputStream): OutputStream =
    (new LZFOutputStream(s)).setFinishBlockOnFlush(true)

  def compressionInputStream(s: InputStream): InputStream =
    new LZFInputStream(s)
}
