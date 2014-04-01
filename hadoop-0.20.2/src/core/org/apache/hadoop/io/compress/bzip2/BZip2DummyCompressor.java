package org.apache.hadoop.io.compress.bzip2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;

/**
 * This is a dummy compressor for BZip2.
 */
public class BZip2DummyCompressor implements Compressor {

  @Override
  public int compress(byte[] b, int off, int len) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void end() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void finish() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean finished() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getBytesRead() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getBytesWritten() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean needsInput() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void reset() {
    // do nothing
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInput(byte[] b, int off, int len) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void reinit(Configuration conf) {
    // do nothing
  }

}
