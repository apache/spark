package org.apache.hadoop.io.compress.bzip2;

import java.io.IOException;

import org.apache.hadoop.io.compress.Decompressor;

/**
 * This is a dummy decompressor for BZip2.
 */
public class BZip2DummyDecompressor implements Decompressor {

  @Override
  public int decompress(byte[] b, int off, int len) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void end() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean finished() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean needsDictionary() {
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

}
