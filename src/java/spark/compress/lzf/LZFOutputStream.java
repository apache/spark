package spark.compress.lzf;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class LZFOutputStream extends FilterOutputStream {
  private static final int BLOCKSIZE = 1024 * 64 - 1;
  private static final int MAX_HDR_SIZE = 7;

  private byte[] inBuf;   // Holds input data to be compressed
  private byte[] outBuf;  // Holds compressed data to be written
  private int inPos;      // Current position in inBuf

  public LZFOutputStream(OutputStream out) {
    super(out);
    inBuf = new byte[BLOCKSIZE + MAX_HDR_SIZE];
    outBuf = new byte[BLOCKSIZE + MAX_HDR_SIZE];
    inPos = MAX_HDR_SIZE;
  }

  @Override
  public void write(int b) throws IOException {
    inBuf[inPos++] = (byte) b;
    if (inPos == inBuf.length)
      compressAndSendBlock();
  }
  
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if ((off | len | (off + len) | (b.length - (off + len))) < 0)
      throw new IndexOutOfBoundsException();

    // If we're given a large array, copy it piece by piece into inBuf and
    // write one BLOCKSIZE at a time. This is done to prevent the JNI code
    // from copying the whole array repeatedly if GetPrimitiveArrayCritical
    // decides to copy instead of pinning.
    while (inPos + len >= inBuf.length) {
      int amtToCopy = inBuf.length - inPos;
      System.arraycopy(b, off, inBuf, inPos, amtToCopy);
      inPos += amtToCopy;
      compressAndSendBlock();
      off += amtToCopy;
      len -= amtToCopy;
    }

    // Copy the remaining (incomplete) block into inBuf
    System.arraycopy(b, off, inBuf, inPos, len);
    inPos += len;
  }

  @Override
  public void flush() throws IOException {
    if (inPos > MAX_HDR_SIZE)
      compressAndSendBlock();
    out.flush();
  }

  // Send the data in inBuf, and reset inPos to start writing a new block.
  private void compressAndSendBlock() throws IOException {
    int us = inPos - MAX_HDR_SIZE;
    int maxcs = us > 4 ? us - 4 : us;
    int cs = LZF.compress(inBuf, MAX_HDR_SIZE, us, outBuf, MAX_HDR_SIZE, maxcs);
    if (cs != 0) {
      // Compression made the data smaller; use type 1 header
      outBuf[0] = 'Z';
      outBuf[1] = 'V';
      outBuf[2] = 1;
      outBuf[3] = (byte) (cs >> 8);
      outBuf[4] = (byte) (cs & 0xFF);
      outBuf[5] = (byte) (us >> 8);
      outBuf[6] = (byte) (us & 0xFF);
      out.write(outBuf, 0, 7 + cs);
    } else {
      // Compression didn't help; use type 0 header and uncompressed data
      inBuf[2] = 'Z';
      inBuf[3] = 'V';
      inBuf[4] = 0;
      inBuf[5] = (byte) (us >> 8);
      inBuf[6] = (byte) (us & 0xFF);
      out.write(inBuf, 2, 5 + us);
    }
    inPos = MAX_HDR_SIZE;
  }
}
