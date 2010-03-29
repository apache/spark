package spark.compress.lzf;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class LZFInputStream extends FilterInputStream {
  private static final int MAX_BLOCKSIZE = 1024 * 64 - 1;
  private static final int MAX_HDR_SIZE = 7;

  private byte[] inBuf;     // Holds data to decompress (including header)
  private byte[] outBuf;    // Holds decompressed data to output
  private int outPos;       // Current position in outBuf
  private int outSize;      // Total amount of data in outBuf

  private boolean closed;
  private boolean reachedEof;

  private byte[] singleByte = new byte[1];

  public LZFInputStream(InputStream in) {
    super(in);
    if (in == null)
      throw new NullPointerException();
    inBuf = new byte[MAX_BLOCKSIZE + MAX_HDR_SIZE];
    outBuf = new byte[MAX_BLOCKSIZE + MAX_HDR_SIZE];
    outPos = 0;
    outSize = 0;
  }

  private void ensureOpen() throws IOException {
    if (closed) throw new IOException("Stream closed");
  }

  @Override
  public int read() throws IOException {
    ensureOpen();
    int count = read(singleByte, 0, 1);
    return (count == -1 ? -1 : singleByte[0] & 0xFF);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    ensureOpen();
    if ((off | len | (off + len) | (b.length - (off + len))) < 0)
      throw new IndexOutOfBoundsException();

    int totalRead = 0;

    // Start with the current block in outBuf, and read and decompress any
    // further blocks necessary. Instead of trying to decompress directly to b
    // when b is large, we always use outBuf as an intermediate holding space
    // in case GetPrimitiveArrayCritical decides to copy arrays instead of
    // pinning them, which would cause b to be copied repeatedly into C-land.
    while (len > 0) {
      if (outPos == outSize) {
        readNextBlock();
        if (reachedEof)
          return totalRead == 0 ? -1 : totalRead;
      }
      int amtToCopy = Math.min(outSize - outPos, len);
      System.arraycopy(outBuf, outPos, b, off, amtToCopy);
      off += amtToCopy;
      len -= amtToCopy;
      outPos += amtToCopy;
      totalRead += amtToCopy;
    }

    return totalRead;
  }

  // Read len bytes from this.in to a buffer, stopping only if EOF is reached
  private int readFully(byte[] b, int off, int len) throws IOException {
    int totalRead = 0;
    while (len > 0) {
      int amt = in.read(b, off, len);
      if (amt == -1)
        break;
      off += amt;
      len -= amt;
      totalRead += amt;
    }
    return totalRead;
  }

  // Read the next block from the underlying InputStream into outBuf,
  // setting outPos and outSize, or set reachedEof if the stream ends.
  private void readNextBlock() throws IOException {
    // Read first 5 bytes of header
    int count = readFully(inBuf, 0, 5);
    if (count == 0) {
      reachedEof = true;
      return;
    } else if (count < 5) {
      throw new EOFException("Truncated LZF block header");
    }

    // Check magic bytes
    if (inBuf[0] != 'Z' || inBuf[1] != 'V')
      throw new IOException("Wrong magic bytes in LZF block header");

    // Read the block
    if (inBuf[2] == 0) {
      // Uncompressed block - read directly to outBuf
      int size = ((inBuf[3] & 0xFF) << 8) | (inBuf[4] & 0xFF);
      if (readFully(outBuf, 0, size) != size)
        throw new EOFException("EOF inside LZF block");
      outPos = 0;
      outSize = size;
    } else if (inBuf[2] == 1) {
      // Compressed block - read to inBuf and decompress
      if (readFully(inBuf, 5, 2) != 2)
        throw new EOFException("Truncated LZF block header");
      int csize = ((inBuf[3] & 0xFF) << 8) | (inBuf[4] & 0xFF);
      int usize = ((inBuf[5] & 0xFF) << 8) | (inBuf[6] & 0xFF);
      if (readFully(inBuf, 7, csize) != csize)
        throw new EOFException("Truncated LZF block");
      if (LZF.decompress(inBuf, 7, csize, outBuf, 0, usize) != usize)
        throw new IOException("Corrupt LZF data stream");
      outPos = 0;
      outSize = usize;
    } else {
      throw new IOException("Unknown block type in LZF block header");
    }
  }

  /**
   * Returns 0 after EOF has been reached, otherwise always return 1.
   *
   * Programs should not count on this method to return the actual number
   * of bytes that could be read without blocking.
   */
  @Override
  public int available() throws IOException {
    ensureOpen();
    return reachedEof ? 0 : 1;
  }

  // TODO: Skip complete chunks without decompressing them?
  @Override
  public long skip(long n) throws IOException {
    ensureOpen();
    if (n < 0)
      throw new IllegalArgumentException("negative skip length");
    byte[] buf = new byte[512];
    long skipped = 0;
    while (skipped < n) {
      int len = (int) Math.min(n - skipped, buf.length);
      len = read(buf, 0, len);
      if (len == -1) {
        reachedEof = true;
        break;
      }
      skipped += len;
    }
    return skipped;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      in.close();
      closed = true;
    }
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void mark(int readLimit) {}

  @Override
  public void reset() throws IOException {
    throw new IOException("mark/reset not supported");
  }
}
