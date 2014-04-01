/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

/*
 * This package is based on the work done by Keiron Liddle, Aftex Software
 * <keiron@aftexsw.com> to whom the Ant project is very grateful for his
 * great code.
 */
package org.apache.hadoop.io.compress.bzip2;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.IOException;

import org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE;


/**
 * An input stream that decompresses from the BZip2 format (without the file
 * header chars) to be read as any other stream.
 *
 * <p>
 * The decompression requires large amounts of memory. Thus you should call the
 * {@link #close() close()} method as soon as possible, to force
 * <tt>CBZip2InputStream</tt> to release the allocated memory. See
 * {@link CBZip2OutputStream CBZip2OutputStream} for information about memory
 * usage.
 * </p>
 *
 * <p>
 * <tt>CBZip2InputStream</tt> reads bytes from the compressed source stream via
 * the single byte {@link java.io.InputStream#read() read()} method exclusively.
 * Thus you should consider to use a buffered source stream.
 * </p>
 *
 * <p>
 * This Ant code was enhanced so that it can de-compress blocks of bzip2 data.
 * Current position in the stream is an important statistic for Hadoop. For
 * example in LineRecordReader, we solely depend on the current position in the
 * stream to know about the progess. The notion of position becomes complicated
 * for compressed files. The Hadoop splitting is done in terms of compressed
 * file. But a compressed file deflates to a large amount of data. So we have
 * handled this problem in the following way.
 *
 * On object creation time, we find the next block start delimiter. Once such a
 * marker is found, the stream stops there (we discard any read compressed data
 * in this process) and the position is updated (i.e. the caller of this class
 * will find out the stream location). At this point we are ready for actual
 * reading (i.e. decompression) of data.
 *
 * The subsequent read calls give out data. The position is updated when the
 * caller of this class has read off the current block + 1 bytes. In between the
 * block reading, position is not updated. (We can only update the postion on
 * block boundaries).
 * </p>
 *
 * <p>
 * Instances of this class are not threadsafe.
 * </p>
 */
public class CBZip2InputStream extends InputStream implements BZip2Constants {

  public static final long BLOCK_DELIMITER = 0X314159265359L;// start of block
  public static final long EOS_DELIMITER = 0X177245385090L;// end of bzip2 stream
  private static final int DELIMITER_BIT_LENGTH = 48;
  READ_MODE readMode = READ_MODE.CONTINUOUS;
  // The variable records the current advertised position of the stream.
  private long reportedBytesReadFromCompressedStream = 0L;
  // The following variable keep record of compressed bytes read.
  private long bytesReadFromCompressedStream = 0L;
  private boolean lazyInitialization = false;
  private byte array[] = new byte[1];

  /**
  * Index of the last char in the block, so the block size == last + 1.
  */
  private int last;

  /**
  * Index in zptr[] of original string after sorting.
  */
  private int origPtr;

  /**
  * always: in the range 0 .. 9. The current block size is 100000 * this
  * number.
  */
  private int blockSize100k;

  private boolean blockRandomised = false;

  private long bsBuff;
  private long bsLive;
  private final CRC crc = new CRC();

  private int nInUse;

  private BufferedInputStream in;

  private int currentChar = -1;

  /**
   * A state machine to keep track of current state of the de-coder
   *
   */
  public enum STATE {
    EOF, START_BLOCK_STATE, RAND_PART_A_STATE, RAND_PART_B_STATE, RAND_PART_C_STATE, NO_RAND_PART_A_STATE, NO_RAND_PART_B_STATE, NO_RAND_PART_C_STATE, NO_PROCESS_STATE
  };

  private STATE currentState = STATE.START_BLOCK_STATE;

  private int storedBlockCRC, storedCombinedCRC;
  private int computedBlockCRC, computedCombinedCRC;

  private boolean skipResult = false;// used by skipToNextMarker
  private static boolean skipDecompression = false;

  // Variables used by setup* methods exclusively

  private int su_count;
  private int su_ch2;
  private int su_chPrev;
  private int su_i2;
  private int su_j2;
  private int su_rNToGo;
  private int su_rTPos;
  private int su_tPos;
  private char su_z;

  /**
  * All memory intensive stuff. This field is initialized by initBlock().
  */
  private CBZip2InputStream.Data data;

  /**
  * This method reports the processed bytes so far. Please note that this
  * statistic is only updated on block boundaries and only when the stream is
  * initiated in BYBLOCK mode.
  */
  public long getProcessedByteCount() {
    return reportedBytesReadFromCompressedStream;
  }

  /**
   * This method keeps track of raw processed compressed
   * bytes.
   *
   * @param count count is the number of bytes to be
   *           added to raw processed bytes
   */

  protected void updateProcessedByteCount(int count) {
    this.bytesReadFromCompressedStream += count;
  }

  /**
   * This method is called by the client of this
   * class in case there are any corrections in
   * the stream position.  One common example is
   * when client of this code removes starting BZ
   * characters from the compressed stream.
   *
   * @param count count bytes are added to the reported bytes
   *
   */
  public void updateReportedByteCount(int count) {
    this.reportedBytesReadFromCompressedStream += count;
    this.updateProcessedByteCount(count);
  }

  /**
  * This method reads a Byte from the compressed stream. Whenever we need to
  * read from the underlying compressed stream, this method should be called
  * instead of directly calling the read method of the underlying compressed
  * stream. This method does important record keeping to have the statistic
  * that how many bytes have been read off the compressed stream.
  */
  private int readAByte(InputStream inStream) throws IOException {
    int read = inStream.read();
    if (read >= 0) {
      this.updateProcessedByteCount(1);
    }
    return read;
  }

  /**
  * This method tries to find the marker (passed to it as the first parameter)
  * in the stream.  It can find bit patterns of length <= 63 bits.  Specifically
  * this method is used in CBZip2InputStream to find the end of block (EOB)
  * delimiter in the stream, starting from the current position of the stream.
  * If marker is found, the stream position will be right after marker at the
  * end of this call.
  *
  * @param marker  The bit pattern to be found in the stream
  * @param markerBitLength  No of bits in the marker
  *
  * @throws IOException
  * @throws IllegalArgumentException  if marketBitLength is greater than 63
  */
  public boolean skipToNextMarker(long marker, int markerBitLength)
      throws IOException, IllegalArgumentException {
    try {
      if (markerBitLength > 63) {
        throw new IllegalArgumentException(
            "skipToNextMarker can not find patterns greater than 63 bits");
      }
      // pick next marketBitLength bits in the stream
      long bytes = 0;
      bytes = this.bsR(markerBitLength);
      if (bytes == -1) {
        return false;
      }
      while (true) {
        if (bytes == marker) {
          return true;

        } else {
          bytes = bytes << 1;
          bytes = bytes & ((1L << markerBitLength) - 1);
          int oneBit = (int) this.bsR(1);
          if (oneBit != -1) {
            bytes = bytes | oneBit;
          } else
            return false;
        }
      }
    } catch (IOException ex) {
      return false;
    }
  }

  protected void reportCRCError() throws IOException {
    throw new IOException("crc error");
  }

  private void makeMaps() {
    final boolean[] inUse = this.data.inUse;
    final byte[] seqToUnseq = this.data.seqToUnseq;

    int nInUseShadow = 0;

    for (int i = 0; i < 256; i++) {
      if (inUse[i])
        seqToUnseq[nInUseShadow++] = (byte) i;
    }

    this.nInUse = nInUseShadow;
  }

  /**
  * Constructs a new CBZip2InputStream which decompresses bytes read from the
  * specified stream.
  *
  * <p>
  * Although BZip2 headers are marked with the magic <tt>"Bz"</tt> this
  * constructor expects the next byte in the stream to be the first one after
  * the magic. Thus callers have to skip the first two bytes. Otherwise this
  * constructor will throw an exception.
  * </p>
  *
  * @throws IOException
  *             if the stream content is malformed or an I/O error occurs.
  * @throws NullPointerException
  *             if <tt>in == null</tt>
  */
  public CBZip2InputStream(final InputStream in, READ_MODE readMode)
      throws IOException {

    super();
    int blockSize = 0X39;// i.e 9
    this.blockSize100k = blockSize - '0';
    this.in = new BufferedInputStream(in, 1024 * 9);// >1 MB buffer
    this.readMode = readMode;
    if (readMode == READ_MODE.CONTINUOUS) {
      currentState = STATE.START_BLOCK_STATE;
      lazyInitialization = (in.available() == 0)?true:false;
      if(!lazyInitialization){
    init();
  }
    } else if (readMode == READ_MODE.BYBLOCK) {
      this.currentState = STATE.NO_PROCESS_STATE;
      skipResult = this.skipToNextMarker(CBZip2InputStream.BLOCK_DELIMITER,DELIMITER_BIT_LENGTH);
      this.reportedBytesReadFromCompressedStream = this.bytesReadFromCompressedStream;
      if(!skipDecompression){
        changeStateToProcessABlock();
      }
    }
  }

  /**
   * Returns the number of bytes between the current stream position
   * and the immediate next BZip2 block marker.
   *
   * @param in
   *             The InputStream
   *
   * @return long Number of bytes between current stream position and the
   * next BZip2 block start marker.
 * @throws IOException
   *
   */
  public static long numberOfBytesTillNextMarker(final InputStream in) throws IOException{
    CBZip2InputStream.skipDecompression = true;
    CBZip2InputStream anObject = null;

    anObject = new CBZip2InputStream(in, READ_MODE.BYBLOCK);

    return anObject.getProcessedByteCount();
  }

  public CBZip2InputStream(final InputStream in) throws IOException {
    this(in, READ_MODE.CONTINUOUS);
  }

  private void changeStateToProcessABlock() throws IOException {
    if (skipResult == true) {
      initBlock();
      setupBlock();
    } else {
      this.currentState = STATE.EOF;
    }
  }


  public int read() throws IOException {

    if (this.in != null) {
      int result = this.read(array, 0, 1);
      int value = 0XFF & array[0];
      return (result > 0 ? value : result);

    } else {
      throw new IOException("stream closed");
    }
  }

  /**
   * In CONTINOUS reading mode, this read method starts from the
   * start of the compressed stream and end at the end of file by
   * emitting un-compressed data.  In this mode stream positioning
   * is not announced and should be ignored.
   *
   * In BYBLOCK reading mode, this read method informs about the end
   * of a BZip2 block by returning EOB.  At this event, the compressed
   * stream position is also announced.  This announcement tells that
   * how much of the compressed stream has been de-compressed and read
   * out of this class.  In between EOB events, the stream position is
   * not updated.
   *
   *
   * @throws IOException
   *             if the stream content is malformed or an I/O error occurs.
   *
   * @return int The return value greater than 0 are the bytes read.  A value
   * of -1 means end of stream while -2 represents end of block
   */


  public int read(final byte[] dest, final int offs, final int len)
      throws IOException {
    if (offs < 0) {
      throw new IndexOutOfBoundsException("offs(" + offs + ") < 0.");
    }
    if (len < 0) {
      throw new IndexOutOfBoundsException("len(" + len + ") < 0.");
    }
    if (offs + len > dest.length) {
      throw new IndexOutOfBoundsException("offs(" + offs + ") + len("
          + len + ") > dest.length(" + dest.length + ").");
    }
    if (this.in == null) {
      throw new IOException("stream closed");
    }

    if(lazyInitialization){
      this.init();
      this.lazyInitialization = false;
    }

    if(skipDecompression){
      changeStateToProcessABlock();
      CBZip2InputStream.skipDecompression = false;
    }

    final int hi = offs + len;
    int destOffs = offs;
    int b = 0;



    for (; ((destOffs < hi) && ((b = read0())) >= 0);) {
      dest[destOffs++] = (byte) b;

    }

    int result = destOffs - offs;
    if (result == 0) {
      //report 'end of block' or 'end of stream'
      result = b;

      skipResult = this.skipToNextMarker(CBZip2InputStream.BLOCK_DELIMITER, DELIMITER_BIT_LENGTH);
      //Exactly when we are about to start a new block, we advertise the stream position.
      this.reportedBytesReadFromCompressedStream = this.bytesReadFromCompressedStream;

      changeStateToProcessABlock();
    }
    return result;
  }

  private int read0() throws IOException {
    final int retChar = this.currentChar;

    switch (this.currentState) {
    case EOF:
      return END_OF_STREAM;// return -1

    case NO_PROCESS_STATE:
      return END_OF_BLOCK;// return -2

    case START_BLOCK_STATE:
      throw new IllegalStateException();

    case RAND_PART_A_STATE:
      throw new IllegalStateException();

    case RAND_PART_B_STATE:
      setupRandPartB();
      break;

    case RAND_PART_C_STATE:
      setupRandPartC();
      break;

    case NO_RAND_PART_A_STATE:
      throw new IllegalStateException();

    case NO_RAND_PART_B_STATE:
      setupNoRandPartB();
      break;

    case NO_RAND_PART_C_STATE:
      setupNoRandPartC();
      break;

    default:
      throw new IllegalStateException();
    }

    return retChar;
  }

  private void init() throws IOException {
    int magic2 = this.readAByte(in);
    if (magic2 != 'h') {
      throw new IOException("Stream is not BZip2 formatted: expected 'h'"
          + " as first byte but got '" + (char) magic2 + "'");
    }

    int blockSize = this.readAByte(in);
    if ((blockSize < '1') || (blockSize > '9')) {
      throw new IOException("Stream is not BZip2 formatted: illegal "
          + "blocksize " + (char) blockSize);
    }

    this.blockSize100k = blockSize - '0';

    initBlock();
    setupBlock();
  }

  private void initBlock() throws IOException {
    if (this.readMode == READ_MODE.BYBLOCK) {
      // this.checkBlockIntegrity();
      this.storedBlockCRC = bsGetInt();
      this.blockRandomised = bsR(1) == 1;

      /**
      * Allocate data here instead in constructor, so we do not allocate
      * it if the input file is empty.
      */
      if (this.data == null) {
        this.data = new Data(this.blockSize100k);
      }

      // currBlockNo++;
      getAndMoveToFrontDecode();

      this.crc.initialiseCRC();
      this.currentState = STATE.START_BLOCK_STATE;
      return;
    }

    char magic0 = bsGetUByte();
    char magic1 = bsGetUByte();
    char magic2 = bsGetUByte();
    char magic3 = bsGetUByte();
    char magic4 = bsGetUByte();
    char magic5 = bsGetUByte();

    if (magic0 == 0x17 && magic1 == 0x72 && magic2 == 0x45
        && magic3 == 0x38 && magic4 == 0x50 && magic5 == 0x90) {
      complete(); // end of file
    } else if (magic0 != 0x31 || // '1'
        magic1 != 0x41 || // ')'
        magic2 != 0x59 || // 'Y'
        magic3 != 0x26 || // '&'
        magic4 != 0x53 || // 'S'
        magic5 != 0x59 // 'Y'
    ) {
      this.currentState = STATE.EOF;
      throw new IOException("bad block header");
    } else {
      this.storedBlockCRC = bsGetInt();
      this.blockRandomised = bsR(1) == 1;

      /**
      * Allocate data here instead in constructor, so we do not allocate
      * it if the input file is empty.
      */
      if (this.data == null) {
        this.data = new Data(this.blockSize100k);
      }

      // currBlockNo++;
      getAndMoveToFrontDecode();

      this.crc.initialiseCRC();
      this.currentState = STATE.START_BLOCK_STATE;
    }
  }

  private void endBlock() throws IOException {
    this.computedBlockCRC = this.crc.getFinalCRC();

    // A bad CRC is considered a fatal error.
    if (this.storedBlockCRC != this.computedBlockCRC) {
      // make next blocks readable without error
      // (repair feature, not yet documented, not tested)
      this.computedCombinedCRC = (this.storedCombinedCRC << 1)
          | (this.storedCombinedCRC >>> 31);
      this.computedCombinedCRC ^= this.storedBlockCRC;

      reportCRCError();
    }

    this.computedCombinedCRC = (this.computedCombinedCRC << 1)
        | (this.computedCombinedCRC >>> 31);
    this.computedCombinedCRC ^= this.computedBlockCRC;
  }

  private void complete() throws IOException {
    this.storedCombinedCRC = bsGetInt();
    this.currentState = STATE.EOF;
    this.data = null;

    if (this.storedCombinedCRC != this.computedCombinedCRC) {
      reportCRCError();
    }
  }

  public void close() throws IOException {
    InputStream inShadow = this.in;
    if (inShadow != null) {
      try {
        if (inShadow != System.in) {
          inShadow.close();
        }
      } finally {
        this.data = null;
        this.in = null;
      }
    }
  }

  private long bsR(final long n) throws IOException {
    long bsLiveShadow = this.bsLive;
    long bsBuffShadow = this.bsBuff;

    if (bsLiveShadow < n) {
      final InputStream inShadow = this.in;
      do {
        int thech = readAByte(inShadow);

        if (thech < 0) {
          throw new IOException("unexpected end of stream");
        }

        bsBuffShadow = (bsBuffShadow << 8) | thech;
        bsLiveShadow += 8;
      } while (bsLiveShadow < n);

      this.bsBuff = bsBuffShadow;
    }

    this.bsLive = bsLiveShadow - n;
    return (bsBuffShadow >> (bsLiveShadow - n)) & ((1L << n) - 1);
  }

  private boolean bsGetBit() throws IOException {
    long bsLiveShadow = this.bsLive;
    long bsBuffShadow = this.bsBuff;

    if (bsLiveShadow < 1) {
      int thech = this.readAByte(in);

      if (thech < 0) {
        throw new IOException("unexpected end of stream");
      }

      bsBuffShadow = (bsBuffShadow << 8) | thech;
      bsLiveShadow += 8;
      this.bsBuff = bsBuffShadow;
    }

    this.bsLive = bsLiveShadow - 1;
    return ((bsBuffShadow >> (bsLiveShadow - 1)) & 1) != 0;
  }

  private char bsGetUByte() throws IOException {
    return (char) bsR(8);
  }

  private int bsGetInt() throws IOException {
    return (int) ((((((bsR(8) << 8) | bsR(8)) << 8) | bsR(8)) << 8) | bsR(8));
  }

  /**
  * Called by createHuffmanDecodingTables() exclusively.
  */
  private static void hbCreateDecodeTables(final int[] limit,
      final int[] base, final int[] perm, final char[] length,
      final int minLen, final int maxLen, final int alphaSize) {
    for (int i = minLen, pp = 0; i <= maxLen; i++) {
      for (int j = 0; j < alphaSize; j++) {
        if (length[j] == i) {
          perm[pp++] = j;
        }
      }
    }

    for (int i = MAX_CODE_LEN; --i > 0;) {
      base[i] = 0;
      limit[i] = 0;
    }

    for (int i = 0; i < alphaSize; i++) {
      base[length[i] + 1]++;
    }

    for (int i = 1, b = base[0]; i < MAX_CODE_LEN; i++) {
      b += base[i];
      base[i] = b;
    }

    for (int i = minLen, vec = 0, b = base[i]; i <= maxLen; i++) {
      final int nb = base[i + 1];
      vec += nb - b;
      b = nb;
      limit[i] = vec - 1;
      vec <<= 1;
    }

    for (int i = minLen + 1; i <= maxLen; i++) {
      base[i] = ((limit[i - 1] + 1) << 1) - base[i];
    }
  }

  private void recvDecodingTables() throws IOException {
    final Data dataShadow = this.data;
    final boolean[] inUse = dataShadow.inUse;
    final byte[] pos = dataShadow.recvDecodingTables_pos;
    final byte[] selector = dataShadow.selector;
    final byte[] selectorMtf = dataShadow.selectorMtf;

    int inUse16 = 0;

    /* Receive the mapping table */
    for (int i = 0; i < 16; i++) {
      if (bsGetBit()) {
        inUse16 |= 1 << i;
      }
    }

    for (int i = 256; --i >= 0;) {
      inUse[i] = false;
    }

    for (int i = 0; i < 16; i++) {
      if ((inUse16 & (1 << i)) != 0) {
        final int i16 = i << 4;
        for (int j = 0; j < 16; j++) {
          if (bsGetBit()) {
            inUse[i16 + j] = true;
          }
        }
      }
    }

    makeMaps();
    final int alphaSize = this.nInUse + 2;

    /* Now the selectors */
    final int nGroups = (int) bsR(3);
    final int nSelectors = (int) bsR(15);

    for (int i = 0; i < nSelectors; i++) {
      int j = 0;
      while (bsGetBit()) {
        j++;
      }
      selectorMtf[i] = (byte) j;
    }

    /* Undo the MTF values for the selectors. */
    for (int v = nGroups; --v >= 0;) {
      pos[v] = (byte) v;
    }

    for (int i = 0; i < nSelectors; i++) {
      int v = selectorMtf[i] & 0xff;
      final byte tmp = pos[v];
      while (v > 0) {
        // nearly all times v is zero, 4 in most other cases
        pos[v] = pos[v - 1];
        v--;
      }
      pos[0] = tmp;
      selector[i] = tmp;
    }

    final char[][] len = dataShadow.temp_charArray2d;

    /* Now the coding tables */
    for (int t = 0; t < nGroups; t++) {
      int curr = (int) bsR(5);
      final char[] len_t = len[t];
      for (int i = 0; i < alphaSize; i++) {
        while (bsGetBit()) {
          curr += bsGetBit() ? -1 : 1;
        }
        len_t[i] = (char) curr;
      }
    }

    // finally create the Huffman tables
    createHuffmanDecodingTables(alphaSize, nGroups);
  }

  /**
  * Called by recvDecodingTables() exclusively.
  */
  private void createHuffmanDecodingTables(final int alphaSize,
      final int nGroups) {
    final Data dataShadow = this.data;
    final char[][] len = dataShadow.temp_charArray2d;
    final int[] minLens = dataShadow.minLens;
    final int[][] limit = dataShadow.limit;
    final int[][] base = dataShadow.base;
    final int[][] perm = dataShadow.perm;

    for (int t = 0; t < nGroups; t++) {
      int minLen = 32;
      int maxLen = 0;
      final char[] len_t = len[t];
      for (int i = alphaSize; --i >= 0;) {
        final char lent = len_t[i];
        if (lent > maxLen) {
          maxLen = lent;
        }
        if (lent < minLen) {
          minLen = lent;
        }
      }
      hbCreateDecodeTables(limit[t], base[t], perm[t], len[t], minLen,
          maxLen, alphaSize);
      minLens[t] = minLen;
    }
  }

  private void getAndMoveToFrontDecode() throws IOException {
    this.origPtr = (int) bsR(24);
    recvDecodingTables();

    final InputStream inShadow = this.in;
    final Data dataShadow = this.data;
    final byte[] ll8 = dataShadow.ll8;
    final int[] unzftab = dataShadow.unzftab;
    final byte[] selector = dataShadow.selector;
    final byte[] seqToUnseq = dataShadow.seqToUnseq;
    final char[] yy = dataShadow.getAndMoveToFrontDecode_yy;
    final int[] minLens = dataShadow.minLens;
    final int[][] limit = dataShadow.limit;
    final int[][] base = dataShadow.base;
    final int[][] perm = dataShadow.perm;
    final int limitLast = this.blockSize100k * 100000;

    /*
    * Setting up the unzftab entries here is not strictly necessary, but it
    * does save having to do it later in a separate pass, and so saves a
    * block's worth of cache misses.
    */
    for (int i = 256; --i >= 0;) {
      yy[i] = (char) i;
      unzftab[i] = 0;
    }

    int groupNo = 0;
    int groupPos = G_SIZE - 1;
    final int eob = this.nInUse + 1;
    int nextSym = getAndMoveToFrontDecode0(0);
    int bsBuffShadow = (int) this.bsBuff;
    int bsLiveShadow = (int) this.bsLive;
    int lastShadow = -1;
    int zt = selector[groupNo] & 0xff;
    int[] base_zt = base[zt];
    int[] limit_zt = limit[zt];
    int[] perm_zt = perm[zt];
    int minLens_zt = minLens[zt];

    while (nextSym != eob) {
      if ((nextSym == RUNA) || (nextSym == RUNB)) {
        int s = -1;

        for (int n = 1; true; n <<= 1) {
          if (nextSym == RUNA) {
            s += n;
          } else if (nextSym == RUNB) {
            s += n << 1;
          } else {
            break;
          }

          if (groupPos == 0) {
            groupPos = G_SIZE - 1;
            zt = selector[++groupNo] & 0xff;
            base_zt = base[zt];
            limit_zt = limit[zt];
            perm_zt = perm[zt];
            minLens_zt = minLens[zt];
          } else {
            groupPos--;
          }

          int zn = minLens_zt;

          while (bsLiveShadow < zn) {
            final int thech = readAByte(inShadow);
            if (thech >= 0) {
              bsBuffShadow = (bsBuffShadow << 8) | thech;
              bsLiveShadow += 8;
              continue;
            } else {
              throw new IOException("unexpected end of stream");
            }
          }
          long zvec = (bsBuffShadow >> (bsLiveShadow - zn))
              & ((1 << zn) - 1);
          bsLiveShadow -= zn;

          while (zvec > limit_zt[zn]) {
            zn++;
            while (bsLiveShadow < 1) {
              final int thech = readAByte(inShadow);
              if (thech >= 0) {
                bsBuffShadow = (bsBuffShadow << 8) | thech;
                bsLiveShadow += 8;
                continue;
              } else {
                throw new IOException(
                    "unexpected end of stream");
              }
            }
            bsLiveShadow--;
            zvec = (zvec << 1)
                | ((bsBuffShadow >> bsLiveShadow) & 1);
          }
          nextSym = perm_zt[(int) (zvec - base_zt[zn])];
        }

        final byte ch = seqToUnseq[yy[0]];
        unzftab[ch & 0xff] += s + 1;

        while (s-- >= 0) {
          ll8[++lastShadow] = ch;
        }

        if (lastShadow >= limitLast) {
          throw new IOException("block overrun");
        }
      } else {
        if (++lastShadow >= limitLast) {
          throw new IOException("block overrun");
        }

        final char tmp = yy[nextSym - 1];
        unzftab[seqToUnseq[tmp] & 0xff]++;
        ll8[lastShadow] = seqToUnseq[tmp];

        /*
        * This loop is hammered during decompression, hence avoid
        * native method call overhead of System.arraycopy for very
        * small ranges to copy.
        */
        if (nextSym <= 16) {
          for (int j = nextSym - 1; j > 0;) {
            yy[j] = yy[--j];
          }
        } else {
          System.arraycopy(yy, 0, yy, 1, nextSym - 1);
        }

        yy[0] = tmp;

        if (groupPos == 0) {
          groupPos = G_SIZE - 1;
          zt = selector[++groupNo] & 0xff;
          base_zt = base[zt];
          limit_zt = limit[zt];
          perm_zt = perm[zt];
          minLens_zt = minLens[zt];
        } else {
          groupPos--;
        }

        int zn = minLens_zt;

        while (bsLiveShadow < zn) {
          final int thech = readAByte(inShadow);
          if (thech >= 0) {
            bsBuffShadow = (bsBuffShadow << 8) | thech;
            bsLiveShadow += 8;
            continue;
          } else {
            throw new IOException("unexpected end of stream");
          }
        }
        int zvec = (bsBuffShadow >> (bsLiveShadow - zn))
            & ((1 << zn) - 1);
        bsLiveShadow -= zn;

        while (zvec > limit_zt[zn]) {
          zn++;
          while (bsLiveShadow < 1) {
            final int thech = readAByte(inShadow);
            if (thech >= 0) {
              bsBuffShadow = (bsBuffShadow << 8) | thech;
              bsLiveShadow += 8;
              continue;
            } else {
              throw new IOException("unexpected end of stream");
            }
          }
          bsLiveShadow--;
          zvec = ((zvec << 1) | ((bsBuffShadow >> bsLiveShadow) & 1));
        }
        nextSym = perm_zt[zvec - base_zt[zn]];
      }
    }

    this.last = lastShadow;
    this.bsLive = bsLiveShadow;
    this.bsBuff = bsBuffShadow;
  }

  private int getAndMoveToFrontDecode0(final int groupNo) throws IOException {
    final InputStream inShadow = this.in;
    final Data dataShadow = this.data;
    final int zt = dataShadow.selector[groupNo] & 0xff;
    final int[] limit_zt = dataShadow.limit[zt];
    int zn = dataShadow.minLens[zt];
    int zvec = (int) bsR(zn);
    int bsLiveShadow = (int) this.bsLive;
    int bsBuffShadow = (int) this.bsBuff;

    while (zvec > limit_zt[zn]) {
      zn++;
      while (bsLiveShadow < 1) {
        final int thech = readAByte(inShadow);

        if (thech >= 0) {
          bsBuffShadow = (bsBuffShadow << 8) | thech;
          bsLiveShadow += 8;
          continue;
        } else {
          throw new IOException("unexpected end of stream");
        }
      }
      bsLiveShadow--;
      zvec = (zvec << 1) | ((bsBuffShadow >> bsLiveShadow) & 1);
    }

    this.bsLive = bsLiveShadow;
    this.bsBuff = bsBuffShadow;

    return dataShadow.perm[zt][zvec - dataShadow.base[zt][zn]];
  }

  private void setupBlock() throws IOException {
    if (this.data == null) {
      return;
    }

    final int[] cftab = this.data.cftab;
    final int[] tt = this.data.initTT(this.last + 1);
    final byte[] ll8 = this.data.ll8;
    cftab[0] = 0;
    System.arraycopy(this.data.unzftab, 0, cftab, 1, 256);

    for (int i = 1, c = cftab[0]; i <= 256; i++) {
      c += cftab[i];
      cftab[i] = c;
    }

    for (int i = 0, lastShadow = this.last; i <= lastShadow; i++) {
      tt[cftab[ll8[i] & 0xff]++] = i;
    }

    if ((this.origPtr < 0) || (this.origPtr >= tt.length)) {
      throw new IOException("stream corrupted");
    }

    this.su_tPos = tt[this.origPtr];
    this.su_count = 0;
    this.su_i2 = 0;
    this.su_ch2 = 256; /* not a char and not EOF */

    if (this.blockRandomised) {
      this.su_rNToGo = 0;
      this.su_rTPos = 0;
      setupRandPartA();
    } else {
      setupNoRandPartA();
    }
  }

  private void setupRandPartA() throws IOException {
    if (this.su_i2 <= this.last) {
      this.su_chPrev = this.su_ch2;
      int su_ch2Shadow = this.data.ll8[this.su_tPos] & 0xff;
      this.su_tPos = this.data.tt[this.su_tPos];
      if (this.su_rNToGo == 0) {
        this.su_rNToGo = BZip2Constants.rNums[this.su_rTPos] - 1;
        if (++this.su_rTPos == 512) {
          this.su_rTPos = 0;
        }
      } else {
        this.su_rNToGo--;
      }
      this.su_ch2 = su_ch2Shadow ^= (this.su_rNToGo == 1) ? 1 : 0;
      this.su_i2++;
      this.currentChar = su_ch2Shadow;
      this.currentState = STATE.RAND_PART_B_STATE;
      this.crc.updateCRC(su_ch2Shadow);
    } else {
      endBlock();
      if (readMode == READ_MODE.CONTINUOUS) {
      initBlock();
      setupBlock();
      } else if (readMode == READ_MODE.BYBLOCK) {
        this.currentState = STATE.NO_PROCESS_STATE;
      }
    }
  }

  private void setupNoRandPartA() throws IOException {
    if (this.su_i2 <= this.last) {
      this.su_chPrev = this.su_ch2;
      int su_ch2Shadow = this.data.ll8[this.su_tPos] & 0xff;
      this.su_ch2 = su_ch2Shadow;
      this.su_tPos = this.data.tt[this.su_tPos];
      this.su_i2++;
      this.currentChar = su_ch2Shadow;
      this.currentState = STATE.NO_RAND_PART_B_STATE;
      this.crc.updateCRC(su_ch2Shadow);
    } else {
      this.currentState = STATE.NO_RAND_PART_A_STATE;
      endBlock();
      if (readMode == READ_MODE.CONTINUOUS) {
      initBlock();
      setupBlock();
      } else if (readMode == READ_MODE.BYBLOCK) {
        this.currentState = STATE.NO_PROCESS_STATE;
      }
    }
  }

  private void setupRandPartB() throws IOException {
    if (this.su_ch2 != this.su_chPrev) {
      this.currentState = STATE.RAND_PART_A_STATE;
      this.su_count = 1;
      setupRandPartA();
    } else if (++this.su_count >= 4) {
      this.su_z = (char) (this.data.ll8[this.su_tPos] & 0xff);
      this.su_tPos = this.data.tt[this.su_tPos];
      if (this.su_rNToGo == 0) {
        this.su_rNToGo = BZip2Constants.rNums[this.su_rTPos] - 1;
        if (++this.su_rTPos == 512) {
          this.su_rTPos = 0;
        }
      } else {
        this.su_rNToGo--;
      }
      this.su_j2 = 0;
      this.currentState = STATE.RAND_PART_C_STATE;
      if (this.su_rNToGo == 1) {
        this.su_z ^= 1;
      }
      setupRandPartC();
    } else {
      this.currentState = STATE.RAND_PART_A_STATE;
      setupRandPartA();
    }
  }

  private void setupRandPartC() throws IOException {
    if (this.su_j2 < this.su_z) {
      this.currentChar = this.su_ch2;
      this.crc.updateCRC(this.su_ch2);
      this.su_j2++;
    } else {
      this.currentState = STATE.RAND_PART_A_STATE;
      this.su_i2++;
      this.su_count = 0;
      setupRandPartA();
    }
  }

  private void setupNoRandPartB() throws IOException {
    if (this.su_ch2 != this.su_chPrev) {
      this.su_count = 1;
      setupNoRandPartA();
    } else if (++this.su_count >= 4) {
      this.su_z = (char) (this.data.ll8[this.su_tPos] & 0xff);
      this.su_tPos = this.data.tt[this.su_tPos];
      this.su_j2 = 0;
      setupNoRandPartC();
    } else {
      setupNoRandPartA();
    }
  }

  private void setupNoRandPartC() throws IOException {
    if (this.su_j2 < this.su_z) {
      int su_ch2Shadow = this.su_ch2;
      this.currentChar = su_ch2Shadow;
      this.crc.updateCRC(su_ch2Shadow);
      this.su_j2++;
      this.currentState = STATE.NO_RAND_PART_C_STATE;
    } else {
      this.su_i2++;
      this.su_count = 0;
      setupNoRandPartA();
    }
  }

  private static final class Data extends Object {

    // (with blockSize 900k)
    final boolean[] inUse = new boolean[256]; // 256 byte

    final byte[] seqToUnseq = new byte[256]; // 256 byte
    final byte[] selector = new byte[MAX_SELECTORS]; // 18002 byte
    final byte[] selectorMtf = new byte[MAX_SELECTORS]; // 18002 byte

    /**
    * Freq table collected to save a pass over the data during
    * decompression.
    */
    final int[] unzftab = new int[256]; // 1024 byte

    final int[][] limit = new int[N_GROUPS][MAX_ALPHA_SIZE]; // 6192 byte
    final int[][] base = new int[N_GROUPS][MAX_ALPHA_SIZE]; // 6192 byte
    final int[][] perm = new int[N_GROUPS][MAX_ALPHA_SIZE]; // 6192 byte
    final int[] minLens = new int[N_GROUPS]; // 24 byte

    final int[] cftab = new int[257]; // 1028 byte
    final char[] getAndMoveToFrontDecode_yy = new char[256]; // 512 byte
    final char[][] temp_charArray2d = new char[N_GROUPS][MAX_ALPHA_SIZE]; // 3096
                                        // byte
    final byte[] recvDecodingTables_pos = new byte[N_GROUPS]; // 6 byte
    // ---------------
    // 60798 byte

    int[] tt; // 3600000 byte
    byte[] ll8; // 900000 byte

    // ---------------
    // 4560782 byte
    // ===============

    Data(int blockSize100k) {
      super();

      this.ll8 = new byte[blockSize100k * BZip2Constants.baseBlockSize];
    }

    /**
    * Initializes the {@link #tt} array.
    *
    * This method is called when the required length of the array is known.
    * I don't initialize it at construction time to avoid unneccessary
    * memory allocation when compressing small files.
    */
    final int[] initTT(int length) {
      int[] ttShadow = this.tt;

      // tt.length should always be >= length, but theoretically
      // it can happen, if the compressor mixed small and large
      // blocks. Normally only the last block will be smaller
      // than others.
      if ((ttShadow == null) || (ttShadow.length < length)) {
        this.tt = ttShadow = new int[length];
      }

      return ttShadow;
    }

  }
}
