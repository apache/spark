/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.io.file.tfile;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.file.tfile.CompareUtils.Scalar;
import org.apache.hadoop.io.file.tfile.CompareUtils.ScalarComparator;
import org.apache.hadoop.io.file.tfile.CompareUtils.ScalarLong;
import org.apache.hadoop.io.file.tfile.Compression.Algorithm;
import org.apache.hadoop.io.file.tfile.Utils.Version;

/**
 * Block Compressed file, the underlying physical storage layer for TFile.
 * BCFile provides the basic block level compression for the data block and meta
 * blocks. It is separated from TFile as it may be used for other
 * block-compressed file implementation.
 */
final class BCFile {
  // the current version of BCFile impl, increment them (major or minor) made
  // enough changes
  static final Version API_VERSION = new Version((short) 1, (short) 0);
  static final Log LOG = LogFactory.getLog(BCFile.class);

  /**
   * Prevent the instantiation of BCFile objects.
   */
  private BCFile() {
    // nothing
  }

  /**
   * BCFile writer, the entry point for creating a new BCFile.
   */
  static public class Writer implements Closeable {
    private final FSDataOutputStream out;
    private final Configuration conf;
    // the single meta block containing index of compressed data blocks
    final DataIndex dataIndex;
    // index for meta blocks
    final MetaIndex metaIndex;
    boolean blkInProgress = false;
    private boolean metaBlkSeen = false;
    private boolean closed = false;
    long errorCount = 0;
    // reusable buffers.
    private BytesWritable fsOutputBuffer;

    /**
     * Call-back interface to register a block after a block is closed.
     */
    private static interface BlockRegister {
      /**
       * Register a block that is fully closed.
       * 
       * @param raw
       *          The size of block in terms of uncompressed bytes.
       * @param offsetStart
       *          The start offset of the block.
       * @param offsetEnd
       *          One byte after the end of the block. Compressed block size is
       *          offsetEnd - offsetStart.
       */
      public void register(long raw, long offsetStart, long offsetEnd);
    }

    /**
     * Intermediate class that maintain the state of a Writable Compression
     * Block.
     */
    private static final class WBlockState {
      private final Algorithm compressAlgo;
      private Compressor compressor; // !null only if using native
      // Hadoop compression
      private final FSDataOutputStream fsOut;
      private final long posStart;
      private final SimpleBufferedOutputStream fsBufferedOutput;
      private OutputStream out;

      /**
       * @param compressionAlgo
       *          The compression algorithm to be used to for compression.
       * @throws IOException
       */
      public WBlockState(Algorithm compressionAlgo, FSDataOutputStream fsOut,
          BytesWritable fsOutputBuffer, Configuration conf) throws IOException {
        this.compressAlgo = compressionAlgo;
        this.fsOut = fsOut;
        this.posStart = fsOut.getPos();

        fsOutputBuffer.setCapacity(TFile.getFSOutputBufferSize(conf));

        this.fsBufferedOutput =
            new SimpleBufferedOutputStream(this.fsOut, fsOutputBuffer.get());
        this.compressor = compressAlgo.getCompressor();

        try {
          this.out =
              compressionAlgo.createCompressionStream(fsBufferedOutput,
                  compressor, 0);
        } catch (IOException e) {
          compressAlgo.returnCompressor(compressor);
          throw e;
        }
      }

      /**
       * Get the output stream for BlockAppender's consumption.
       * 
       * @return the output stream suitable for writing block data.
       */
      OutputStream getOutputStream() {
        return out;
      }

      /**
       * Get the current position in file.
       * 
       * @return The current byte offset in underlying file.
       * @throws IOException
       */
      long getCurrentPos() throws IOException {
        return fsOut.getPos() + fsBufferedOutput.size();
      }

      long getStartPos() {
        return posStart;
      }

      /**
       * Current size of compressed data.
       * 
       * @return
       * @throws IOException
       */
      long getCompressedSize() throws IOException {
        long ret = getCurrentPos() - posStart;
        return ret;
      }

      /**
       * Finishing up the current block.
       */
      public void finish() throws IOException {
        try {
          if (out != null) {
            out.flush();
            out = null;
          }
        } finally {
          compressAlgo.returnCompressor(compressor);
          compressor = null;
        }
      }
    }

    /**
     * Access point to stuff data into a block.
     * 
     * TODO: Change DataOutputStream to something else that tracks the size as
     * long instead of int. Currently, we will wrap around if the row block size
     * is greater than 4GB.
     */
    public class BlockAppender extends DataOutputStream {
      private final BlockRegister blockRegister;
      private final WBlockState wBlkState;
      @SuppressWarnings("hiding")
      private boolean closed = false;

      /**
       * Constructor
       * 
       * @param register
       *          the block register, which is called when the block is closed.
       * @param wbs
       *          The writable compression block state.
       */
      BlockAppender(BlockRegister register, WBlockState wbs) {
        super(wbs.getOutputStream());
        this.blockRegister = register;
        this.wBlkState = wbs;
      }

      /**
       * Get the raw size of the block.
       * 
       * @return the number of uncompressed bytes written through the
       *         BlockAppender so far.
       * @throws IOException
       */
      public long getRawSize() throws IOException {
        /**
         * Expecting the size() of a block not exceeding 4GB. Assuming the
         * size() will wrap to negative integer if it exceeds 2GB.
         */
        return size() & 0x00000000ffffffffL;
      }

      /**
       * Get the compressed size of the block in progress.
       * 
       * @return the number of compressed bytes written to the underlying FS
       *         file. The size may be smaller than actual need to compress the
       *         all data written due to internal buffering inside the
       *         compressor.
       * @throws IOException
       */
      public long getCompressedSize() throws IOException {
        return wBlkState.getCompressedSize();
      }

      @Override
      public void flush() {
        // The down stream is a special kind of stream that finishes a
        // compression block upon flush. So we disable flush() here.
      }

      /**
       * Signaling the end of write to the block. The block register will be
       * called for registering the finished block.
       */
      @Override
      public void close() throws IOException {
        if (closed == true) {
          return;
        }
        try {
          ++errorCount;
          wBlkState.finish();
          blockRegister.register(getRawSize(), wBlkState.getStartPos(),
              wBlkState.getCurrentPos());
          --errorCount;
        } finally {
          closed = true;
          blkInProgress = false;
        }
      }
    }

    /**
     * Constructor
     * 
     * @param fout
     *          FS output stream.
     * @param compressionName
     *          Name of the compression algorithm, which will be used for all
     *          data blocks.
     * @throws IOException
     * @see Compression#getSupportedAlgorithms
     */
    public Writer(FSDataOutputStream fout, String compressionName,
        Configuration conf) throws IOException {
      if (fout.getPos() != 0) {
        throw new IOException("Output file not at zero offset.");
      }

      this.out = fout;
      this.conf = conf;
      dataIndex = new DataIndex(compressionName);
      metaIndex = new MetaIndex();
      fsOutputBuffer = new BytesWritable();
      Magic.write(fout);
    }

    /**
     * Close the BCFile Writer. Attempting to use the Writer after calling
     * <code>close</code> is not allowed and may lead to undetermined results.
     */
    public void close() throws IOException {
      if (closed == true) {
        return;
      }

      try {
        if (errorCount == 0) {
          if (blkInProgress == true) {
            throw new IllegalStateException(
                "Close() called with active block appender.");
          }

          // add metaBCFileIndex to metaIndex as the last meta block
          BlockAppender appender =
              prepareMetaBlock(DataIndex.BLOCK_NAME,
                  getDefaultCompressionAlgorithm());
          try {
            dataIndex.write(appender);
          } finally {
            appender.close();
          }

          long offsetIndexMeta = out.getPos();
          metaIndex.write(out);

          // Meta Index and the trailing section are written out directly.
          out.writeLong(offsetIndexMeta);

          API_VERSION.write(out);
          Magic.write(out);
          out.flush();
        }
      } finally {
        closed = true;
      }
    }

    private Algorithm getDefaultCompressionAlgorithm() {
      return dataIndex.getDefaultCompressionAlgorithm();
    }

    private BlockAppender prepareMetaBlock(String name, Algorithm compressAlgo)
        throws IOException, MetaBlockAlreadyExists {
      if (blkInProgress == true) {
        throw new IllegalStateException(
            "Cannot create Meta Block until previous block is closed.");
      }

      if (metaIndex.getMetaByName(name) != null) {
        throw new MetaBlockAlreadyExists("name=" + name);
      }

      MetaBlockRegister mbr = new MetaBlockRegister(name, compressAlgo);
      WBlockState wbs =
          new WBlockState(compressAlgo, out, fsOutputBuffer, conf);
      BlockAppender ba = new BlockAppender(mbr, wbs);
      blkInProgress = true;
      metaBlkSeen = true;
      return ba;
    }

    /**
     * Create a Meta Block and obtain an output stream for adding data into the
     * block. There can only be one BlockAppender stream active at any time.
     * Regular Blocks may not be created after the first Meta Blocks. The caller
     * must call BlockAppender.close() to conclude the block creation.
     * 
     * @param name
     *          The name of the Meta Block. The name must not conflict with
     *          existing Meta Blocks.
     * @param compressionName
     *          The name of the compression algorithm to be used.
     * @return The BlockAppender stream
     * @throws IOException
     * @throws MetaBlockAlreadyExists
     *           If the meta block with the name already exists.
     */
    public BlockAppender prepareMetaBlock(String name, String compressionName)
        throws IOException, MetaBlockAlreadyExists {
      return prepareMetaBlock(name, Compression
          .getCompressionAlgorithmByName(compressionName));
    }

    /**
     * Create a Meta Block and obtain an output stream for adding data into the
     * block. The Meta Block will be compressed with the same compression
     * algorithm as data blocks. There can only be one BlockAppender stream
     * active at any time. Regular Blocks may not be created after the first
     * Meta Blocks. The caller must call BlockAppender.close() to conclude the
     * block creation.
     * 
     * @param name
     *          The name of the Meta Block. The name must not conflict with
     *          existing Meta Blocks.
     * @return The BlockAppender stream
     * @throws MetaBlockAlreadyExists
     *           If the meta block with the name already exists.
     * @throws IOException
     */
    public BlockAppender prepareMetaBlock(String name) throws IOException,
        MetaBlockAlreadyExists {
      return prepareMetaBlock(name, getDefaultCompressionAlgorithm());
    }

    /**
     * Create a Data Block and obtain an output stream for adding data into the
     * block. There can only be one BlockAppender stream active at any time.
     * Data Blocks may not be created after the first Meta Blocks. The caller
     * must call BlockAppender.close() to conclude the block creation.
     * 
     * @return The BlockAppender stream
     * @throws IOException
     */
    public BlockAppender prepareDataBlock() throws IOException {
      if (blkInProgress == true) {
        throw new IllegalStateException(
            "Cannot create Data Block until previous block is closed.");
      }

      if (metaBlkSeen == true) {
        throw new IllegalStateException(
            "Cannot create Data Block after Meta Blocks.");
      }

      DataBlockRegister dbr = new DataBlockRegister();

      WBlockState wbs =
          new WBlockState(getDefaultCompressionAlgorithm(), out,
              fsOutputBuffer, conf);
      BlockAppender ba = new BlockAppender(dbr, wbs);
      blkInProgress = true;
      return ba;
    }

    /**
     * Callback to make sure a meta block is added to the internal list when its
     * stream is closed.
     */
    private class MetaBlockRegister implements BlockRegister {
      private final String name;
      private final Algorithm compressAlgo;

      MetaBlockRegister(String name, Algorithm compressAlgo) {
        this.name = name;
        this.compressAlgo = compressAlgo;
      }

      public void register(long raw, long begin, long end) {
        metaIndex.addEntry(new MetaIndexEntry(name, compressAlgo,
            new BlockRegion(begin, end - begin, raw)));
      }
    }

    /**
     * Callback to make sure a data block is added to the internal list when
     * it's being closed.
     * 
     */
    private class DataBlockRegister implements BlockRegister {
      DataBlockRegister() {
        // do nothing
      }

      public void register(long raw, long begin, long end) {
        dataIndex.addBlockRegion(new BlockRegion(begin, end - begin, raw));
      }
    }
  }

  /**
   * BCFile Reader, interface to read the file's data and meta blocks.
   */
  static public class Reader implements Closeable {
    private final FSDataInputStream in;
    private final Configuration conf;
    final DataIndex dataIndex;
    // Index for meta blocks
    final MetaIndex metaIndex;
    final Version version;

    /**
     * Intermediate class that maintain the state of a Readable Compression
     * Block.
     */
    static private final class RBlockState {
      private final Algorithm compressAlgo;
      private Decompressor decompressor;
      private final BlockRegion region;
      private final InputStream in;

      public RBlockState(Algorithm compressionAlgo, FSDataInputStream fsin,
          BlockRegion region, Configuration conf) throws IOException {
        this.compressAlgo = compressionAlgo;
        this.region = region;
        this.decompressor = compressionAlgo.getDecompressor();

        try {
          this.in =
              compressAlgo
                  .createDecompressionStream(new BoundedRangeFileInputStream(
                      fsin, this.region.getOffset(), this.region
                          .getCompressedSize()), decompressor, TFile
                      .getFSInputBufferSize(conf));
        } catch (IOException e) {
          compressAlgo.returnDecompressor(decompressor);
          throw e;
        }
      }

      /**
       * Get the output stream for BlockAppender's consumption.
       * 
       * @return the output stream suitable for writing block data.
       */
      public InputStream getInputStream() {
        return in;
      }

      public String getCompressionName() {
        return compressAlgo.getName();
      }

      public BlockRegion getBlockRegion() {
        return region;
      }

      public void finish() throws IOException {
        try {
          in.close();
        } finally {
          compressAlgo.returnDecompressor(decompressor);
          decompressor = null;
        }
      }
    }

    /**
     * Access point to read a block.
     */
    public static class BlockReader extends DataInputStream {
      private final RBlockState rBlkState;
      private boolean closed = false;

      BlockReader(RBlockState rbs) {
        super(rbs.getInputStream());
        rBlkState = rbs;
      }

      /**
       * Finishing reading the block. Release all resources.
       */
      @Override
      public void close() throws IOException {
        if (closed == true) {
          return;
        }
        try {
          // Do not set rBlkState to null. People may access stats after calling
          // close().
          rBlkState.finish();
        } finally {
          closed = true;
        }
      }

      /**
       * Get the name of the compression algorithm used to compress the block.
       * 
       * @return name of the compression algorithm.
       */
      public String getCompressionName() {
        return rBlkState.getCompressionName();
      }

      /**
       * Get the uncompressed size of the block.
       * 
       * @return uncompressed size of the block.
       */
      public long getRawSize() {
        return rBlkState.getBlockRegion().getRawSize();
      }

      /**
       * Get the compressed size of the block.
       * 
       * @return compressed size of the block.
       */
      public long getCompressedSize() {
        return rBlkState.getBlockRegion().getCompressedSize();
      }

      /**
       * Get the starting position of the block in the file.
       * 
       * @return the starting position of the block in the file.
       */
      public long getStartPos() {
        return rBlkState.getBlockRegion().getOffset();
      }
    }

    /**
     * Constructor
     * 
     * @param fin
     *          FS input stream.
     * @param fileLength
     *          Length of the corresponding file
     * @throws IOException
     */
    public Reader(FSDataInputStream fin, long fileLength, Configuration conf)
        throws IOException {
      this.in = fin;
      this.conf = conf;

      // move the cursor to the beginning of the tail, containing: offset to the
      // meta block index, version and magic
      fin.seek(fileLength - Magic.size() - Version.size() - Long.SIZE
          / Byte.SIZE);
      long offsetIndexMeta = fin.readLong();
      version = new Version(fin);
      Magic.readAndVerify(fin);

      if (!version.compatibleWith(BCFile.API_VERSION)) {
        throw new RuntimeException("Incompatible BCFile fileBCFileVersion.");
      }

      // read meta index
      fin.seek(offsetIndexMeta);
      metaIndex = new MetaIndex(fin);

      // read data:BCFile.index, the data block index
      BlockReader blockR = getMetaBlock(DataIndex.BLOCK_NAME);
      try {
        dataIndex = new DataIndex(blockR);
      } finally {
        blockR.close();
      }
    }

    /**
     * Get the name of the default compression algorithm.
     * 
     * @return the name of the default compression algorithm.
     */
    public String getDefaultCompressionName() {
      return dataIndex.getDefaultCompressionAlgorithm().getName();
    }

    /**
     * Get version of BCFile file being read.
     * 
     * @return version of BCFile file being read.
     */
    public Version getBCFileVersion() {
      return version;
    }

    /**
     * Get version of BCFile API.
     * 
     * @return version of BCFile API.
     */
    public Version getAPIVersion() {
      return API_VERSION;
    }

    /**
     * Finishing reading the BCFile. Release all resources.
     */
    public void close() {
      // nothing to be done now
    }

    /**
     * Get the number of data blocks.
     * 
     * @return the number of data blocks.
     */
    public int getBlockCount() {
      return dataIndex.getBlockRegionList().size();
    }

    /**
     * Stream access to a Meta Block.
     * 
     * @param name
     *          meta block name
     * @return BlockReader input stream for reading the meta block.
     * @throws IOException
     * @throws MetaBlockDoesNotExist
     *           The Meta Block with the given name does not exist.
     */
    public BlockReader getMetaBlock(String name) throws IOException,
        MetaBlockDoesNotExist {
      MetaIndexEntry imeBCIndex = metaIndex.getMetaByName(name);
      if (imeBCIndex == null) {
        throw new MetaBlockDoesNotExist("name=" + name);
      }

      BlockRegion region = imeBCIndex.getRegion();
      return createReader(imeBCIndex.getCompressionAlgorithm(), region);
    }

    /**
     * Stream access to a Data Block.
     * 
     * @param blockIndex
     *          0-based data block index.
     * @return BlockReader input stream for reading the data block.
     * @throws IOException
     */
    public BlockReader getDataBlock(int blockIndex) throws IOException {
      if (blockIndex < 0 || blockIndex >= getBlockCount()) {
        throw new IndexOutOfBoundsException(String.format(
            "blockIndex=%d, numBlocks=%d", blockIndex, getBlockCount()));
      }

      BlockRegion region = dataIndex.getBlockRegionList().get(blockIndex);
      return createReader(dataIndex.getDefaultCompressionAlgorithm(), region);
    }

    private BlockReader createReader(Algorithm compressAlgo, BlockRegion region)
        throws IOException {
      RBlockState rbs = new RBlockState(compressAlgo, in, region, conf);
      return new BlockReader(rbs);
    }

    /**
     * Find the smallest Block index whose starting offset is greater than or
     * equal to the specified offset.
     * 
     * @param offset
     *          User-specific offset.
     * @return the index to the data Block if such block exists; or -1
     *         otherwise.
     */
    public int getBlockIndexNear(long offset) {
      ArrayList<BlockRegion> list = dataIndex.getBlockRegionList();
      int idx =
          Utils
              .lowerBound(list, new ScalarLong(offset), new ScalarComparator());

      if (idx == list.size()) {
        return -1;
      }

      return idx;
    }
  }

  /**
   * Index for all Meta blocks.
   */
  static class MetaIndex {
    // use a tree map, for getting a meta block entry by name
    final Map<String, MetaIndexEntry> index;

    // for write
    public MetaIndex() {
      index = new TreeMap<String, MetaIndexEntry>();
    }

    // for read, construct the map from the file
    public MetaIndex(DataInput in) throws IOException {
      int count = Utils.readVInt(in);
      index = new TreeMap<String, MetaIndexEntry>();

      for (int nx = 0; nx < count; nx++) {
        MetaIndexEntry indexEntry = new MetaIndexEntry(in);
        index.put(indexEntry.getMetaName(), indexEntry);
      }
    }

    public void addEntry(MetaIndexEntry indexEntry) {
      index.put(indexEntry.getMetaName(), indexEntry);
    }

    public MetaIndexEntry getMetaByName(String name) {
      return index.get(name);
    }

    public void write(DataOutput out) throws IOException {
      Utils.writeVInt(out, index.size());

      for (MetaIndexEntry indexEntry : index.values()) {
        indexEntry.write(out);
      }
    }
  }

  /**
   * An entry describes a meta block in the MetaIndex.
   */
  static final class MetaIndexEntry {
    private final String metaName;
    private final Algorithm compressionAlgorithm;
    private final static String defaultPrefix = "data:";

    private final BlockRegion region;

    public MetaIndexEntry(DataInput in) throws IOException {
      String fullMetaName = Utils.readString(in);
      if (fullMetaName.startsWith(defaultPrefix)) {
        metaName =
            fullMetaName.substring(defaultPrefix.length(), fullMetaName
                .length());
      } else {
        throw new IOException("Corrupted Meta region Index");
      }

      compressionAlgorithm =
          Compression.getCompressionAlgorithmByName(Utils.readString(in));
      region = new BlockRegion(in);
    }

    public MetaIndexEntry(String metaName, Algorithm compressionAlgorithm,
        BlockRegion region) {
      this.metaName = metaName;
      this.compressionAlgorithm = compressionAlgorithm;
      this.region = region;
    }

    public String getMetaName() {
      return metaName;
    }

    public Algorithm getCompressionAlgorithm() {
      return compressionAlgorithm;
    }

    public BlockRegion getRegion() {
      return region;
    }

    public void write(DataOutput out) throws IOException {
      Utils.writeString(out, defaultPrefix + metaName);
      Utils.writeString(out, compressionAlgorithm.getName());

      region.write(out);
    }
  }

  /**
   * Index of all compressed data blocks.
   */
  static class DataIndex {
    final static String BLOCK_NAME = "BCFile.index";

    private final Algorithm defaultCompressionAlgorithm;

    // for data blocks, each entry specifies a block's offset, compressed size
    // and raw size
    private final ArrayList<BlockRegion> listRegions;

    // for read, deserialized from a file
    public DataIndex(DataInput in) throws IOException {
      defaultCompressionAlgorithm =
          Compression.getCompressionAlgorithmByName(Utils.readString(in));

      int n = Utils.readVInt(in);
      listRegions = new ArrayList<BlockRegion>(n);

      for (int i = 0; i < n; i++) {
        BlockRegion region = new BlockRegion(in);
        listRegions.add(region);
      }
    }

    // for write
    public DataIndex(String defaultCompressionAlgorithmName) {
      this.defaultCompressionAlgorithm =
          Compression
              .getCompressionAlgorithmByName(defaultCompressionAlgorithmName);
      listRegions = new ArrayList<BlockRegion>();
    }

    public Algorithm getDefaultCompressionAlgorithm() {
      return defaultCompressionAlgorithm;
    }

    public ArrayList<BlockRegion> getBlockRegionList() {
      return listRegions;
    }

    public void addBlockRegion(BlockRegion region) {
      listRegions.add(region);
    }

    public void write(DataOutput out) throws IOException {
      Utils.writeString(out, defaultCompressionAlgorithm.getName());

      Utils.writeVInt(out, listRegions.size());

      for (BlockRegion region : listRegions) {
        region.write(out);
      }
    }
  }

  /**
   * Magic number uniquely identifying a BCFile in the header/footer.
   */
  static final class Magic {
    private final static byte[] AB_MAGIC_BCFILE =
        {
            // ... total of 16 bytes
            (byte) 0xd1, (byte) 0x11, (byte) 0xd3, (byte) 0x68, (byte) 0x91,
            (byte) 0xb5, (byte) 0xd7, (byte) 0xb6, (byte) 0x39, (byte) 0xdf,
            (byte) 0x41, (byte) 0x40, (byte) 0x92, (byte) 0xba, (byte) 0xe1,
            (byte) 0x50 };

    public static void readAndVerify(DataInput in) throws IOException {
      byte[] abMagic = new byte[size()];
      in.readFully(abMagic);

      // check against AB_MAGIC_BCFILE, if not matching, throw an
      // Exception
      if (!Arrays.equals(abMagic, AB_MAGIC_BCFILE)) {
        throw new IOException("Not a valid BCFile.");
      }
    }

    public static void write(DataOutput out) throws IOException {
      out.write(AB_MAGIC_BCFILE);
    }

    public static int size() {
      return AB_MAGIC_BCFILE.length;
    }
  }

  /**
   * Block region.
   */
  static final class BlockRegion implements Scalar {
    private final long offset;
    private final long compressedSize;
    private final long rawSize;

    public BlockRegion(DataInput in) throws IOException {
      offset = Utils.readVLong(in);
      compressedSize = Utils.readVLong(in);
      rawSize = Utils.readVLong(in);
    }

    public BlockRegion(long offset, long compressedSize, long rawSize) {
      this.offset = offset;
      this.compressedSize = compressedSize;
      this.rawSize = rawSize;
    }

    public void write(DataOutput out) throws IOException {
      Utils.writeVLong(out, offset);
      Utils.writeVLong(out, compressedSize);
      Utils.writeVLong(out, rawSize);
    }

    public long getOffset() {
      return offset;
    }

    public long getCompressedSize() {
      return compressedSize;
    }

    public long getRawSize() {
      return rawSize;
    }

    @Override
    public long magnitude() {
      return offset;
    }
  }
}
