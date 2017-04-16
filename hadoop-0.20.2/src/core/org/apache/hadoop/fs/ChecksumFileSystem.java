/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs;

import java.io.*;
import java.util.Arrays;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.StringUtils;

/****************************************************************
 * Abstract Checksumed FileSystem.
 * It provide a basice implementation of a Checksumed FileSystem,
 * which creates a checksum file for each raw file.
 * It generates & verifies checksums at the client side.
 *
 *****************************************************************/
public abstract class ChecksumFileSystem extends FilterFileSystem {
  private static final byte[] CHECKSUM_VERSION = new byte[] {'c', 'r', 'c', 0};
  private int bytesPerChecksum = 512;
  private boolean verifyChecksum = true;

  public static double getApproxChkSumLength(long size) {
    return ChecksumFSOutputSummer.CHKSUM_AS_FRACTION * size;
  }
  
  public ChecksumFileSystem(FileSystem fs) {
    super(fs);
  }

  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      bytesPerChecksum = conf.getInt("io.bytes.per.checksum", 512);
    }
  }
  
  /**
   * Set whether to verify checksum.
   */
  public void setVerifyChecksum(boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
  }

  /** get the raw file system */
  public FileSystem getRawFileSystem() {
    return fs;
  }

  /** Return the name of the checksum file associated with a file.*/
  public Path getChecksumFile(Path file) {
    return new Path(file.getParent(), "." + file.getName() + ".crc");
  }

  /** Return true iff file is a checksum file name.*/
  public static boolean isChecksumFile(Path file) {
    String name = file.getName();
    return name.startsWith(".") && name.endsWith(".crc");
  }

  /** Return the length of the checksum file given the size of the 
   * actual file.
   **/
  public long getChecksumFileLength(Path file, long fileSize) {
    return getChecksumLength(fileSize, getBytesPerSum());
  }

  /** Return the bytes Per Checksum */
  public int getBytesPerSum() {
    return bytesPerChecksum;
  }

  private int getSumBufferSize(int bytesPerSum, int bufferSize) {
    int defaultBufferSize = getConf().getInt("io.file.buffer.size", 4096);
    int proportionalBufferSize = bufferSize / bytesPerSum;
    return Math.max(bytesPerSum,
                    Math.max(proportionalBufferSize, defaultBufferSize));
  }

  /*******************************************************
   * For open()'s FSInputStream
   * It verifies that data matches checksums.
   *******************************************************/
  private static class ChecksumFSInputChecker extends FSInputChecker {
    public static final Log LOG 
      = LogFactory.getLog(FSInputChecker.class);
    
    private ChecksumFileSystem fs;
    private FSDataInputStream datas;
    private FSDataInputStream sums;
    
    private static final int HEADER_LENGTH = 8;
    
    private int bytesPerSum = 1;
    private long fileLen = -1L;
    
    public ChecksumFSInputChecker(ChecksumFileSystem fs, Path file)
      throws IOException {
      this(fs, file, fs.getConf().getInt("io.file.buffer.size", 4096));
    }
    
    public ChecksumFSInputChecker(ChecksumFileSystem fs, Path file, int bufferSize)
      throws IOException {
      super( file, fs.getFileStatus(file).getReplication() );
      this.datas = fs.getRawFileSystem().open(file, bufferSize);
      this.fs = fs;
      Path sumFile = fs.getChecksumFile(file);
      try {
        int sumBufferSize = fs.getSumBufferSize(fs.getBytesPerSum(), bufferSize);
        sums = fs.getRawFileSystem().open(sumFile, sumBufferSize);

        byte[] version = new byte[CHECKSUM_VERSION.length];
        sums.readFully(version);
        if (!Arrays.equals(version, CHECKSUM_VERSION))
          throw new IOException("Not a checksum file: "+sumFile);
        this.bytesPerSum = sums.readInt();
        set(fs.verifyChecksum, new PureJavaCrc32(), bytesPerSum, 4);
      } catch (FileNotFoundException e) {         // quietly ignore
        set(fs.verifyChecksum, null, 1, 0);
      } catch (IOException e) {                   // loudly ignore
        LOG.warn("Problem opening checksum file: "+ file + 
                 ".  Ignoring exception: " + 
                 StringUtils.stringifyException(e));
        set(fs.verifyChecksum, null, 1, 0);
      }
    }
    
    private long getChecksumFilePos( long dataPos ) {
      return HEADER_LENGTH + 4*(dataPos/bytesPerSum);
    }
    
    protected long getChunkPosition( long dataPos ) {
      return dataPos/bytesPerSum*bytesPerSum;
    }
    
    public int available() throws IOException {
      return datas.available() + super.available();
    }
    
    public int read(long position, byte[] b, int off, int len)
      throws IOException {
      // parameter check
      if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }
      if( position<0 ) {
        throw new IllegalArgumentException(
            "Parameter position can not to be negative");
      }

      ChecksumFSInputChecker checker = new ChecksumFSInputChecker(fs, file);
      checker.seek(position);
      int nread = checker.read(b, off, len);
      checker.close();
      return nread;
    }
    
    public void close() throws IOException {
      datas.close();
      if( sums != null ) {
        sums.close();
      }
      set(fs.verifyChecksum, null, 1, 0);
    }
    

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      long sumsPos = getChecksumFilePos(targetPos);
      fs.reportChecksumFailure(file, datas, targetPos, sums, sumsPos);
      boolean newDataSource = datas.seekToNewSource(targetPos);
      return sums.seekToNewSource(sumsPos) || newDataSource;
    }

    @Override
    protected int readChunk(long pos, byte[] buf, int offset, int len,
        byte[] checksum) throws IOException {
      boolean eof = false;
      if(needChecksum()) {
        try {
          long checksumPos = getChecksumFilePos(pos); 
          if(checksumPos != sums.getPos()) {
            sums.seek(checksumPos);
          }
          sums.readFully(checksum);
        } catch (EOFException e) {
          eof = true;
        }
        len = bytesPerSum;
      }
      if(pos != datas.getPos()) {
        datas.seek(pos);
      }
      int nread = readFully(datas, buf, offset, len);
      if( eof && nread > 0) {
        throw new ChecksumException("Checksum error: "+file+" at "+pos, pos);
      }
      return nread;
    }
    
    /* Return the file length */
    private long getFileLength() throws IOException {
      if( fileLen==-1L ) {
        fileLen = fs.getContentSummary(file).getLength();
      }
      return fileLen;
    }
    
    /**
     * Skips over and discards <code>n</code> bytes of data from the
     * input stream.
     *
     *The <code>skip</code> method skips over some smaller number of bytes
     * when reaching end of file before <code>n</code> bytes have been skipped.
     * The actual number of bytes skipped is returned.  If <code>n</code> is
     * negative, no bytes are skipped.
     *
     * @param      n   the number of bytes to be skipped.
     * @return     the actual number of bytes skipped.
     * @exception  IOException  if an I/O error occurs.
     *             ChecksumException if the chunk to skip to is corrupted
     */
    public synchronized long skip(long n) throws IOException {
      long curPos = getPos();
      long fileLength = getFileLength();
      if( n+curPos > fileLength ) {
        n = fileLength - curPos;
      }
      return super.skip(n);
    }
    
    /**
     * Seek to the given position in the stream.
     * The next read() will be from that position.
     * 
     * <p>This method does not allow seek past the end of the file.
     * This produces IOException.
     *
     * @param      pos   the postion to seek to.
     * @exception  IOException  if an I/O error occurs or seeks after EOF
     *             ChecksumException if the chunk to seek to is corrupted
     */

    public synchronized void seek(long pos) throws IOException {
      if(pos>getFileLength()) {
        throw new IOException("Cannot seek after EOF");
      }
      super.seek(pos);
    }

  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return new FSDataInputStream(
        new ChecksumFSInputChecker(this, f, bufferSize));
  }

  /** {@inheritDoc} */
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * Calculated the length of the checksum file in bytes.
   * @param size the length of the data file in bytes
   * @param bytesPerSum the number of bytes in a checksum block
   * @return the number of bytes in the checksum file
   */
  public static long getChecksumLength(long size, int bytesPerSum) {
    //the checksum length is equal to size passed divided by bytesPerSum +
    //bytes written in the beginning of the checksum file.  
    return ((size + bytesPerSum - 1) / bytesPerSum) * 4 +
             CHECKSUM_VERSION.length + 4;  
  }

  /** This class provides an output stream for a checksummed file.
   * It generates checksums for data. */
  private static class ChecksumFSOutputSummer extends FSOutputSummer {
    private FSDataOutputStream datas;    
    private FSDataOutputStream sums;
    private static final float CHKSUM_AS_FRACTION = 0.01f;
    
    public ChecksumFSOutputSummer(ChecksumFileSystem fs, 
                          Path file, 
                          boolean overwrite, 
                          short replication,
                          long blockSize,
                          Configuration conf)
      throws IOException {
      this(fs, file, overwrite, 
           conf.getInt("io.file.buffer.size", 4096),
           replication, blockSize, null);
    }
    
    public ChecksumFSOutputSummer(ChecksumFileSystem fs, 
                          Path file, 
                          boolean overwrite,
                          int bufferSize,
                          short replication,
                          long blockSize,
                          Progressable progress)
      throws IOException {
      super(new PureJavaCrc32(), fs.getBytesPerSum(), 4);
      int bytesPerSum = fs.getBytesPerSum();
      this.datas = fs.getRawFileSystem().create(file, overwrite, bufferSize, 
                                         replication, blockSize, progress);
      int sumBufferSize = fs.getSumBufferSize(bytesPerSum, bufferSize);
      this.sums = fs.getRawFileSystem().create(fs.getChecksumFile(file), true, 
                                               sumBufferSize, replication,
                                               blockSize);
      sums.write(CHECKSUM_VERSION, 0, CHECKSUM_VERSION.length);
      sums.writeInt(bytesPerSum);
    }
    
    public void close() throws IOException {
      flushBuffer();
      sums.close();
      datas.close();
    }
    
    @Override
    protected void writeChunk(byte[] b, int offset, int len, byte[] checksum)
    throws IOException {
      datas.write(b, offset, len);
      sums.write(checksum);
    }
  }

  /** {@inheritDoc} */
  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return create(f, permission, overwrite, true, bufferSize, 
        replication, blockSize, progress);
  }

  private FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, boolean createParent, int bufferSize, 
      short replication, long blockSize,
      Progressable progress) throws IOException {
    Path parent = f.getParent();
    if (parent != null) {
      if (!createParent && !exists(parent)) {
        throw new FileNotFoundException("Parent directory doesn't exist: "
            + parent);
      } else if (!mkdirs(parent)) {
        throw new IOException("Mkdirs failed to create " + parent);
      }
    }
    final FSDataOutputStream out = new FSDataOutputStream(
        new ChecksumFSOutputSummer(this, f, overwrite, bufferSize, replication,
            blockSize, progress), null);
    if (permission != null) {
      setPermission(f, permission);
    }
    return out;
  }

  /** {@inheritDoc} */
  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return create(f, permission, overwrite, false, bufferSize, replication, 
        blockSize, progress);
  }

  /**
   * Set replication for an existing file.
   * Implement the abstract <tt>setReplication</tt> of <tt>FileSystem</tt>
   * @param src file name
   * @param replication new replication
   * @throws IOException
   * @return true if successful;
   *         false if file does not exist or is a directory
   */
  public boolean setReplication(Path src, short replication) throws IOException {
    boolean value = fs.setReplication(src, replication);
    if (!value)
      return false;

    Path checkFile = getChecksumFile(src);
    if (exists(checkFile))
      fs.setReplication(checkFile, replication);

    return true;
  }

  /**
   * Rename files/dirs
   */
  public boolean rename(Path src, Path dst) throws IOException {
    if (fs.isDirectory(src)) {
      return fs.rename(src, dst);
    } else {

      boolean value = fs.rename(src, dst);
      if (!value)
        return false;

      Path checkFile = getChecksumFile(src);
      if (fs.exists(checkFile)) { //try to rename checksum
        if (fs.isDirectory(dst)) {
          value = fs.rename(checkFile, dst);
        } else {
          value = fs.rename(checkFile, getChecksumFile(dst));
        }
      }

      return value;
    }
  }

  /**
   * Implement the delete(Path, boolean) in checksum
   * file system.
   */
  public boolean delete(Path f, boolean recursive) throws IOException{
    FileStatus fstatus = null;
    try {
      fstatus = fs.getFileStatus(f);
    } catch(FileNotFoundException e) {
      return false;
    }
    if(fstatus.isDir()) {
      //this works since the crcs are in the same
      //directories and the files. so we just delete
      //everything in the underlying filesystem
      return fs.delete(f, recursive);
    } else {
      Path checkFile = getChecksumFile(f);
      if (fs.exists(checkFile)) {
        fs.delete(checkFile, true);
      }
      return fs.delete(f, true);
    }
  }
    
  final private static PathFilter DEFAULT_FILTER = new PathFilter() {
    public boolean accept(Path file) {
      return !isChecksumFile(file);
    }
  };

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * 
   * @param f
   *          given path
   * @return the statuses of the files/directories in the given patch
   * @throws IOException
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    return fs.listStatus(f, DEFAULT_FILTER);
  }
  
  @Override
  public boolean mkdirs(Path f) throws IOException {
    return fs.mkdirs(f);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    Configuration conf = getConf();
    FileUtil.copy(getLocal(conf), src, this, dst, delSrc, conf);
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   */
  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    Configuration conf = getConf();
    FileUtil.copy(this, src, getLocal(conf), dst, delSrc, conf);
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * If src and dst are directories, the copyCrc parameter
   * determines whether to copy CRC files.
   */
  public void copyToLocalFile(Path src, Path dst, boolean copyCrc)
    throws IOException {
    if (!fs.isDirectory(src)) { // source is a file
      fs.copyToLocalFile(src, dst);
      FileSystem localFs = getLocal(getConf()).getRawFileSystem();
      if (localFs.isDirectory(dst)) {
        dst = new Path(dst, src.getName());
      }
      dst = getChecksumFile(dst);
      if (localFs.exists(dst)) { //remove old local checksum file
        localFs.delete(dst, true);
      }
      Path checksumFile = getChecksumFile(src);
      if (copyCrc && fs.exists(checksumFile)) { //copy checksum file
        fs.copyToLocalFile(checksumFile, dst);
      }
    } else {
      FileStatus[] srcs = listStatus(src);
      for (FileStatus srcFile : srcs) {
        copyToLocalFile(srcFile.getPath(), 
                        new Path(dst, srcFile.getPath().getName()), copyCrc);
      }
    }
  }

  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    return tmpLocalFile;
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    moveFromLocalFile(tmpLocalFile, fsOutputFile);
  }

  /**
   * Report a checksum error to the file system.
   * @param f the file name containing the error
   * @param in the stream open on the file
   * @param inPos the position of the beginning of the bad data in the file
   * @param sums the stream open on the checksum file
   * @param sumsPos the position of the beginning of the bad data in the checksum file
   * @return if retry is neccessary
   */
  public boolean reportChecksumFailure(Path f, FSDataInputStream in,
                                       long inPos, FSDataInputStream sums, long sumsPos) {
    return false;
  }
}
