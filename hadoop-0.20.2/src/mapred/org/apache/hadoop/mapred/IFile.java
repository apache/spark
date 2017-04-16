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
package org.apache.hadoop.mapred;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <code>IFile</code> is the simple <key-len, value-len, key, value> format
 * for the intermediate map-outputs in Map-Reduce.
 * 
 * There is a <code>Writer</code> to write out map-outputs in this format and 
 * a <code>Reader</code> to read files of this format.
 */
class IFile {

  private static final Log LOG = LogFactory.getLog(IFile.class);
  private static final int EOF_MARKER = -1;
  
  /**
   * <code>IFile.Writer</code> to write out intermediate map-outputs. 
   */
  public static class Writer<K extends Object, V extends Object> {
    FSDataOutputStream out;
    boolean ownOutputStream = false;
    long start = 0;
    FSDataOutputStream rawOut;
    
    CompressionOutputStream compressedOut;
    Compressor compressor;
    boolean compressOutput = false;
    
    long decompressedBytesWritten = 0;
    long compressedBytesWritten = 0;

    // Count records written to disk
    private long numRecordsWritten = 0;
    private final Counters.Counter writtenRecordsCounter;

    IFileOutputStream checksumOut;

    Class<K> keyClass;
    Class<V> valueClass;
    Serializer<K> keySerializer;
    Serializer<V> valueSerializer;
    
    DataOutputBuffer buffer = new DataOutputBuffer();

    public Writer(Configuration conf, FileSystem fs, Path file, 
                  Class<K> keyClass, Class<V> valueClass,
                  CompressionCodec codec,
                  Counters.Counter writesCounter) throws IOException {
      this(conf, fs.create(file), keyClass, valueClass, codec,
           writesCounter);
      ownOutputStream = true;
    }
    
    public Writer(Configuration conf, FSDataOutputStream out, 
        Class<K> keyClass, Class<V> valueClass,
        CompressionCodec codec, Counters.Counter writesCounter)
        throws IOException {
      this.writtenRecordsCounter = writesCounter;
      this.checksumOut = new IFileOutputStream(out);
      this.rawOut = out;
      this.start = this.rawOut.getPos();
      if (codec != null) {
        this.compressor = CodecPool.getCompressor(codec);
        if (this.compressor != null) {
          this.compressor.reset();
          this.compressedOut = codec.createOutputStream(checksumOut, compressor);
          this.out = new FSDataOutputStream(this.compressedOut,  null);
          this.compressOutput = true;
        } else {
          LOG.warn("Could not obtain compressor from CodecPool");
          this.out = new FSDataOutputStream(checksumOut,null);
        }
      } else {
        this.out = new FSDataOutputStream(checksumOut,null);
      }
      
      this.keyClass = keyClass;
      this.valueClass = valueClass;
      SerializationFactory serializationFactory = new SerializationFactory(conf);
      this.keySerializer = serializationFactory.getSerializer(keyClass);
      this.keySerializer.open(buffer);
      this.valueSerializer = serializationFactory.getSerializer(valueClass);
      this.valueSerializer.open(buffer);
    }

    public void close() throws IOException {

      // Close the serializers
      keySerializer.close();
      valueSerializer.close();

      // Write EOF_MARKER for key/value length
      WritableUtils.writeVInt(out, EOF_MARKER);
      WritableUtils.writeVInt(out, EOF_MARKER);
      decompressedBytesWritten += 2 * WritableUtils.getVIntSize(EOF_MARKER);
      
      //Flush the stream
      out.flush();
  
      if (compressOutput) {
        // Flush
        compressedOut.finish();
        compressedOut.resetState();
      }
      
      // Close the underlying stream iff we own it...
      if (ownOutputStream) {
        out.close();
      }
      else {
        // Write the checksum
        checksumOut.finish();
      }

      compressedBytesWritten = rawOut.getPos() - start;

      if (compressOutput) {
        // Return back the compressor
        CodecPool.returnCompressor(compressor);
        compressor = null;
      }

      out = null;
      if(writtenRecordsCounter != null) {
        writtenRecordsCounter.increment(numRecordsWritten);
      }
    }

    public void append(K key, V value) throws IOException {
      if (key.getClass() != keyClass)
        throw new IOException("wrong key class: "+ key.getClass()
                              +" is not "+ keyClass);
      if (value.getClass() != valueClass)
        throw new IOException("wrong value class: "+ value.getClass()
                              +" is not "+ valueClass);

      // Append the 'key'
      keySerializer.serialize(key);
      int keyLength = buffer.getLength();
      if (keyLength < 0) {
        throw new IOException("Negative key-length not allowed: " + keyLength + 
                              " for " + key);
      }

      // Append the 'value'
      valueSerializer.serialize(value);
      int valueLength = buffer.getLength() - keyLength;
      if (valueLength < 0) {
        throw new IOException("Negative value-length not allowed: " + 
                              valueLength + " for " + value);
      }
      
      // Write the record out
      WritableUtils.writeVInt(out, keyLength);                  // key length
      WritableUtils.writeVInt(out, valueLength);                // value length
      out.write(buffer.getData(), 0, buffer.getLength());       // data

      // Reset
      buffer.reset();
      
      // Update bytes written
      decompressedBytesWritten += keyLength + valueLength + 
                                  WritableUtils.getVIntSize(keyLength) + 
                                  WritableUtils.getVIntSize(valueLength);
      ++numRecordsWritten;
    }
    
    public void append(DataInputBuffer key, DataInputBuffer value)
    throws IOException {
      int keyLength = key.getLength() - key.getPosition();
      if (keyLength < 0) {
        throw new IOException("Negative key-length not allowed: " + keyLength + 
                              " for " + key);
      }
      
      int valueLength = value.getLength() - value.getPosition();
      if (valueLength < 0) {
        throw new IOException("Negative value-length not allowed: " + 
                              valueLength + " for " + value);
      }

      WritableUtils.writeVInt(out, keyLength);
      WritableUtils.writeVInt(out, valueLength);
      out.write(key.getData(), key.getPosition(), keyLength); 
      out.write(value.getData(), value.getPosition(), valueLength); 

      // Update bytes written
      decompressedBytesWritten += keyLength + valueLength + 
                      WritableUtils.getVIntSize(keyLength) + 
                      WritableUtils.getVIntSize(valueLength);
      ++numRecordsWritten;
    }
    
    public long getRawLength() {
      return decompressedBytesWritten;
    }
    
    public long getCompressedLength() {
      return compressedBytesWritten;
    }
  }

  /**
   * <code>IFile.Reader</code> to read intermediate map-outputs. 
   */
  public static class Reader<K extends Object, V extends Object> {
    private static final int DEFAULT_BUFFER_SIZE = 128*1024;
    private static final int MAX_VINT_SIZE = 9;

    // Count records read from disk
    private long numRecordsRead = 0;
    private final Counters.Counter readRecordsCounter;

    final InputStream in;        // Possibly decompressed stream that we read
    Decompressor decompressor;
    long bytesRead = 0;
    final long fileLength;
    boolean eof = false;
    final IFileInputStream checksumIn;
    
    byte[] buffer = null;
    int bufferSize = DEFAULT_BUFFER_SIZE;
    DataInputBuffer dataIn = new DataInputBuffer();

    int recNo = 1;
    
    /**
     * Construct an IFile Reader.
     * 
     * @param conf Configuration File 
     * @param fs  FileSystem
     * @param file Path of the file to be opened. This file should have
     *             checksum bytes for the data at the end of the file.
     * @param codec codec
     * @param readsCounter Counter for records read from disk
     * @throws IOException
     */
    public Reader(Configuration conf, FileSystem fs, Path file,
                  CompressionCodec codec,
                  Counters.Counter readsCounter) throws IOException {
      this(conf, fs.open(file), 
           fs.getFileStatus(file).getLen(),
           codec, readsCounter);
    }

    /**
     * Construct an IFile Reader.
     * 
     * @param conf Configuration File 
     * @param in   The input stream
     * @param length Length of the data in the stream, including the checksum
     *               bytes.
     * @param codec codec
     * @param readsCounter Counter for records read from disk
     * @throws IOException
     */
    public Reader(Configuration conf, FSDataInputStream in, long length, 
                  CompressionCodec codec,
                  Counters.Counter readsCounter) throws IOException {
      readRecordsCounter = readsCounter;
      checksumIn = new IFileInputStream(in,length);
      if (codec != null) {
        decompressor = CodecPool.getDecompressor(codec);
        if (decompressor != null) {
          this.in = codec.createInputStream(checksumIn, decompressor);
        } else {
          LOG.warn("Could not obtain decompressor from CodecPool");
          this.in = checksumIn;
        }
      } else {
        this.in = checksumIn;
      }
      this.fileLength = length;
      
      if (conf != null) {
        bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
      }
    }
    
    public long getLength() { 
      return fileLength - checksumIn.getSize();
    }
    
    public long getPosition() throws IOException {    
      return checksumIn.getPosition(); 
    }
    
    /**
     * Read upto len bytes into buf starting at offset off.
     * 
     * @param buf buffer 
     * @param off offset
     * @param len length of buffer
     * @return the no. of bytes read
     * @throws IOException
     */
    private int readData(byte[] buf, int off, int len) throws IOException {
      int bytesRead = 0;
      while (bytesRead < len) {
        int n = in.read(buf, off+bytesRead, len-bytesRead);
        if (n < 0) {
          return bytesRead;
        }
        bytesRead += n;
      }
      return len;
    }
    
    void readNextBlock(int minSize) throws IOException {
      if (buffer == null) {
        buffer = new byte[bufferSize];
        dataIn.reset(buffer, 0, 0);
      }
      buffer = 
        rejigData(buffer, 
                  (bufferSize < minSize) ? new byte[minSize << 1] : buffer);
      bufferSize = buffer.length;
    }
    
    private byte[] rejigData(byte[] source, byte[] destination) 
    throws IOException{
      // Copy remaining data into the destination array
      int bytesRemaining = dataIn.getLength()-dataIn.getPosition();
      if (bytesRemaining > 0) {
        System.arraycopy(source, dataIn.getPosition(), 
            destination, 0, bytesRemaining);
      }
      
      // Read as much data as will fit from the underlying stream 
      int n = readData(destination, bytesRemaining, 
                       (destination.length - bytesRemaining));
      dataIn.reset(destination, 0, (bytesRemaining + n));
      
      return destination;
    }
    
    public boolean next(DataInputBuffer key, DataInputBuffer value) 
    throws IOException {
      // Sanity check
      if (eof) {
        throw new EOFException("Completed reading " + bytesRead);
      }
      
      // Check if we have enough data to read lengths
      if ((dataIn.getLength() - dataIn.getPosition()) < 2*MAX_VINT_SIZE) {
        readNextBlock(2*MAX_VINT_SIZE);
      }
      
      // Read key and value lengths
      int oldPos = dataIn.getPosition();
      int keyLength = WritableUtils.readVInt(dataIn);
      int valueLength = WritableUtils.readVInt(dataIn);
      int pos = dataIn.getPosition();
      bytesRead += pos - oldPos;
      
      // Check for EOF
      if (keyLength == EOF_MARKER && valueLength == EOF_MARKER) {
        eof = true;
        return false;
      }
      
      // Sanity check
      if (keyLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative key-length: " + 
                              keyLength);
      }
      if (valueLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative value-length: " + 
                              valueLength);
      }
      
      final int recordLength = keyLength + valueLength;
      
      // Check if we have the raw key/value in the buffer
      if ((dataIn.getLength()-pos) < recordLength) {
        readNextBlock(recordLength);
        
        // Sanity check
        if ((dataIn.getLength() - dataIn.getPosition()) < recordLength) {
          throw new EOFException("Rec# " + recNo + ": Could read the next " +
          		                   " record");
        }
      }

      // Setup the key and value
      pos = dataIn.getPosition();
      byte[] data = dataIn.getData();
      key.reset(data, pos, keyLength);
      value.reset(data, (pos + keyLength), valueLength);
      
      // Position for the next record
      long skipped = dataIn.skip(recordLength);
      if (skipped != recordLength) {
        throw new IOException("Rec# " + recNo + ": Failed to skip past record " +
        		                  "of length: " + recordLength);
      }
      
      // Record the bytes read
      bytesRead += recordLength;

      ++recNo;
      ++numRecordsRead;

      return true;
    }

    public void close() throws IOException {
      // Return the decompressor
      if (decompressor != null) {
        decompressor.reset();
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      }
      
      // Close the underlying stream
      in.close();
      
      // Release the buffer
      dataIn = null;
      buffer = null;
      if(readRecordsCounter != null) {
        readRecordsCounter.increment(numRecordsRead);
      }
    }
  }    
  
  /**
   * <code>IFile.InMemoryReader</code> to read map-outputs present in-memory.
   */
  public static class InMemoryReader<K, V> extends Reader<K, V> {
    RamManager ramManager;
    TaskAttemptID taskAttemptId;
    
    public InMemoryReader(RamManager ramManager, TaskAttemptID taskAttemptId,
                          byte[] data, int start, int length)
                          throws IOException {
      super(null, null, length - start, null, null);
      this.ramManager = ramManager;
      this.taskAttemptId = taskAttemptId;
      
      buffer = data;
      bufferSize = (int)fileLength;
      dataIn.reset(buffer, start, length);
    }
    
    @Override
    public long getPosition() throws IOException {
      // InMemoryReader does not initialize streams like Reader, so in.getPos()
      // would not work. Instead, return the number of uncompressed bytes read,
      // which will be correct since in-memory data is not compressed.
      return bytesRead;
    }
    
    @Override
    public long getLength() { 
      return fileLength;
    }
    
    private void dumpOnError() {
      File dumpFile = new File("../output/" + taskAttemptId + ".dump");
      System.err.println("Dumping corrupt map-output of " + taskAttemptId + 
                         " to " + dumpFile.getAbsolutePath());
      try {
        FileOutputStream fos = new FileOutputStream(dumpFile);
        fos.write(buffer, 0, bufferSize);
        fos.close();
      } catch (IOException ioe) {
        System.err.println("Failed to dump map-output of " + taskAttemptId);
      }
    }
    
    public boolean next(DataInputBuffer key, DataInputBuffer value) 
    throws IOException {
      try {
      // Sanity check
      if (eof) {
        throw new EOFException("Completed reading " + bytesRead);
      }
      
      // Read key and value lengths
      int oldPos = dataIn.getPosition();
      int keyLength = WritableUtils.readVInt(dataIn);
      int valueLength = WritableUtils.readVInt(dataIn);
      int pos = dataIn.getPosition();
      bytesRead += pos - oldPos;
      
      // Check for EOF
      if (keyLength == EOF_MARKER && valueLength == EOF_MARKER) {
        eof = true;
        return false;
      }
      
      // Sanity check
      if (keyLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative key-length: " + 
                              keyLength);
      }
      if (valueLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative value-length: " + 
                              valueLength);
      }

      final int recordLength = keyLength + valueLength;
      
      // Setup the key and value
      pos = dataIn.getPosition();
      byte[] data = dataIn.getData();
      key.reset(data, pos, keyLength);
      value.reset(data, (pos + keyLength), valueLength);
      
      // Position for the next record
      long skipped = dataIn.skip(recordLength);
      if (skipped != recordLength) {
        throw new IOException("Rec# " + recNo + ": Failed to skip past record of length: " + 
                              recordLength);
      }
      
      // Record the byte
      bytesRead += recordLength;

      ++recNo;
      
      return true;
      } catch (IOException ioe) {
        dumpOnError();
        throw ioe;
      }
    }
      
    public void close() {
      // Release
      dataIn = null;
      buffer = null;
      
      // Inform the RamManager
      ramManager.unreserve(bufferSize);
    }
  }
}
