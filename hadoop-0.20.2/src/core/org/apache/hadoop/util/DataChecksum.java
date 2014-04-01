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

package org.apache.hadoop.util;

import java.util.zip.Checksum;
import java.util.zip.CRC32;

import java.io.*;

/**
 * This class provides inteface and utilities for processing checksums for
 * DFS data transfers.
 */

public class DataChecksum implements Checksum {
  
  // Misc constants
  public static final int HEADER_LEN = 5; /// 1 byte type and 4 byte len
  
  // checksum types
  public static final int CHECKSUM_NULL    = 0;
  public static final int CHECKSUM_CRC32   = 1;
  
  private static final int CHECKSUM_NULL_SIZE  = 0;
  private static final int CHECKSUM_CRC32_SIZE = 4;
  
  
  public static DataChecksum newDataChecksum( int type, int bytesPerChecksum ) {
    if ( bytesPerChecksum <= 0 ) {
      return null;
    }
    
    switch ( type ) {
    case CHECKSUM_NULL :
      return new DataChecksum( CHECKSUM_NULL, new ChecksumNull(), 
                               CHECKSUM_NULL_SIZE, bytesPerChecksum );
    case CHECKSUM_CRC32 :
      return new DataChecksum( CHECKSUM_CRC32, new PureJavaCrc32(), 
                               CHECKSUM_CRC32_SIZE, bytesPerChecksum );
    default:
      return null;  
    }
  }
  
  /**
   * Creates a DataChecksum from HEADER_LEN bytes from arr[offset].
   * @return DataChecksum of the type in the array or null in case of an error.
   */
  public static DataChecksum newDataChecksum( byte bytes[], int offset ) {
    if ( offset < 0 || bytes.length < offset + HEADER_LEN ) {
      return null;
    }
    
    // like readInt():
    int bytesPerChecksum = ( (bytes[offset+1] & 0xff) << 24 ) | 
                           ( (bytes[offset+2] & 0xff) << 16 ) |
                           ( (bytes[offset+3] & 0xff) << 8 )  |
                           ( (bytes[offset+4] & 0xff) );
    return newDataChecksum( bytes[0], bytesPerChecksum );
  }
  
  /**
   * This constructucts a DataChecksum by reading HEADER_LEN bytes from
   * input stream <i>in</i>
   */
  public static DataChecksum newDataChecksum( DataInputStream in )
                                 throws IOException {
    int type = in.readByte();
    int bpc = in.readInt();
    DataChecksum summer = newDataChecksum( type, bpc );
    if ( summer == null ) {
      throw new IOException( "Could not create DataChecksum of type " +
                             type + " with bytesPerChecksum " + bpc );
    }
    return summer;
  }
  
  /**
   * Writes the checksum header to the output stream <i>out</i>.
   */
  public void writeHeader( DataOutputStream out ) 
                           throws IOException { 
    out.writeByte( type );
    out.writeInt( bytesPerChecksum );
  }

  public byte[] getHeader() {
    byte[] header = new byte[DataChecksum.HEADER_LEN];
    header[0] = (byte) (type & 0xff);
    // Writing in buffer just like DataOutput.WriteInt()
    header[1+0] = (byte) ((bytesPerChecksum >>> 24) & 0xff);
    header[1+1] = (byte) ((bytesPerChecksum >>> 16) & 0xff);
    header[1+2] = (byte) ((bytesPerChecksum >>> 8) & 0xff);
    header[1+3] = (byte) (bytesPerChecksum & 0xff);
    return header;
  }
  
  /**
   * Writes the current checksum to the stream.
   * If <i>reset</i> is true, then resets the checksum.
   * @return number of bytes written. Will be equal to getChecksumSize();
   */
   public int writeValue( DataOutputStream out, boolean reset )
                          throws IOException {
     if ( size <= 0 ) {
       return 0;
     }

     if ( type == CHECKSUM_CRC32 ) {
       out.writeInt( (int) summer.getValue() );
     } else {
       throw new IOException( "Unknown Checksum " + type );
     }
     
     if ( reset ) {
       reset();
     }
     
     return size;
   }
   
   /**
    * Writes the current checksum to a buffer.
    * If <i>reset</i> is true, then resets the checksum.
    * @return number of bytes written. Will be equal to getChecksumSize();
    */
    public int writeValue( byte[] buf, int offset, boolean reset )
                           throws IOException {
      if ( size <= 0 ) {
        return 0;
      }

      if ( type == CHECKSUM_CRC32 ) {
        int checksum = (int) summer.getValue();
        buf[offset+0] = (byte) ((checksum >>> 24) & 0xff);
        buf[offset+1] = (byte) ((checksum >>> 16) & 0xff);
        buf[offset+2] = (byte) ((checksum >>> 8) & 0xff);
        buf[offset+3] = (byte) (checksum & 0xff);
      } else {
        throw new IOException( "Unknown Checksum " + type );
      }
      
      if ( reset ) {
        reset();
      }
      
      return size;
    }
   
   /**
    * Compares the checksum located at buf[offset] with the current checksum.
    * @return true if the checksum matches and false otherwise.
    */
   public boolean compare( byte buf[], int offset ) {
     if ( size > 0 && type == CHECKSUM_CRC32 ) {
       int checksum = ( (buf[offset+0] & 0xff) << 24 ) | 
                      ( (buf[offset+1] & 0xff) << 16 ) |
                      ( (buf[offset+2] & 0xff) << 8 )  |
                      ( (buf[offset+3] & 0xff) );
       return checksum == (int) summer.getValue();
     }
     return size == 0;
   }
   
  private final int type;
  private final int size;
  private final Checksum summer;
  private final int bytesPerChecksum;
  private int inSum = 0;
  
  private DataChecksum( int checksumType, Checksum checksum,
                        int sumSize, int chunkSize ) {
    type = checksumType;
    summer = checksum;
    size = sumSize;
    bytesPerChecksum = chunkSize;
  }
  
  // Accessors
  public int getChecksumType() {
    return type;
  }
  public int getChecksumSize() {
    return size;
  }
  public int getBytesPerChecksum() {
    return bytesPerChecksum;
  }
  public int getNumBytesInSum() {
    return inSum;
  }
  
  public static final int SIZE_OF_INTEGER = Integer.SIZE / Byte.SIZE;
  static public int getChecksumHeaderSize() {
    return 1 + SIZE_OF_INTEGER; // type byte, bytesPerChecksum int
  }
  //Checksum Interface. Just a wrapper around member summer.
  public long getValue() {
    return summer.getValue();
  }
  public void reset() {
    summer.reset();
    inSum = 0;
  }
  public void update( byte[] b, int off, int len ) {
    if ( len > 0 ) {
      summer.update( b, off, len );
      inSum += len;
    }
  }
  public void update( int b ) {
    summer.update( b );
    inSum += 1;
  }
  
  /**
   * This just provides a dummy implimentation for Checksum class
   * This is used when there is no checksum available or required for 
   * data
   */
  static class ChecksumNull implements Checksum {
    
    public ChecksumNull() {}
    
    //Dummy interface
    public long getValue() { return 0; }
    public void reset() {}
    public void update(byte[] b, int off, int len) {}
    public void update(int b) {}
  };
}
