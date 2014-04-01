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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.WritableUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.znerd.xmlenc.XMLOutputter;

/** MD5 of MD5 of CRC32. */
public class MD5MD5CRC32FileChecksum extends FileChecksum {
  public static final int LENGTH = MD5Hash.MD5_LEN
      + (Integer.SIZE + Long.SIZE)/Byte.SIZE;

  private int bytesPerCRC;
  private long crcPerBlock;
  private MD5Hash md5;

  /** Same as this(0, 0, null) */
  public MD5MD5CRC32FileChecksum() {
    this(0, 0, null);
  }

  /** Create a MD5FileChecksum */
  public MD5MD5CRC32FileChecksum(int bytesPerCRC, long crcPerBlock, MD5Hash md5) {
    this.bytesPerCRC = bytesPerCRC;
    this.crcPerBlock = crcPerBlock;
    this.md5 = md5;
  }
  
  /** {@inheritDoc} */ 
  public String getAlgorithmName() {
    return "MD5-of-" + crcPerBlock + "MD5-of-" + bytesPerCRC + "CRC32";
  }

  /** {@inheritDoc} */ 
  public int getLength() {return LENGTH;}

  /** {@inheritDoc} */ 
  public byte[] getBytes() {
    return WritableUtils.toByteArray(this);
  }

  /** {@inheritDoc} */ 
  public void readFields(DataInput in) throws IOException {
    bytesPerCRC = in.readInt();
    crcPerBlock = in.readLong();
    md5 = MD5Hash.read(in);
  }

  /** {@inheritDoc} */ 
  public void write(DataOutput out) throws IOException {
    out.writeInt(bytesPerCRC);
    out.writeLong(crcPerBlock);
    md5.write(out);    
  }

  /** Write that object to xml output. */
  public static void write(XMLOutputter xml, MD5MD5CRC32FileChecksum that
      ) throws IOException {
    xml.startTag(MD5MD5CRC32FileChecksum.class.getName());
    if (that != null) {
      xml.attribute("bytesPerCRC", "" + that.bytesPerCRC);
      xml.attribute("crcPerBlock", "" + that.crcPerBlock);
      xml.attribute("md5", "" + that.md5);
    }
    xml.endTag();
  }

  /** Return the object represented in the attributes. */
  public static MD5MD5CRC32FileChecksum valueOf(Attributes attrs
      ) throws SAXException {
    final String bytesPerCRC = attrs.getValue("bytesPerCRC");
    final String crcPerBlock = attrs.getValue("crcPerBlock");
    final String md5 = attrs.getValue("md5");
    if (bytesPerCRC == null || crcPerBlock == null || md5 == null) {
      return null;
    }

    try {
      return new MD5MD5CRC32FileChecksum(Integer.valueOf(bytesPerCRC),
          Integer.valueOf(crcPerBlock), new MD5Hash(md5));
    } catch(Exception e) {
      throw new SAXException("Invalid attributes: bytesPerCRC=" + bytesPerCRC
          + ", crcPerBlock=" + crcPerBlock + ", md5=" + md5, e);
    }
  }

  /** {@inheritDoc} */ 
  public String toString() {
    return getAlgorithmName() + ":" + md5;
  }
}