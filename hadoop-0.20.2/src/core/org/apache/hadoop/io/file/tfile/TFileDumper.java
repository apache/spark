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

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.file.tfile.BCFile.BlockRegion;
import org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry;
import org.apache.hadoop.io.file.tfile.TFile.TFileIndexEntry;
import org.apache.hadoop.io.file.tfile.Utils.Version;

/**
 * Dumping the information of a TFile.
 */
class TFileDumper {
  static final Log LOG = LogFactory.getLog(TFileDumper.class);

  private TFileDumper() {
    // namespace object not constructable.
  }

  private enum Align {
    LEFT, CENTER, RIGHT, ZERO_PADDED;
    static String format(String s, int width, Align align) {
      if (s.length() >= width) return s;
      int room = width - s.length();
      Align alignAdjusted = align;
      if (room == 1) {
        alignAdjusted = LEFT;
      }
      if (alignAdjusted == LEFT) {
        return s + String.format("%" + room + "s", "");
      }
      if (alignAdjusted == RIGHT) {
        return String.format("%" + room + "s", "") + s;
      }
      if (alignAdjusted == CENTER) {
        int half = room / 2;
        return String.format("%" + half + "s", "") + s
            + String.format("%" + (room - half) + "s", "");
      }
      throw new IllegalArgumentException("Unsupported alignment");
    }

    static String format(long l, int width, Align align) {
      if (align == ZERO_PADDED) {
        return String.format("%0" + width + "d", l);
      }
      return format(Long.toString(l), width, align);
    }

    static int calculateWidth(String caption, long max) {
      return Math.max(caption.length(), Long.toString(max).length());
    }
  }

  /**
   * Dump information about TFile.
   * 
   * @param file
   *          Path string of the TFile
   * @param out
   *          PrintStream to output the information.
   * @param conf
   *          The configuration object.
   * @throws IOException
   */
  static public void dumpInfo(String file, PrintStream out, Configuration conf)
      throws IOException {
    final int maxKeySampleLen = 16;
    Path path = new Path(file);
    FileSystem fs = path.getFileSystem(conf);
    long length = fs.getFileStatus(path).getLen();
    FSDataInputStream fsdis = fs.open(path);
    TFile.Reader reader = new TFile.Reader(fsdis, length, conf);
    try {
      LinkedHashMap<String, String> properties =
          new LinkedHashMap<String, String>();
      int blockCnt = reader.readerBCF.getBlockCount();
      int metaBlkCnt = reader.readerBCF.metaIndex.index.size();
      properties.put("BCFile Version", reader.readerBCF.version.toString());
      properties.put("TFile Version", reader.tfileMeta.version.toString());
      properties.put("File Length", Long.toString(length));
      properties.put("Data Compression", reader.readerBCF
          .getDefaultCompressionName());
      properties.put("Record Count", Long.toString(reader.getEntryCount()));
      properties.put("Sorted", Boolean.toString(reader.isSorted()));
      if (reader.isSorted()) {
        properties.put("Comparator", reader.getComparatorName());
      }
      properties.put("Data Block Count", Integer.toString(blockCnt));
      long dataSize = 0, dataSizeUncompressed = 0;
      if (blockCnt > 0) {
        for (int i = 0; i < blockCnt; ++i) {
          BlockRegion region =
              reader.readerBCF.dataIndex.getBlockRegionList().get(i);
          dataSize += region.getCompressedSize();
          dataSizeUncompressed += region.getRawSize();
        }
        properties.put("Data Block Bytes", Long.toString(dataSize));
        if (reader.readerBCF.getDefaultCompressionName() != "none") {
          properties.put("Data Block Uncompressed Bytes", Long
              .toString(dataSizeUncompressed));
          properties.put("Data Block Compression Ratio", String.format(
              "1:%.1f", (double) dataSizeUncompressed / dataSize));
        }
      }

      properties.put("Meta Block Count", Integer.toString(metaBlkCnt));
      long metaSize = 0, metaSizeUncompressed = 0;
      if (metaBlkCnt > 0) {
        Collection<MetaIndexEntry> metaBlks =
            reader.readerBCF.metaIndex.index.values();
        boolean calculateCompression = false;
        for (Iterator<MetaIndexEntry> it = metaBlks.iterator(); it.hasNext();) {
          MetaIndexEntry e = it.next();
          metaSize += e.getRegion().getCompressedSize();
          metaSizeUncompressed += e.getRegion().getRawSize();
          if (e.getCompressionAlgorithm() != Compression.Algorithm.NONE) {
            calculateCompression = true;
          }
        }
        properties.put("Meta Block Bytes", Long.toString(metaSize));
        if (calculateCompression) {
          properties.put("Meta Block Uncompressed Bytes", Long
              .toString(metaSizeUncompressed));
          properties.put("Meta Block Compression Ratio", String.format(
              "1:%.1f", (double) metaSizeUncompressed / metaSize));
        }
      }
      properties.put("Meta-Data Size Ratio", String.format("1:%.1f",
          (double) dataSize / metaSize));
      long leftOverBytes = length - dataSize - metaSize;
      long miscSize =
          BCFile.Magic.size() * 2 + Long.SIZE / Byte.SIZE + Version.size();
      long metaIndexSize = leftOverBytes - miscSize;
      properties.put("Meta Block Index Bytes", Long.toString(metaIndexSize));
      properties.put("Headers Etc Bytes", Long.toString(miscSize));
      // Now output the properties table.
      int maxKeyLength = 0;
      Set<Map.Entry<String, String>> entrySet = properties.entrySet();
      for (Iterator<Map.Entry<String, String>> it = entrySet.iterator(); it
          .hasNext();) {
        Map.Entry<String, String> e = it.next();
        if (e.getKey().length() > maxKeyLength) {
          maxKeyLength = e.getKey().length();
        }
      }
      for (Iterator<Map.Entry<String, String>> it = entrySet.iterator(); it
          .hasNext();) {
        Map.Entry<String, String> e = it.next();
        out.printf("%s : %s\n", Align.format(e.getKey(), maxKeyLength,
            Align.LEFT), e.getValue());
      }
      out.println();
      reader.checkTFileDataIndex();
      if (blockCnt > 0) {
        String blkID = "Data-Block";
        int blkIDWidth = Align.calculateWidth(blkID, blockCnt);
        int blkIDWidth2 = Align.calculateWidth("", blockCnt);
        String offset = "Offset";
        int offsetWidth = Align.calculateWidth(offset, length);
        String blkLen = "Length";
        int blkLenWidth =
            Align.calculateWidth(blkLen, dataSize / blockCnt * 10);
        String rawSize = "Raw-Size";
        int rawSizeWidth =
            Align.calculateWidth(rawSize, dataSizeUncompressed / blockCnt * 10);
        String records = "Records";
        int recordsWidth =
            Align.calculateWidth(records, reader.getEntryCount() / blockCnt
                * 10);
        String endKey = "End-Key";
        int endKeyWidth = Math.max(endKey.length(), maxKeySampleLen * 2 + 5);

        out.printf("%s %s %s %s %s %s\n", Align.format(blkID, blkIDWidth,
            Align.CENTER), Align.format(offset, offsetWidth, Align.CENTER),
            Align.format(blkLen, blkLenWidth, Align.CENTER), Align.format(
                rawSize, rawSizeWidth, Align.CENTER), Align.format(records,
                recordsWidth, Align.CENTER), Align.format(endKey, endKeyWidth,
                Align.LEFT));

        for (int i = 0; i < blockCnt; ++i) {
          BlockRegion region =
              reader.readerBCF.dataIndex.getBlockRegionList().get(i);
          TFileIndexEntry indexEntry = reader.tfileIndex.getEntry(i);
          out.printf("%s %s %s %s %s ", Align.format(Align.format(i,
              blkIDWidth2, Align.ZERO_PADDED), blkIDWidth, Align.LEFT), Align
              .format(region.getOffset(), offsetWidth, Align.LEFT), Align
              .format(region.getCompressedSize(), blkLenWidth, Align.LEFT),
              Align.format(region.getRawSize(), rawSizeWidth, Align.LEFT),
              Align.format(indexEntry.kvEntries, recordsWidth, Align.LEFT));
          byte[] key = indexEntry.key;
          boolean asAscii = true;
          int sampleLen = Math.min(maxKeySampleLen, key.length);
          for (int j = 0; j < sampleLen; ++j) {
            byte b = key[j];
            if ((b < 32 && b != 9) || (b == 127)) {
              asAscii = false;
            }
          }
          if (!asAscii) {
            out.print("0X");
            for (int j = 0; j < sampleLen; ++j) {
              byte b = key[i];
              out.printf("%X", b);
            }
          } else {
            out.print(new String(key, 0, sampleLen));
          }
          if (sampleLen < key.length) {
            out.print("...");
          }
          out.println();
        }
      }

      out.println();
      if (metaBlkCnt > 0) {
        String name = "Meta-Block";
        int maxNameLen = 0;
        Set<Map.Entry<String, MetaIndexEntry>> metaBlkEntrySet =
            reader.readerBCF.metaIndex.index.entrySet();
        for (Iterator<Map.Entry<String, MetaIndexEntry>> it =
            metaBlkEntrySet.iterator(); it.hasNext();) {
          Map.Entry<String, MetaIndexEntry> e = it.next();
          if (e.getKey().length() > maxNameLen) {
            maxNameLen = e.getKey().length();
          }
        }
        int nameWidth = Math.max(name.length(), maxNameLen);
        String offset = "Offset";
        int offsetWidth = Align.calculateWidth(offset, length);
        String blkLen = "Length";
        int blkLenWidth =
            Align.calculateWidth(blkLen, metaSize / metaBlkCnt * 10);
        String rawSize = "Raw-Size";
        int rawSizeWidth =
            Align.calculateWidth(rawSize, metaSizeUncompressed / metaBlkCnt
                * 10);
        String compression = "Compression";
        int compressionWidth = compression.length();
        out.printf("%s %s %s %s %s\n", Align.format(name, nameWidth,
            Align.CENTER), Align.format(offset, offsetWidth, Align.CENTER),
            Align.format(blkLen, blkLenWidth, Align.CENTER), Align.format(
                rawSize, rawSizeWidth, Align.CENTER), Align.format(compression,
                compressionWidth, Align.LEFT));

        for (Iterator<Map.Entry<String, MetaIndexEntry>> it =
            metaBlkEntrySet.iterator(); it.hasNext();) {
          Map.Entry<String, MetaIndexEntry> e = it.next();
          String blkName = e.getValue().getMetaName();
          BlockRegion region = e.getValue().getRegion();
          String blkCompression =
              e.getValue().getCompressionAlgorithm().getName();
          out.printf("%s %s %s %s %s\n", Align.format(blkName, nameWidth,
              Align.LEFT), Align.format(region.getOffset(), offsetWidth,
              Align.LEFT), Align.format(region.getCompressedSize(),
              blkLenWidth, Align.LEFT), Align.format(region.getRawSize(),
              rawSizeWidth, Align.LEFT), Align.format(blkCompression,
              compressionWidth, Align.LEFT));
        }
      }
    } finally {
      IOUtils.cleanup(LOG, reader, fsdis);
    }
  }
}
