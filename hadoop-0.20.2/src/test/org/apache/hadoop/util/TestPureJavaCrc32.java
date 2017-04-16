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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test to verify that the pure-Java CRC32 algorithm gives
 * the same results as the built-in implementation.
 */
public class TestPureJavaCrc32 {
  private final CRC32 theirs = new CRC32();
  private final PureJavaCrc32 ours = new PureJavaCrc32();

  @Test
  public void testCorrectness() throws Exception {
    checkSame();

    theirs.update(104);
    ours.update(104);
    checkSame();

    checkOnBytes(new byte[] {40, 60, 97, -70}, false);
    
    checkOnBytes("hello world!".getBytes("UTF-8"), false);

    for (int i = 0; i < 10000; i++) {
      byte randomBytes[] = new byte[new Random().nextInt(2048)];
      new Random().nextBytes(randomBytes);
      checkOnBytes(randomBytes, false);
    }
    
  }

  private void checkOnBytes(byte[] bytes, boolean print) {
    theirs.reset();
    ours.reset();
    checkSame();
    
    for (int i = 0; i < bytes.length; i++) {
      ours.update(bytes[i]);
      theirs.update(bytes[i]);
      checkSame();
    }

    if (print) {
      System.out.println("theirs:\t" + Long.toHexString(theirs.getValue())
                         + "\nours:\t" + Long.toHexString(ours.getValue()));
    }
  
    theirs.reset();
    ours.reset();
    
    ours.update(bytes, 0, bytes.length);
    theirs.update(bytes, 0, bytes.length);
    if (print) {
      System.out.println("theirs:\t" + Long.toHexString(theirs.getValue())
                         + "\nours:\t" + Long.toHexString(ours.getValue()));
    }

    checkSame();
    
    if (bytes.length >= 10) {
      ours.update(bytes, 5, 5);
      theirs.update(bytes, 5, 5);
      checkSame();
    }
  }

  private void checkSame() {
    Assert.assertEquals(theirs.getValue(), ours.getValue());
  }

  /**
   * Generate a table to perform checksums based on the same CRC-32 polynomial
   * that java.util.zip.CRC32 uses.
   */
  public static class Table {
    private static final int polynomial = 0xEDB88320;

    private final int[][] tables;

    private Table(final int nBits, final int nTables) {
      tables = new int[nTables][];
      final int size = 1 << nBits;
      for(int i = 0; i < tables.length; i++) {
        tables[i] = new int[size];
      }

      //compute the first table
      final int[] first = tables[0];
      for (int i = 0; i < first.length; i++) {
        int crc = i;
        for (int j = 0; j < nBits; j++) {
          if ((crc & 1) == 1) {
            crc >>>= 1;
            crc ^= polynomial;
          } else {
            crc >>>= 1;
          }
        }
        first[i] = crc;
      }

      //compute the remaining tables
      final int mask = first.length - 1;
      for(int j = 1; j < tables.length; j++) {
        final int[] previous = tables[j-1];
        final int[] current = tables[j];
        for (int i = 0; i < current.length; i++) {
          current[i] = (previous[i] >>> nBits) ^ first[previous[i] & mask];
        }
      }
    }

    String[] toStrings(String nameformat) {
      final String[] s = new String[tables.length];
      for (int j = 0; j < tables.length; j++) {
        final int[] t = tables[j];
        final StringBuilder b = new StringBuilder();
        b.append(String.format("  static final int[] " + nameformat
            + " = new int[] {", j));
        for (int i = 0; i < t.length;) {
          b.append("\n    ");
          for(int k = 0; k < 4; k++) {
            b.append(String.format("0x%08X, ", t[i++]));
          }
        }
        b.setCharAt(b.length() - 2, '\n');
        s[j] = b.toString() + " };\n";
      }
      return s;
    }

    /** {@inheritDoc} */
    public String toString() {
      final StringBuilder b = new StringBuilder();
      for(String s : toStrings(String.format("T%d_",
          Integer.numberOfTrailingZeros(tables[0].length)) + "%d")) {
        b.append(s);
      }
      return b.toString();
    }

    /** Generate CRC-32 lookup tables */
    public static void main(String[] args) throws FileNotFoundException {
      int i = 8;
      final PrintStream out = new PrintStream(
          new FileOutputStream("table" + i + ".txt"), true);
      final Table t = new Table(i, 16);
      final String s = t.toString();
      System.out.println(s);
      out.println(s);
    }
  }
  
  /**
   * Performance tests to compare performance of the Pure Java implementation
   * to the built-in java.util.zip implementation. This can be run from the
   * command line with:
   *
   *   java -cp path/to/test/classes:path/to/common/classes \
   *      'org.apache.hadoop.util.TestPureJavaCrc32$PerformanceTest'
   *
   * The output is in JIRA table format.
   */
  public static class PerformanceTest {
    public static final int MAX_LEN = 32*1024*1024; // up to 32MB chunks
    public static final int BYTES_PER_SIZE = MAX_LEN * 4;

    static final Checksum zip = new CRC32(); 
    static final Checksum[] CRCS = {new PureJavaCrc32()};

    public static void main(String args[]) {
      printSystemProperties(System.out);
      doBench(CRCS, System.out);
    }

    private static void printCell(String s, int width, PrintStream out) {
      final int w = s.length() > width? s.length(): width;
      out.printf(" %" + w + "s |", s);
    }

    private static void doBench(final Checksum[] crcs, final PrintStream out) {
      final ArrayList<Checksum> a = new ArrayList<Checksum>();
      a.add(zip);
      for (Checksum c : crcs)
        if(c.getClass() != zip.getClass())
          a.add(c);
      doBench(a, out);
    }

    private static void doBench(final List<Checksum> crcs, final PrintStream out
        ) {
      final byte[] bytes = new byte[MAX_LEN];
      new Random().nextBytes(bytes);

      // Print header
      out.printf("\nPerformance Table (The unit is MB/sec)\n||");
      final String title = "Num Bytes";
      printCell("Num Bytes", 0, out);
      for (Checksum c : crcs) {
        out.printf("|");
        printCell(c.getClass().getSimpleName(), 8, out);
      }
      out.printf("|\n");

      // Warm up implementations to get jit going.
      for (Checksum c : crcs) {
        doBench(c, bytes, 2, null);
        doBench(c, bytes, 2101, null);
      }

      // Test on a variety of sizes
      for (int size = 1; size < MAX_LEN; size *= 2) {
        out.printf("|");
        printCell(String.valueOf(size), title.length()+1, out);

        Long expected = null;
        for(Checksum c : crcs) {
          System.gc();
          final long result = doBench(c, bytes, size, out);
          if(c.getClass() == zip.getClass()) {
            expected = result;
          } else if (result != expected) {
            throw new RuntimeException(c.getClass() + " has bugs!");
          }
            
        }
        out.printf("\n");
      }
    }

    private static long doBench(Checksum crc, byte[] bytes, int size,
        PrintStream out) {
      final String name = crc.getClass().getSimpleName();
      final int trials = BYTES_PER_SIZE / size;

      final long st = System.nanoTime();
      crc.reset();
      for (int i = 0; i < trials; i++) {
        crc.update(bytes, 0, size);
      }
      final long result = crc.getValue();
      final long et = System.nanoTime();

      double mbProcessed = trials * size / 1024.0 / 1024.0;
      double secsElapsed = (et - st) / 1000000000.0d;
      if (out != null) {
        final String s = String.format("%9.3f",  mbProcessed/secsElapsed);
        printCell(s, name.length()+1, out);
      }
      return result;
    }
    
    private static void printSystemProperties(PrintStream out) {
      final String[] names = {
          "java.version",
          "java.runtime.name",
          "java.runtime.version",
          "java.vm.version",
          "java.vm.vendor",
          "java.vm.name",
          "java.vm.specification.version",
          "java.specification.version",
          "os.arch",
          "os.name",
          "os.version"
      };
      final Properties p = System.getProperties();
      for(String n : names) {
        out.println(n + " = " + p.getProperty(n));
      }
    }
  }
}
