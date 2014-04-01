/*
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

package org.apache.hadoop.record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Random;

/**
 * Benchmark for various types of serializations
 */
public class RecordBench {
  
  private static class Times {
    long init;
    long serialize;
    long deserialize;
    long write;
    long readFields;
  };
  
  private static final long SEED = 0xDEADBEEFL;
  private static final Random rand = new Random();
  
  /** Do not allow to create a new instance of RecordBench */
  private RecordBench() {}
  
  private static void initBuffers(Record[] buffers) {
    final int BUFLEN = 32;
    for (int idx = 0; idx < buffers.length; idx++) {
      buffers[idx] = new RecBuffer();
      int buflen = rand.nextInt(BUFLEN);
      byte[] bytes = new byte[buflen];
      rand.nextBytes(bytes);
      ((RecBuffer)buffers[idx]).setData(new Buffer(bytes));
    }
  }
  
  private static void initStrings(Record[] strings) {
    final int STRLEN = 32;
    for (int idx = 0; idx < strings.length; idx++) {
      strings[idx] = new RecString();
      int strlen = rand.nextInt(STRLEN);
      StringBuilder sb = new StringBuilder(strlen);
      for (int ich = 0; ich < strlen; ich++) {
        int cpt = 0;
        while (true) {
          cpt = rand.nextInt(0x10FFFF+1);
          if (Utils.isValidCodePoint(cpt)) {
            break;
          }
        }
        sb.appendCodePoint(cpt);
      }
      ((RecString)strings[idx]).setData(sb.toString());
    }
  }
  
  private static void initInts(Record[] ints) {
    for (int idx = 0; idx < ints.length; idx++) {
      ints[idx] = new RecInt();
      ((RecInt)ints[idx]).setData(rand.nextInt());
    }
  }
  
  private static Record[] makeArray(String type, int numRecords, Times times) {
    Method init = null;
    try {
      init = RecordBench.class.getDeclaredMethod("init"+
                                                 toCamelCase(type) + "s",
                                                 new Class[] {Record[].class});
    } catch (NoSuchMethodException ex) {
      throw new RuntimeException(ex);
    }

    Record[] records = new Record[numRecords];
    times.init = System.nanoTime();
    try {
      init.invoke(null, new Object[]{records});
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    times.init = System.nanoTime() - times.init;
    return records;
  }
  
  private static void runBinaryBench(String type, int numRecords, Times times)
    throws IOException {
    Record[] records = makeArray(type, numRecords, times);
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    BinaryRecordOutput rout = new BinaryRecordOutput(bout);
    DataOutputStream dout = new DataOutputStream(bout);
    
    for(int idx = 0; idx < numRecords; idx++) {
      records[idx].serialize(rout);
    }
    bout.reset();
    
    times.serialize = System.nanoTime();
    for(int idx = 0; idx < numRecords; idx++) {
      records[idx].serialize(rout);
    }
    times.serialize = System.nanoTime() - times.serialize;
    
    byte[] serialized = bout.toByteArray();
    ByteArrayInputStream bin = new ByteArrayInputStream(serialized);
    BinaryRecordInput rin = new BinaryRecordInput(bin);
    
    times.deserialize = System.nanoTime();
    for(int idx = 0; idx < numRecords; idx++) {
      records[idx].deserialize(rin);
    }
    times.deserialize = System.nanoTime() - times.deserialize;
    
    bout.reset();
    
    times.write = System.nanoTime();
    for(int idx = 0; idx < numRecords; idx++) {
      records[idx].write(dout);
    }
    times.write = System.nanoTime() - times.write;
    
    bin.reset();
    DataInputStream din = new DataInputStream(bin);
    
    times.readFields = System.nanoTime();
    for(int idx = 0; idx < numRecords; idx++) {
      records[idx].readFields(din);
    }
    times.readFields = System.nanoTime() - times.readFields;
  }
  
  private static void runCsvBench(String type, int numRecords, Times times)
    throws IOException {
    Record[] records = makeArray(type, numRecords, times);
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    CsvRecordOutput rout = new CsvRecordOutput(bout);
    
    for(int idx = 0; idx < numRecords; idx++) {
      records[idx].serialize(rout);
    }
    bout.reset();
    
    times.serialize = System.nanoTime();
    for(int idx = 0; idx < numRecords; idx++) {
      records[idx].serialize(rout);
    }
    times.serialize = System.nanoTime() - times.serialize;
    
    byte[] serialized = bout.toByteArray();
    ByteArrayInputStream bin = new ByteArrayInputStream(serialized);
    CsvRecordInput rin = new CsvRecordInput(bin);
    
    times.deserialize = System.nanoTime();
    for(int idx = 0; idx < numRecords; idx++) {
      records[idx].deserialize(rin);
    }
    times.deserialize = System.nanoTime() - times.deserialize;
  }
  
  private static void runXmlBench(String type, int numRecords, Times times)
    throws IOException {
    Record[] records = makeArray(type, numRecords, times);
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    XmlRecordOutput rout = new XmlRecordOutput(bout);
    
    for(int idx = 0; idx < numRecords; idx++) {
      records[idx].serialize(rout);
    }
    bout.reset();
    
    bout.write("<records>\n".getBytes());
    
    times.serialize = System.nanoTime();
    for(int idx = 0; idx < numRecords; idx++) {
      records[idx].serialize(rout);
    }
    times.serialize = System.nanoTime() - times.serialize;
    
    bout.write("</records>\n".getBytes());
    
    byte[] serialized = bout.toByteArray();
    ByteArrayInputStream bin = new ByteArrayInputStream(serialized);
        
    times.deserialize = System.nanoTime();
    XmlRecordInput rin = new XmlRecordInput(bin);
    for(int idx = 0; idx < numRecords; idx++) {
      records[idx].deserialize(rin);
    }
    times.deserialize = System.nanoTime() - times.deserialize;
  }

  private static void printTimes(String type,
                                 String format,
                                 int numRecords,
                                 Times times) {
    System.out.println("Type: " + type + " Format: " + format +
                       " #Records: "+numRecords);
    if (times.init != 0) {
      System.out.println("Initialization Time (Per record) : "+
                         times.init/numRecords + " Nanoseconds");
    }
    
    if (times.serialize != 0) {
      System.out.println("Serialization Time (Per Record) : "+
                         times.serialize/numRecords + " Nanoseconds");
    }
    
    if (times.deserialize != 0) {
      System.out.println("Deserialization Time (Per Record) : "+
                         times.deserialize/numRecords + " Nanoseconds");
    }
    
    if (times.write != 0) {
      System.out.println("Write Time (Per Record) : "+
                         times.write/numRecords + " Nanoseconds");
    }
    
    if (times.readFields != 0) {
      System.out.println("ReadFields Time (Per Record) : "+
                         times.readFields/numRecords + " Nanoseconds");
    }
    
    System.out.println();
  }
  
  private static String toCamelCase(String inp) {
    char firstChar = inp.charAt(0);
    if (Character.isLowerCase(firstChar)) {
      return ""+Character.toUpperCase(firstChar) + inp.substring(1);
    }
    return inp;
  }
  
  private static void exitOnError() {
    String usage = "RecordBench {buffer|string|int}"+
      " {binary|csv|xml} <numRecords>";
    System.out.println(usage);
    System.exit(1);
  }
  
  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) throws IOException {
    String version = "RecordBench v0.1";
    System.out.println(version+"\n");
    
    if (args.length != 3) {
      exitOnError();
    }
    
    String typeName = args[0];
    String format = args[1];
    int numRecords = Integer.decode(args[2]).intValue();
    
    Method bench = null;
    try {
      bench = RecordBench.class.getDeclaredMethod("run"+
                                                  toCamelCase(format) + "Bench",
                                                  new Class[] {String.class, Integer.TYPE, Times.class});
    } catch (NoSuchMethodException ex) {
      ex.printStackTrace();
      exitOnError();
    }
    
    if (numRecords < 0) {
      exitOnError();
    }
    
    // dry run
    rand.setSeed(SEED);
    Times times = new Times();
    try {
      bench.invoke(null, new Object[] {typeName, numRecords, times});
    } catch (Exception ex) {
      ex.printStackTrace();
      System.exit(1);
    }
    
    // timed run
    rand.setSeed(SEED);
    try {
      bench.invoke(null, new Object[] {typeName, numRecords, times});
    } catch (Exception ex) {
      ex.printStackTrace();
      System.exit(1);
    }
    printTimes(typeName, format, numRecords, times);
  }
}
