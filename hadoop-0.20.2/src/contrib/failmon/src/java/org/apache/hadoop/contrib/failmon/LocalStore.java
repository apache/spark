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

package org.apache.hadoop.contrib.failmon;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**********************************************************
 * This class takes care of the temporary local storage of 
 * gathered metrics before they get uploaded into HDFS. It writes 
 * Serialized Records as lines in a temporary file and then 
 * compresses and uploads it into HDFS.
 * 
 **********************************************************/

public class LocalStore {

  public final static char FIELD_SEPARATOR = '|';

  public final static char RECORD_SEPARATOR = '\n';

  public final static String COMPRESSION_SUFFIX = ".zip";

  public final static int UPLOAD_INTERVAL = 600;

  String filename;
  String hdfsDir;

  boolean compress;

  FileWriter fw;

  BufferedWriter writer;

  /**
   * Create an instance of the class and read the configuration
   * file to determine some output parameters. Then, initiate the 
   * structured needed for the buffered I/O (so that smal appends
   * can be handled efficiently).
   * 
   */ 

  public LocalStore() {
    // determine the local output file name
    if (Environment.getProperty("local.tmp.filename") == null)
      Environment.setProperty("local.tmp.filename", "failmon.dat");
    
    // local.tmp.dir has been set by the Executor
    if (Environment.getProperty("local.tmp.dir") == null)
      Environment.setProperty("local.tmp.dir", System.getProperty("java.io.tmpdir"));
    
    filename = Environment.getProperty("local.tmp.dir") + "/" +
      Environment.getProperty("local.tmp.filename");

    // determine the upload location
    hdfsDir = Environment.getProperty("hdfs.upload.dir");
    if (hdfsDir == null)
      hdfsDir = "/failmon";

    // determine if compression is enabled
    compress = true;
    if ("false".equalsIgnoreCase(Environment
        .getProperty("local.tmp.compression")))
      compress = false;

    try {
      fw = new FileWriter(filename, true);
      writer = new BufferedWriter(fw);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Insert an EventRecord to the local storage, after it
   * gets serialized and anonymized.
   * 
   * @param er the EventRecord to be inserted
   */ 
  
  public void insert(EventRecord er) {
    SerializedRecord sr = new SerializedRecord(er);
    try {
      Anonymizer.anonymize(sr);
    } catch (Exception e) {
      e.printStackTrace();
    }
    append(sr);
  }

  /**
   * Insert an array of EventRecords to the local storage, after they
   * get serialized and anonymized.
   * 
   * @param ers the array of EventRecords to be inserted
   */
  public void insert(EventRecord[] ers) {
    for (EventRecord er : ers)
      insert(er);
  }

  private void append(SerializedRecord sr) {
    try {
      writer.write(pack(sr).toString());
      writer.write(RECORD_SEPARATOR);
      // writer.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Pack a SerializedRecord into an array of bytes
   * 
   * @param sr the SerializedRecord to be packed
   */
  public static StringBuffer pack(SerializedRecord sr) {
    StringBuffer sb = new StringBuffer();

    ArrayList<String> keys = new ArrayList<String>(sr.fields.keySet());

    if (sr.isValid())
      SerializedRecord.arrangeKeys(keys);

    for (int i = 0; i < keys.size(); i++) {
      String value = sr.fields.get(keys.get(i));
      sb.append(keys.get(i) + ":" + value);
      sb.append(FIELD_SEPARATOR);
    }
    return sb;
  }

  /**
   * Upload the local file store into HDFS, after it 
   * compressing it. Then a new local file is created 
   * as a temporary record store.
   * 
   */
  public void upload() {
    try {
      writer.flush();
      if (compress)
        zipCompress(filename);
      String remoteName = "failmon-";
      if ("true".equalsIgnoreCase(Environment.getProperty("anonymizer.hash.hostnames")))
        remoteName += Anonymizer.getMD5Hash(InetAddress.getLocalHost().getCanonicalHostName()) + "-";
      else
        remoteName += InetAddress.getLocalHost().getCanonicalHostName() + "-"; 
      remoteName += Calendar.getInstance().getTimeInMillis();//.toString();
      if (compress)
	copyToHDFS(filename + COMPRESSION_SUFFIX, hdfsDir + "/" + remoteName + COMPRESSION_SUFFIX);
      else
	copyToHDFS(filename, hdfsDir + "/" + remoteName);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // delete and re-open
    try {
      fw.close();
      fw = new FileWriter(filename);
      writer = new BufferedWriter(fw);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Compress a text file using the ZIP compressing algorithm.
   * 
   * @param filename the path to the file to be compressed
   */
  public static void zipCompress(String filename) throws IOException {
    FileOutputStream fos = new FileOutputStream(filename + COMPRESSION_SUFFIX);
    CheckedOutputStream csum = new CheckedOutputStream(fos, new CRC32());
    ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(csum));
    out.setComment("Failmon records.");

    BufferedReader in = new BufferedReader(new FileReader(filename));
    out.putNextEntry(new ZipEntry(new File(filename).getName()));
    int c;
    while ((c = in.read()) != -1)
      out.write(c);
    in.close();

    out.finish();
    out.close();
  }

  /**
   * Copy a local file to HDFS
   * 
   * @param localFile the filename of the local file
   * @param hdfsFile the HDFS filename to copy to
   */
  public static void copyToHDFS(String localFile, String hdfsFile) throws IOException {

    String hadoopConfPath; 

    if (Environment.getProperty("hadoop.conf.path") == null)
      hadoopConfPath = "../../../conf";
    else
      hadoopConfPath = Environment.getProperty("hadoop.conf.path");

    // Read the configuration for the Hadoop environment
    Configuration hadoopConf = new Configuration();
    hadoopConf.addResource(new Path(hadoopConfPath + "/hadoop-default.xml"));
    hadoopConf.addResource(new Path(hadoopConfPath + "/hadoop-site.xml"));

    // System.out.println(hadoopConf.get("hadoop.tmp.dir"));
    // System.out.println(hadoopConf.get("fs.default.name"));
    FileSystem fs = FileSystem.get(hadoopConf);

    // HadoopDFS deals with Path
    Path inFile = new Path("file://" + localFile);
    Path outFile = new Path(hadoopConf.get("fs.default.name") + hdfsFile);

     // Read from and write to new file
    Environment.logInfo("Uploading to HDFS (file " + outFile + ") ...");
    fs.copyFromLocalFile(false, inFile, outFile);
  }

  /**
   * Close the temporary local file
   * 
   */ 
  public void close() {
    try {
    writer.flush();
    writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
