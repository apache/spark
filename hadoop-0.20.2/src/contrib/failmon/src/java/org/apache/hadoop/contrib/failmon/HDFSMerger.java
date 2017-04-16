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
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;

public class HDFSMerger {

  Configuration hadoopConf;
  FileSystem hdfs;
  
  String hdfsDir;
  
  FileStatus [] inputFiles;

  Path outputFilePath;
  FSDataOutputStream outputFile;
    
  boolean compress;

  FileWriter fw;

  BufferedWriter writer;

  public HDFSMerger() throws IOException {

    String hadoopConfPath; 

    if (Environment.getProperty("hadoop.conf.path") == null)
      hadoopConfPath = "../../../conf";
    else
      hadoopConfPath = Environment.getProperty("hadoop.conf.path");

    // Read the configuration for the Hadoop environment
    Configuration hadoopConf = new Configuration();
    hadoopConf.addResource(new Path(hadoopConfPath + "/hadoop-default.xml"));
    hadoopConf.addResource(new Path(hadoopConfPath + "/hadoop-site.xml"));
    
    // determine the local output file name
    if (Environment.getProperty("local.tmp.filename") == null)
      Environment.setProperty("local.tmp.filename", "failmon.dat");
    
    // determine the upload location
    hdfsDir = Environment.getProperty("hdfs.upload.dir");
    if (hdfsDir == null)
      hdfsDir = "/failmon";

    hdfs = FileSystem.get(hadoopConf);
    
    Path hdfsDirPath = new Path(hadoopConf.get("fs.default.name") + hdfsDir);

    try {
      if (!hdfs.getFileStatus(hdfsDirPath).isDir()) {
	Environment.logInfo("HDFSMerger: Not an HDFS directory: " + hdfsDirPath.toString());
	System.exit(0);
      }
    } catch (FileNotFoundException e) {
      Environment.logInfo("HDFSMerger: Directory not found: " + hdfsDirPath.toString());
    }

    inputFiles = hdfs.listStatus(hdfsDirPath);

    outputFilePath = new Path(hdfsDirPath.toString() + "/" + "merge-"
			  + Calendar.getInstance().getTimeInMillis() + ".dat");
    outputFile = hdfs.create(outputFilePath);
    
    for (FileStatus fstatus : inputFiles) {
      appendFile(fstatus.getPath());
      hdfs.delete(fstatus.getPath());
    }

    outputFile.close();

    Environment.logInfo("HDFS file merging complete!");
  }

  private void appendFile (Path inputPath) throws IOException {
    
    FSDataInputStream anyInputFile = hdfs.open(inputPath);
    InputStream inputFile;
    byte buffer[] = new byte[4096];
    
    if (inputPath.toString().endsWith(LocalStore.COMPRESSION_SUFFIX)) {
      // the file is compressed
      inputFile = new ZipInputStream(anyInputFile);
      ((ZipInputStream) inputFile).getNextEntry();
    } else {
      inputFile = anyInputFile;
    }
    
    try {
      int bytesRead = 0;
      while ((bytesRead = inputFile.read(buffer)) > 0) {
	outputFile.write(buffer, 0, bytesRead);
      }
    } catch (IOException e) {
      Environment.logInfo("Error while copying file:" + inputPath.toString());
    } finally {
      inputFile.close();
    }    
  }

  
  public static void main(String [] args) {

    Environment.prepare("./conf/failmon.properties");

    try {
      new HDFSMerger();
    } catch (IOException e) {
      e.printStackTrace();
      }

  }
}
