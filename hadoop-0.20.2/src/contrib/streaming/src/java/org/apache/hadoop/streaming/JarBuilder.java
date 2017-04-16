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

package org.apache.hadoop.streaming;

import java.io.*;
import java.util.*;
import java.util.jar.*;
import java.util.zip.ZipException;

/**
 * This class is the main class for generating job.jar
 * for Hadoop Streaming jobs. It includes the files specified 
 * with the -file option and includes them in the jar. Also,
 * hadoop-streaming is a user level appplication, so all the classes
 * with hadoop-streaming that are needed in the job are also included
 * in the job.jar.
 */
public class JarBuilder {

  public JarBuilder() {
  }

  public void setVerbose(boolean v) {
    this.verbose = v;
  }

  public void merge(List srcNames, List srcUnjar, String dstJar) throws IOException {
    String source = null;
    JarOutputStream jarOut = null;
    JarFile jarSource = null;
    jarOut = new JarOutputStream(new FileOutputStream(dstJar));
    boolean throwing = false;
    try {
      if (srcNames != null) {
        Iterator iter = srcNames.iterator();
        while (iter.hasNext()) {
          source = (String) iter.next();
          File fsource = new File(source);
          String base = getBasePathInJarOut(source);
          if (!fsource.exists()) {
            throwing = true;
            throw new FileNotFoundException(fsource.getAbsolutePath());
          }
          if (fsource.isDirectory()) {
            addDirectory(jarOut, base, fsource, 0);
          } else {
            addFileStream(jarOut, base, fsource);
          }
        }
      }
      if (srcUnjar != null) {
        Iterator iter = srcUnjar.iterator();
        while (iter.hasNext()) {
          source = (String) iter.next();
          jarSource = new JarFile(source);
          addJarEntries(jarOut, jarSource);
          jarSource.close();
        }

      }
    } finally {
      try {
        jarOut.close();
      } catch (ZipException z) {
        if (!throwing) {
          throw new IOException(z.toString());
        }
      }
    }
  }

  protected String fileExtension(String file) {
    int leafPos = file.lastIndexOf('/');
    if (leafPos == file.length() - 1) return "";
    String leafName = file.substring(leafPos + 1);
    int dotPos = leafName.lastIndexOf('.');
    if (dotPos == -1) return "";
    String ext = leafName.substring(dotPos + 1);
    return ext;
  }

  /** @return empty or a jar base path. Must not start with '/' */
  protected String getBasePathInJarOut(String sourceFile) {
    // TaskRunner will unjar and append to classpath: .:classes/:lib/*    	
    String ext = fileExtension(sourceFile);
    if (ext.equals("class")) {
      return "classes/"; // or ""
    } else if (ext.equals("jar") || ext.equals("zip")) {
      return "lib/";
    } else {
      return "";
    }
  }

  private void addJarEntries(JarOutputStream dst, JarFile src) throws IOException {
    Enumeration entries = src.entries();
    JarEntry entry = null;
    while (entries.hasMoreElements()) {
      entry = (JarEntry) entries.nextElement();
      //if (entry.getName().startsWith("META-INF/")) continue; 
      InputStream in = src.getInputStream(entry);
      addNamedStream(dst, entry.getName(), in);
    }
  }

  /** @param name path in jar for this jar element. Must not start with '/' */
  void addNamedStream(JarOutputStream dst, String name, InputStream in) throws IOException {
    if (verbose) {
      System.err.println("JarBuilder.addNamedStream " + name);
    }
    try {
      dst.putNextEntry(new JarEntry(name));
      int bytesRead = 0;
      while ((bytesRead = in.read(buffer, 0, BUFF_SIZE)) != -1) {
        dst.write(buffer, 0, bytesRead);
      }
    } catch (ZipException ze) {
      if (ze.getMessage().indexOf("duplicate entry") >= 0) {
        if (verbose) {
          System.err.println(ze + " Skip duplicate entry " + name);
        }
      } else {
        throw ze;
      }
    } finally {
      in.close();
      dst.flush();
      dst.closeEntry();
    }
  }

  void addFileStream(JarOutputStream dst, String jarBaseName, File file) throws IOException {
    FileInputStream in = new FileInputStream(file);
    String name = jarBaseName + file.getName();
    addNamedStream(dst, name, in);
    in.close();
  }

  void addDirectory(JarOutputStream dst, String jarBaseName, File dir, int depth) throws IOException {
    File[] contents = dir.listFiles();
    if (contents != null) {
      for (int i = 0; i < contents.length; i++) {
        File f = contents[i];
        String fBaseName = (depth == 0) ? "" : dir.getName();
        if (jarBaseName.length() > 0) {
          fBaseName = jarBaseName + "/" + fBaseName;
        }
        if (f.isDirectory()) {
          addDirectory(dst, fBaseName, f, depth + 1);
        } else {
          addFileStream(dst, fBaseName + "/", f);
        }
      }
    }
  }

  /** Test program */
  public static void main(String args[]) {
    // args = new String[] { "C:/Temp/merged.jar", "C:/jdk1.5.0/jre/lib/ext/dnsns.jar",  "/Temp/addtojar2.log", "C:/jdk1.5.0/jre/lib/ext/mtest.jar", "C:/Temp/base"};
    if (args.length < 2) {
      System.err.println("Usage: JarFiles merged.jar [src.jar | dir | file ]+");
    } else {
      JarBuilder jarFiles = new JarBuilder();
      List names = new ArrayList();
      List unjar = new ArrayList();
      for (int i = 1; i < args.length; i++) {
        String f = args[i];
        String ext = jarFiles.fileExtension(f);
        boolean expandAsJar = ext.equals("jar") || ext.equals("zip");
        if (expandAsJar) {
          unjar.add(f);
        } else {
          names.add(f);
        }
      }
      try {
        jarFiles.merge(names, unjar, args[0]);
        Date lastMod = new Date(new File(args[0]).lastModified());
        System.out.println("Merge done to " + args[0] + " " + lastMod);
      } catch (Exception ge) {
        ge.printStackTrace(System.err);
      }
    }
  }

  private static final int BUFF_SIZE = 32 * 1024;
  private byte buffer[] = new byte[BUFF_SIZE];
  protected boolean verbose = false;
}
