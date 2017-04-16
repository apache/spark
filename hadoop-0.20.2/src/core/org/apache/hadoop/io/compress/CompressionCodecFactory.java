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
package org.apache.hadoop.io.compress;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A factory that will find the correct codec for a given filename.
 */
public class CompressionCodecFactory {

  public static final Log LOG =
    LogFactory.getLog(CompressionCodecFactory.class.getName());

  /**
   * A map from the reversed filename suffixes to the codecs.
   * This is probably overkill, because the maps should be small, but it 
   * automatically supports finding the longest matching suffix. 
   */
  private SortedMap<String, CompressionCodec> codecs = null;

  /**
   * A map from the reversed filename suffixes to the codecs.
   * This is probably overkill, because the maps should be small, but it
   * automatically supports finding the longest matching suffix.
   */
  private Map<String, CompressionCodec> codecsByName = null;
  
  /**
   * A map from class names to the codecs
   */
  private HashMap<String, CompressionCodec> codecsByClassName = null;

  private void addCodec(CompressionCodec codec) {
    String suffix = codec.getDefaultExtension();
    codecs.put(new StringBuffer(suffix).reverse().toString(), codec);
    codecsByClassName.put(codec.getClass().getCanonicalName(), codec);

    String codecName = codec.getClass().getSimpleName();
    codecsByName.put(codecName.toLowerCase(), codec);
    if (codecName.endsWith("Codec")) {
      codecName = codecName.substring(0, codecName.length() - "Codec".length());
      codecsByName.put(codecName.toLowerCase(), codec);
    }
  }

  /**
   * Print the extension map out as a string.
   */
  public String toString() {
    StringBuffer buf = new StringBuffer();
    Iterator<Map.Entry<String, CompressionCodec>> itr = 
      codecs.entrySet().iterator();
    buf.append("{ ");
    if (itr.hasNext()) {
      Map.Entry<String, CompressionCodec> entry = itr.next();
      buf.append(entry.getKey());
      buf.append(": ");
      buf.append(entry.getValue().getClass().getName());
      while (itr.hasNext()) {
        entry = itr.next();
        buf.append(", ");
        buf.append(entry.getKey());
        buf.append(": ");
        buf.append(entry.getValue().getClass().getName());
      }
    }
    buf.append(" }");
    return buf.toString();
  }

  /**
   * Get the list of codecs listed in the configuration
   * @param conf the configuration to look in
   * @return a list of the Configuration classes or null if the attribute
   *         was not set
   */
  public static List<Class<? extends CompressionCodec>> getCodecClasses(Configuration conf) {
    String codecsString = conf.get("io.compression.codecs");
    if (codecsString != null) {
      List<Class<? extends CompressionCodec>> result
        = new ArrayList<Class<? extends CompressionCodec>>();
      StringTokenizer codecSplit = new StringTokenizer(codecsString, ",");
      while (codecSplit.hasMoreElements()) {
        String codecSubstring = codecSplit.nextToken();
        if (codecSubstring.length() != 0) {
          try {
            Class<?> cls = conf.getClassByName(codecSubstring);
            if (!CompressionCodec.class.isAssignableFrom(cls)) {
              throw new IllegalArgumentException("Class " + codecSubstring +
                                                 " is not a CompressionCodec");
            }
            result.add(cls.asSubclass(CompressionCodec.class));
          } catch (ClassNotFoundException ex) {
            throw new IllegalArgumentException("Compression codec " + 
                                               codecSubstring + " not found.",
                                               ex);
          }
        }
      }
      return result;
    } else {
      return null;
    }
  }
  
  /**
   * Sets a list of codec classes in the configuration.
   * @param conf the configuration to modify
   * @param classes the list of classes to set
   */
  public static void setCodecClasses(Configuration conf,
                                     List<Class> classes) {
    StringBuffer buf = new StringBuffer();
    Iterator<Class> itr = classes.iterator();
    if (itr.hasNext()) {
      Class cls = itr.next();
      buf.append(cls.getName());
      while(itr.hasNext()) {
        buf.append(',');
        buf.append(itr.next().getName());
      }
    }
    conf.set("io.compression.codecs", buf.toString());   
  }
  
  /**
   * Find the codecs specified in the config value io.compression.codecs 
   * and register them. Defaults to gzip and zip.
   */
  public CompressionCodecFactory(Configuration conf) {
    codecs = new TreeMap<String, CompressionCodec>();
    codecsByClassName = new HashMap<String, CompressionCodec>();
    codecsByName = new HashMap<String, CompressionCodec>();
    List<Class<? extends CompressionCodec>> codecClasses = getCodecClasses(conf);
    if (codecClasses == null) {
      addCodec(new GzipCodec());
      addCodec(new DefaultCodec());      
    } else {
      Iterator<Class<? extends CompressionCodec>> itr = codecClasses.iterator();
      while (itr.hasNext()) {
        CompressionCodec codec = ReflectionUtils.newInstance(itr.next(), conf);
        addCodec(codec);     
      }
    }
  }
  
  /**
   * Find the relevant compression codec for the given file based on its
   * filename suffix.
   * @param file the filename to check
   * @return the codec object
   */
  public CompressionCodec getCodec(Path file) {
    CompressionCodec result = null;
    if (codecs != null) {
      String filename = file.getName();
      String reversedFilename = new StringBuffer(filename).reverse().toString();
      SortedMap<String, CompressionCodec> subMap = 
        codecs.headMap(reversedFilename);
      if (!subMap.isEmpty()) {
        String potentialSuffix = subMap.lastKey();
        if (reversedFilename.startsWith(potentialSuffix)) {
          result = codecs.get(potentialSuffix);
        }
      }
    }
    return result;
  }
  
  /**
   * Find the relevant compression codec for the codec's canonical class name.
   * @param classname the canonical class name of the codec
   * @return the codec object
   */
  public CompressionCodec getCodecByClassName(String classname) {
    if (codecsByClassName == null) {
      return null;
    }
    return codecsByClassName.get(classname);
  }

    /**
     * Find the relevant compression codec for the codec's canonical class name
     * or by codec alias.
     * <p/>
     * Codec aliases are case insensitive.
     * <p/>
     * The code alias is the short class name (without the package name).
     * If the short class name ends with 'Codec', then there are two aliases for
     * the codec, the complete short class name and the short class name without
     * the 'Codec' ending. For example for the 'GzipCodec' codec class name the
     * alias are 'gzip' and 'gzipcodec'.
     *
     * @param codecName the canonical class name of the codec
     * @return the codec object
     */
    public CompressionCodec getCodecByName(String codecName) {
      if (codecsByClassName == null) {
        return null;
      }
      CompressionCodec codec = getCodecByClassName(codecName);
      if (codec == null) {
        // trying to get the codec by name in case the name was specified instead a class
        codec = codecsByName.get(codecName.toLowerCase());
      }
      return codec;
    }

    /**
     * Find the relevant compression codec for the codec's canonical class name
     * or by codec alias and returns its implemetation class.
     * <p/>
     * Codec aliases are case insensitive.
     * <p/>
     * The code alias is the short class name (without the package name).
     * If the short class name ends with 'Codec', then there are two aliases for
     * the codec, the complete short class name and the short class name without
     * the 'Codec' ending. For example for the 'GzipCodec' codec class name the
     * alias are 'gzip' and 'gzipcodec'.
     *
     * @param codecName the canonical class name of the codec
     * @return the codec class
     */
    public Class<? extends CompressionCodec> getCodecClassByName(String codecName) {
      CompressionCodec codec = getCodecByName(codecName);
      if (codec == null) {
        return null;
      }
      return codec.getClass();
    }

  /**
   * Removes a suffix from a filename, if it has it.
   * @param filename the filename to strip
   * @param suffix the suffix to remove
   * @return the shortened filename
   */
  public static String removeSuffix(String filename, String suffix) {
    if (filename.endsWith(suffix)) {
      return filename.substring(0, filename.length() - suffix.length());
    }
    return filename;
  }
  
  /**
   * A little test program.
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    boolean encode = false;
    for(int i=0; i < args.length; ++i) {
      if ("-in".equals(args[i])) {
        encode = true;
      } else if ("-out".equals(args[i])) {
        encode = false;
      } else {
        CompressionCodec codec = factory.getCodec(new Path(args[i]));
        if (codec == null) {
          System.out.println("Codec for " + args[i] + " not found.");
        } else { 
          if (encode) {
            CompressionOutputStream out = 
              codec.createOutputStream(new java.io.FileOutputStream(args[i]));
            byte[] buffer = new byte[100];
            String inFilename = removeSuffix(args[i], 
                                             codec.getDefaultExtension());
            java.io.InputStream in = new java.io.FileInputStream(inFilename);
            int len = in.read(buffer);
            while (len > 0) {
              out.write(buffer, 0, len);
              len = in.read(buffer);
            }
            in.close();
            out.close();
          } else {
            CompressionInputStream in = 
              codec.createInputStream(new java.io.FileInputStream(args[i]));
            byte[] buffer = new byte[100];
            int len = in.read(buffer);
            while (len > 0) {
              System.out.write(buffer, 0, len);
              len = in.read(buffer);
            }
            in.close();
          }
        }
      }
    }
  }
}
