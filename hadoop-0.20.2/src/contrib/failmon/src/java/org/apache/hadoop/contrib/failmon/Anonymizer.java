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

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**********************************************************
 * This class provides anonymization to SerializedRecord objects. It 
 * anonymizes all hostnames, ip addresses and file names/paths
 * that appear in EventRecords gathered from the logs
 * and other system utilities. Such values are hashed using a
 * cryptographically safe one-way-hash algorithm (MD5).
 * 
 **********************************************************/

public class Anonymizer {

  /**
	 * Anonymize hostnames, ip addresses and file names/paths
   * that appear in fields of a SerializedRecord.
   * 
	 * @param sr the input SerializedRecord
	 * 
	 * @return the anonymized SerializedRecord
	 */  	
  public static SerializedRecord anonymize(SerializedRecord sr)
      throws Exception {

    String hostname = sr.get("hostname");

    if (hostname == null)
      throw new Exception("Malformed SerializedRecord: no hostname found");

    if ("true".equalsIgnoreCase(Environment
        .getProperty("anonymizer.hash.hostnames"))) {
      // hash the node's hostname
      anonymizeField(sr, "message", hostname, "_hn_");
      anonymizeField(sr, "hostname", hostname, "_hn_");
      // hash all other hostnames
      String suffix = Environment.getProperty("anonymizer.hostname.suffix");
      if (suffix != null)
        anonymizeField(sr, "message", "(\\S+\\.)*" + suffix, "_hn_");
    }

    if ("true".equalsIgnoreCase(Environment.getProperty("anonymizer.hash.ips"))) {
      // hash all ip addresses
      String ipPattern = "(\\d{1,3}\\.){3}\\d{1,3}";
      anonymizeField(sr, "message", ipPattern, "_ip_");
      anonymizeField(sr, "ips", ipPattern, "_ip_");
      // if multiple ips are present for a node:
      int i = 0;
      while (sr.get("ips" + "#" + i) != null)
        anonymizeField(sr, "ips" + "#" + i++, ipPattern, "_ip_");

      if ("NIC".equalsIgnoreCase(sr.get("type")))
        anonymizeField(sr, "ipAddress", ipPattern, "_ip_");
    }

    if ("true".equalsIgnoreCase(Environment
        .getProperty("anonymizer.hash.filenames"))) {
      // hash every filename present in messages
      anonymizeField(sr, "message", "\\s+/(\\S+/)*[^:\\s]*", " _fn_");
      anonymizeField(sr, "message", "\\s+hdfs://(\\S+/)*[^:\\s]*",
          " hdfs://_fn_");
    }

    return sr;
  }

  /**
   * Anonymize hostnames, ip addresses and file names/paths
   * that appear in fields of an EventRecord, after it gets
   * serialized into a SerializedRecord.
   * 
   * @param er the input EventRecord
   * 
   * @return the anonymized SerializedRecord
   */   
  public static SerializedRecord anonymize(EventRecord er) throws Exception {
    return anonymize(new SerializedRecord(er));
  }

  
  private static String anonymizeField(SerializedRecord sr, String fieldName,
      String pattern, String prefix) {
    String txt = sr.get(fieldName);

    if (txt == null)
      return null;
    else {
      String anon = getMD5Hash(pattern);
      sr.set(fieldName, txt.replaceAll(pattern, (prefix == null ? "" : prefix)
          + anon));
      return txt;
    }
  }

  /**
   * Create the MD5 digest of an input text.
   * 
   * @param text the input text
   * 
   * @return the hexadecimal representation of the MD5 digest
   */   
  public static String getMD5Hash(String text) {
    MessageDigest md;
    byte[] md5hash = new byte[32];
    try {
      md = MessageDigest.getInstance("MD5");
      md.update(text.getBytes("iso-8859-1"), 0, text.length());
      md5hash = md.digest();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return convertToHex(md5hash);
  }

  private static String convertToHex(byte[] data) {
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < data.length; i++) {
      int halfbyte = (data[i] >>> 4) & 0x0F;
      int two_halfs = 0;
      do {
        if ((0 <= halfbyte) && (halfbyte <= 9))
          buf.append((char) ('0' + halfbyte));
        else
          buf.append((char) ('a' + (halfbyte - 10)));
        halfbyte = data[i] & 0x0F;
      } while (two_halfs++ < 1);
    }
    return buf.toString();
  }

}
