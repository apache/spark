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
package org.apache.hadoop.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * This class is responsible for quoting HTML characters.
 */
public class HtmlQuoting {
  private static final byte[] ampBytes = "&amp;".getBytes();
  private static final byte[] aposBytes = "&apos;".getBytes();
  private static final byte[] gtBytes = "&gt;".getBytes();
  private static final byte[] ltBytes = "&lt;".getBytes();
  private static final byte[] quotBytes = "&quot;".getBytes();

  /**
   * Does the given string need to be quoted?
   * @param data the string to check
   * @param off the starting position
   * @param len the number of bytes to check
   * @return does the string contain any of the active html characters?
   */
  public static boolean needsQuoting(byte[] data, int off, int len) {
    for(int i=off; i< off+len; ++i) {
      switch(data[i]) {
      case '&':
      case '<':
      case '>':
      case '\'':
      case '"':
        return true;
      default:
        break;
      }
    }
    return false;
  }

  /**
   * Does the given string need to be quoted?
   * @param str the string to check
   * @return does the string contain any of the active html characters?
   */
  public static boolean needsQuoting(String str) {
    if (str == null) {
      return false;
    }
    byte[] bytes = str.getBytes();
    return needsQuoting(bytes, 0 , bytes.length);
  }

  /**
   * Quote all of the active HTML characters in the given string as they
   * are added to the buffer.
   * @param output the stream to write the output to
   * @param buffer the byte array to take the characters from
   * @param off the index of the first byte to quote
   * @param len the number of bytes to quote
   */
  public static void quoteHtmlChars(OutputStream output, byte[] buffer,
                                    int off, int len) throws IOException {
    for(int i=off; i < off+len; i++) {
      switch (buffer[i]) {
      case '&': output.write(ampBytes); break;
      case '<': output.write(ltBytes); break;
      case '>': output.write(gtBytes); break;
      case '\'': output.write(aposBytes); break;
      case '"': output.write(quotBytes); break;
      default: output.write(buffer, i, 1);
      }
    }
  }
  
  /**
   * Quote the given item to make it html-safe.
   * @param item the string to quote
   * @return the quoted string
   */
  public static String quoteHtmlChars(String item) {
    if (item == null) {
      return null;
    }
    byte[] bytes = item.getBytes();
    if (needsQuoting(bytes, 0, bytes.length)) {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      try {
        quoteHtmlChars(buffer, bytes, 0, bytes.length);
      } catch (IOException ioe) {
        // Won't happen, since it is a bytearrayoutputstream
      }
      return buffer.toString();
    } else {
      return item;
    }
  }

  /**
   * Return an output stream that quotes all of the output.
   * @param out the stream to write the quoted output to
   * @return a new stream that the application show write to
   * @throws IOException if the underlying output fails
   */
  public static OutputStream quoteOutputStream(final OutputStream out
                                               ) throws IOException {
    return new OutputStream() {
      private byte[] data = new byte[1];
      @Override
      public void write(byte[] data, int off, int len) throws IOException {
        quoteHtmlChars(out, data, off, len);
      }
      
      @Override
      public void write(int b) throws IOException {
        data[0] = (byte) b;
        quoteHtmlChars(out, data, 0, 1);
      }
      
      @Override
      public void flush() throws IOException {
        out.flush();
      }
      
      @Override
      public void close() throws IOException {
        out.close();
      }
    };
  }

  /**
   * Remove HTML quoting from a string.
   * @param item the string to unquote
   * @return the unquoted string
   */
  public static String unquoteHtmlChars(String item) {
    if (item == null) {
      return null;
    }
    int next = item.indexOf('&');
    // nothing was quoted
    if (next == -1) {
      return item;
    }
    int len = item.length();
    int posn = 0;
    StringBuilder buffer = new StringBuilder();
    while (next != -1) {
      buffer.append(item.substring(posn, next));
      if (item.startsWith("&amp;", next)) {
        buffer.append('&');
        next += 5;
      } else if (item.startsWith("&apos;", next)) {
        buffer.append('\'');
        next += 6;        
      } else if (item.startsWith("&gt;", next)) {
        buffer.append('>');
        next += 4;
      } else if (item.startsWith("&lt;", next)) {
        buffer.append('<');
        next += 4;
      } else if (item.startsWith("&quot;", next)) {
        buffer.append('"');
        next += 6;
      } else {
        int end = item.indexOf(';', next)+1;
        if (end == 0) {
          end = len;
        }
        throw new IllegalArgumentException("Bad HTML quoting for " + 
                                           item.substring(next,end));
      }
      posn = next;
      next = item.indexOf('&', posn);
    }
    buffer.append(item.substring(posn, len));
    return buffer.toString();
  }
  
  public static void main(String[] args) throws Exception {
    for(String arg:args) {
      System.out.println("Original: " + arg);
      String quoted = quoteHtmlChars(arg);
      System.out.println("Quoted: "+ quoted);
      String unquoted = unquoteHtmlChars(quoted);
      System.out.println("Unquoted: " + unquoted);
      System.out.println();
    }
  }
}
