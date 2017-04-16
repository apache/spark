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
 
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A class that allows a map/red job to work on a sample of sequence files.
 * The sample is decided by the filter class set by the job.
 * 
 */

public class SequenceFileInputFilter<K, V>
  extends SequenceFileInputFormat<K, V> {
  
  final private static String FILTER_CLASS = "sequencefile.filter.class";
  final private static String FILTER_FREQUENCY
    = "sequencefile.filter.frequency";
  final private static String FILTER_REGEX = "sequencefile.filter.regex";
    
  public SequenceFileInputFilter() {
  }
    
  /** Create a record reader for the given split
   * @param split file split
   * @param job job configuration
   * @param reporter reporter who sends report to task tracker
   * @return RecordReader
   */
  public RecordReader<K, V> getRecordReader(InputSplit split,
                                      JobConf job, Reporter reporter)
    throws IOException {
        
    reporter.setStatus(split.toString());
        
    return new FilterRecordReader<K, V>(job, (FileSplit) split);
  }


  /** set the filter class
   * 
   * @param conf application configuration
   * @param filterClass filter class
   */
  public static void setFilterClass(Configuration conf, Class filterClass) {
    conf.set(FILTER_CLASS, filterClass.getName());
  }

         
  /**
   * filter interface
   */
  public interface Filter extends Configurable {
    /** filter function
     * Decide if a record should be filtered or not
     * @param key record key
     * @return true if a record is accepted; return false otherwise
     */
    public abstract boolean accept(Object key);
  }
    
  /**
   * base class for Filters
   */
  public static abstract class FilterBase implements Filter {
    Configuration conf;
        
    public Configuration getConf() {
      return conf;
    }
  }
    
  /** Records filter by matching key to regex
   */
  public static class RegexFilter extends FilterBase {
    private Pattern p;
    /** Define the filtering regex and stores it in conf
     * @param conf where the regex is set
     * @param regex regex used as a filter
     */
    public static void setPattern(Configuration conf, String regex)
      throws PatternSyntaxException {
      try {
        Pattern.compile(regex);
      } catch (PatternSyntaxException e) {
        throw new IllegalArgumentException("Invalid pattern: "+regex);
      }
      conf.set(FILTER_REGEX, regex);
    }
        
    public RegexFilter() { }
        
    /** configure the Filter by checking the configuration
     */
    public void setConf(Configuration conf) {
      String regex = conf.get(FILTER_REGEX);
      if (regex==null)
        throw new RuntimeException(FILTER_REGEX + "not set");
      this.p = Pattern.compile(regex);
      this.conf = conf;
    }


    /** Filtering method
     * If key matches the regex, return true; otherwise return false
     * @see org.apache.hadoop.mapred.SequenceFileInputFilter.Filter#accept(Object)
     */
    public boolean accept(Object key) {
      return p.matcher(key.toString()).matches();
    }
  }

  /** This class returns a percentage of records
   * The percentage is determined by a filtering frequency <i>f</i> using
   * the criteria record# % f == 0.
   * For example, if the frequency is 10, one out of 10 records is returned.
   */
  public static class PercentFilter extends FilterBase {
    private int frequency;
    private int count;

    /** set the frequency and stores it in conf
     * @param conf configuration
     * @param frequency filtering frequencey
     */
    public static void setFrequency(Configuration conf, int frequency){
      if (frequency<=0)
        throw new IllegalArgumentException(
                                           "Negative " + FILTER_FREQUENCY + ": "+frequency);
      conf.setInt(FILTER_FREQUENCY, frequency);
    }
        
    public PercentFilter() { }
        
    /** configure the filter by checking the configuration
     * 
     * @param conf configuration
     */
    public void setConf(Configuration conf) {
      this.frequency = conf.getInt("sequencefile.filter.frequency", 10);
      if (this.frequency <=0) {
        throw new RuntimeException(
                                   "Negative "+FILTER_FREQUENCY+": "+this.frequency);
      }
      this.conf = conf;
    }

    /** Filtering method
     * If record# % frequency==0, return true; otherwise return false
     * @see org.apache.hadoop.mapred.SequenceFileInputFilter.Filter#accept(Object)
     */
    public boolean accept(Object key) {
      boolean accepted = false;
      if (count == 0)
        accepted = true;
      if (++count == frequency) {
        count = 0;
      }
      return accepted;
    }
  }

  /** This class returns a set of records by examing the MD5 digest of its
   * key against a filtering frequency <i>f</i>. The filtering criteria is
   * MD5(key) % f == 0.
   */
  public static class MD5Filter extends FilterBase {
    private int frequency;
    private static final MessageDigest DIGESTER;
    public static final int MD5_LEN = 16;
    private byte [] digest = new byte[MD5_LEN];
        
    static {
      try {
        DIGESTER = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    }


    /** set the filtering frequency in configuration
     * 
     * @param conf configuration
     * @param frequency filtering frequency
     */
    public static void setFrequency(Configuration conf, int frequency){
      if (frequency<=0)
        throw new IllegalArgumentException(
                                           "Negative " + FILTER_FREQUENCY + ": "+frequency);
      conf.setInt(FILTER_FREQUENCY, frequency);
    }
        
    public MD5Filter() { }
        
    /** configure the filter according to configuration
     * 
     * @param conf configuration
     */
    public void setConf(Configuration conf) {
      this.frequency = conf.getInt(FILTER_FREQUENCY, 10);
      if (this.frequency <=0) {
        throw new RuntimeException(
                                   "Negative "+FILTER_FREQUENCY+": "+this.frequency);
      }
      this.conf = conf;
    }

    /** Filtering method
     * If MD5(key) % frequency==0, return true; otherwise return false
     * @see org.apache.hadoop.mapred.SequenceFileInputFilter.Filter#accept(Object)
     */
    public boolean accept(Object key) {
      try {
        long hashcode;
        if (key instanceof Text) {
          hashcode = MD5Hashcode((Text)key);
        } else if (key instanceof BytesWritable) {
          hashcode = MD5Hashcode((BytesWritable)key);
        } else {
          ByteBuffer bb;
          bb = Text.encode(key.toString());
          hashcode = MD5Hashcode(bb.array(), 0, bb.limit());
        }
        if (hashcode/frequency*frequency==hashcode)
          return true;
      } catch(Exception e) {
        LOG.warn(e);
        throw new RuntimeException(e);
      }
      return false;
    }
        
    private long MD5Hashcode(Text key) throws DigestException {
      return MD5Hashcode(key.getBytes(), 0, key.getLength());
    }
        
    private long MD5Hashcode(BytesWritable key) throws DigestException {
      return MD5Hashcode(key.getBytes(), 0, key.getLength());
    }
    synchronized private long MD5Hashcode(byte[] bytes, 
                                          int start, int length) throws DigestException {
      DIGESTER.update(bytes, 0, length);
      DIGESTER.digest(digest, 0, MD5_LEN);
      long hashcode=0;
      for (int i = 0; i < 8; i++)
        hashcode |= ((digest[i] & 0xffL) << (8*(7-i)));
      return hashcode;
    }
  }
    
  private static class FilterRecordReader<K, V>
    extends SequenceFileRecordReader<K, V> {
    
    private Filter filter;
        
    public FilterRecordReader(Configuration conf, FileSplit split)
      throws IOException {
      super(conf, split);
      // instantiate filter
      filter = (Filter)ReflectionUtils.newInstance(
                                                   conf.getClass(FILTER_CLASS, PercentFilter.class), 
                                                   conf);
    }
        
    public synchronized boolean next(K key, V value) throws IOException {
      while (next(key)) {
        if (filter.accept(key)) {
          getCurrentValue(value);
          return true;
        }
      }
            
      return false;
    }
  }
}
