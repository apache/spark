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

package org.apache.hadoop.contrib.index.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.index.example.HashingDistributionPolicy;
import org.apache.hadoop.contrib.index.example.LineDocInputFormat;
import org.apache.hadoop.contrib.index.example.LineDocLocalAnalysis;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

/**
 * This class provides the getters and the setters to a number of parameters.
 * Most of the parameters are related to the index update and the rest are
 * from the existing Map/Reduce parameters.  
 */
public class IndexUpdateConfiguration {
  final Configuration conf;

  /**
   * Constructor
   * @param conf
   */
  public IndexUpdateConfiguration(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Get the underlying configuration object.
   * @return the configuration
   */
  public Configuration getConfiguration() {
    return conf;
  }

  //
  // existing map/reduce properties
  //
  // public int getIOFileBufferSize() {
  // return getInt("io.file.buffer.size", 4096);
  // }

  /**
   * Get the IO sort space in MB.
   * @return the IO sort space in MB
   */
  public int getIOSortMB() {
    return conf.getInt("io.sort.mb", 100);
  }

  /**
   * Set the IO sort space in MB.
   * @param mb  the IO sort space in MB
   */
  public void setIOSortMB(int mb) {
    conf.setInt("io.sort.mb", mb);
  }

  /**
   * Get the Map/Reduce temp directory.
   * @return the Map/Reduce temp directory
   */
  public String getMapredTempDir() {
    return conf.get("mapred.temp.dir");
  }

  //
  // properties for index update
  //
  /**
   * Get the distribution policy class.
   * @return the distribution policy class
   */
  public Class<? extends IDistributionPolicy> getDistributionPolicyClass() {
    return conf.getClass("sea.distribution.policy",
        HashingDistributionPolicy.class, IDistributionPolicy.class);
  }

  /**
   * Set the distribution policy class.
   * @param theClass  the distribution policy class
   */
  public void setDistributionPolicyClass(
      Class<? extends IDistributionPolicy> theClass) {
    conf.setClass("sea.distribution.policy", theClass,
        IDistributionPolicy.class);
  }

  /**
   * Get the analyzer class.
   * @return the analyzer class
   */
  public Class<? extends Analyzer> getDocumentAnalyzerClass() {
    return conf.getClass("sea.document.analyzer", StandardAnalyzer.class,
        Analyzer.class);
  }

  /**
   * Set the analyzer class.
   * @param theClass  the analyzer class
   */
  public void setDocumentAnalyzerClass(Class<? extends Analyzer> theClass) {
    conf.setClass("sea.document.analyzer", theClass, Analyzer.class);
  }

  /**
   * Get the index input format class.
   * @return the index input format class
   */
  public Class<? extends InputFormat> getIndexInputFormatClass() {
    return conf.getClass("sea.input.format", LineDocInputFormat.class,
        InputFormat.class);
  }

  /**
   * Set the index input format class.
   * @param theClass  the index input format class
   */
  public void setIndexInputFormatClass(Class<? extends InputFormat> theClass) {
    conf.setClass("sea.input.format", theClass, InputFormat.class);
  }

  /**
   * Get the index updater class.
   * @return the index updater class
   */
  public Class<? extends IIndexUpdater> getIndexUpdaterClass() {
    return conf.getClass("sea.index.updater", IndexUpdater.class,
        IIndexUpdater.class);
  }

  /**
   * Set the index updater class.
   * @param theClass  the index updater class
   */
  public void setIndexUpdaterClass(Class<? extends IIndexUpdater> theClass) {
    conf.setClass("sea.index.updater", theClass, IIndexUpdater.class);
  }

  /**
   * Get the local analysis class.
   * @return the local analysis class
   */
  public Class<? extends ILocalAnalysis> getLocalAnalysisClass() {
    return conf.getClass("sea.local.analysis", LineDocLocalAnalysis.class,
        ILocalAnalysis.class);
  }

  /**
   * Set the local analysis class.
   * @param theClass  the local analysis class
   */
  public void setLocalAnalysisClass(Class<? extends ILocalAnalysis> theClass) {
    conf.setClass("sea.local.analysis", theClass, ILocalAnalysis.class);
  }

  /**
   * Get the string representation of a number of shards.
   * @return the string representation of a number of shards
   */
  public String getIndexShards() {
    return conf.get("sea.index.shards");
  }

  /**
   * Set the string representation of a number of shards.
   * @param shards  the string representation of a number of shards
   */
  public void setIndexShards(String shards) {
    conf.set("sea.index.shards", shards);
  }

  /**
   * Get the max field length for a Lucene instance.
   * @return the max field length for a Lucene instance
   */
  public int getIndexMaxFieldLength() {
    return conf.getInt("sea.max.field.length", -1);
  }

  /**
   * Set the max field length for a Lucene instance.
   * @param maxFieldLength  the max field length for a Lucene instance
   */
  public void setIndexMaxFieldLength(int maxFieldLength) {
    conf.setInt("sea.max.field.length", maxFieldLength);
  }

  /**
   * Get the max number of segments for a Lucene instance.
   * @return the max number of segments for a Lucene instance
   */
  public int getIndexMaxNumSegments() {
    return conf.getInt("sea.max.num.segments", -1);
  }

  /**
   * Set the max number of segments for a Lucene instance.
   * @param maxNumSegments  the max number of segments for a Lucene instance
   */
  public void setIndexMaxNumSegments(int maxNumSegments) {
    conf.setInt("sea.max.num.segments", maxNumSegments);
  }

  /**
   * Check whether to use the compound file format for a Lucene instance.
   * @return true if using the compound file format for a Lucene instance
   */
  public boolean getIndexUseCompoundFile() {
    return conf.getBoolean("sea.use.compound.file", false);
  }

  /**
   * Set whether use the compound file format for a Lucene instance.
   * @param useCompoundFile  whether to use the compound file format
   */
  public void setIndexUseCompoundFile(boolean useCompoundFile) {
    conf.setBoolean("sea.use.compound.file", useCompoundFile);
  }

}
