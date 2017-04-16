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

package org.apache.hadoop.streaming.io;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.typedbytes.TypedBytesWritable;

/**
 * This class is used to resolve a string identifier into the required IO
 * classes. By extending this class and pointing the property
 * <tt>stream.io.identifier.resolver.class</tt> to this extension, additional
 * IO classes can be added by external code.
 */
public class IdentifierResolver {

  // note that the identifiers are case insensitive
  public static final String TEXT_ID = "text";
  public static final String RAW_BYTES_ID = "rawbytes";
  public static final String TYPED_BYTES_ID = "typedbytes";
  
  private Class<? extends InputWriter> inputWriterClass = null;
  private Class<? extends OutputReader> outputReaderClass = null;
  private Class outputKeyClass = null;
  private Class outputValueClass = null;
  
  /**
   * Resolves a given identifier. This method has to be called before calling
   * any of the getters.
   */
  public void resolve(String identifier) {
    if (identifier.equalsIgnoreCase(RAW_BYTES_ID)) {
      setInputWriterClass(RawBytesInputWriter.class);
      setOutputReaderClass(RawBytesOutputReader.class);
      setOutputKeyClass(BytesWritable.class);
      setOutputValueClass(BytesWritable.class);
    } else if (identifier.equalsIgnoreCase(TYPED_BYTES_ID)) {
      setInputWriterClass(TypedBytesInputWriter.class);
      setOutputReaderClass(TypedBytesOutputReader.class);
      setOutputKeyClass(TypedBytesWritable.class);
      setOutputValueClass(TypedBytesWritable.class);
    } else { // assume TEXT_ID
      setInputWriterClass(TextInputWriter.class);
      setOutputReaderClass(TextOutputReader.class);
      setOutputKeyClass(Text.class);
      setOutputValueClass(Text.class);
    }
  }
  
  /**
   * Returns the resolved {@link InputWriter} class.
   */
  public Class<? extends InputWriter> getInputWriterClass() {
    return inputWriterClass;
  }

  /**
   * Returns the resolved {@link OutputReader} class.
   */
  public Class<? extends OutputReader> getOutputReaderClass() {
    return outputReaderClass;
  }
  
  /**
   * Returns the resolved output key class.
   */
  public Class getOutputKeyClass() {
    return outputKeyClass;
  }

  /**
   * Returns the resolved output value class.
   */
  public Class getOutputValueClass() {
    return outputValueClass;
  }
  
  
  /**
   * Sets the {@link InputWriter} class.
   */
  protected void setInputWriterClass(Class<? extends InputWriter>
    inputWriterClass) {
    this.inputWriterClass = inputWriterClass;
  }
  
  /**
   * Sets the {@link OutputReader} class.
   */
  protected void setOutputReaderClass(Class<? extends OutputReader>
    outputReaderClass) {
    this.outputReaderClass = outputReaderClass;
  }
  
  /**
   * Sets the output key class class.
   */
  protected void setOutputKeyClass(Class outputKeyClass) {
    this.outputKeyClass = outputKeyClass;
  }
  
  /**
   * Sets the output value class.
   */
  protected void setOutputValueClass(Class outputValueClass) {
    this.outputValueClass = outputValueClass;
  }

}
