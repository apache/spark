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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class TestCodecFactory extends TestCase {

  private static class BaseCodec implements CompressionCodec {
    private Configuration conf;
    
    public void setConf(Configuration conf) {
      this.conf = conf;
    }
    
    public Configuration getConf() {
      return conf;
    }
    
    public CompressionOutputStream createOutputStream(OutputStream out) 
    throws IOException {
      return null;
    }
    
    public Class<? extends Compressor> getCompressorType() {
      return null;
    }

    public Compressor createCompressor() {
      return null;
    }

    public CompressionInputStream createInputStream(InputStream in, 
                                                    Decompressor decompressor) 
    throws IOException {
      return null;
    }

    public CompressionInputStream createInputStream(InputStream in) 
    throws IOException {
      return null;
    }

    public CompressionOutputStream createOutputStream(OutputStream out, 
                                                      Compressor compressor) 
    throws IOException {
      return null;
    }

    public Class<? extends Decompressor> getDecompressorType() {
      return null;
    }

    public Decompressor createDecompressor() {
      return null;
    }

    public String getDefaultExtension() {
      return ".base";
    }
  }
  
  private static class BarCodec extends BaseCodec {
    public String getDefaultExtension() {
      return "bar";
    }
  }
  
  private static class FooBarCodec extends BaseCodec {
    public String getDefaultExtension() {
      return ".foo.bar";
    }
  }
  
  private static class FooCodec extends BaseCodec {
    public String getDefaultExtension() {
      return ".foo";
    }
  }
  
  /**
   * Returns a factory for a given set of codecs
   * @param classes the codec classes to include
   * @return a new factory
   */
  private static CompressionCodecFactory setClasses(Class[] classes) {
    Configuration conf = new Configuration();
    CompressionCodecFactory.setCodecClasses(conf, Arrays.asList(classes));
    return new CompressionCodecFactory(conf);
  }
  
  private static void checkCodec(String msg, 
                                 Class expected, CompressionCodec actual) {
    assertEquals(msg + " unexpected codec found",
                 expected.getName(),
                 actual.getClass().getName());
  }
  
  public static void testFinding() {
    CompressionCodecFactory factory = 
      new CompressionCodecFactory(new Configuration());
    CompressionCodec codec = factory.getCodec(new Path("/tmp/foo.bar"));
    assertEquals("default factory foo codec", null, codec);
    codec = factory.getCodecByClassName(BarCodec.class.getCanonicalName());
    assertEquals("default factory foo codec", null, codec);
    
    codec = factory.getCodec(new Path("/tmp/foo.gz"));
    checkCodec("default factory for .gz", GzipCodec.class, codec);
    codec = factory.getCodecByClassName(GzipCodec.class.getCanonicalName());
    checkCodec("default factory for gzip codec", GzipCodec.class, codec);
    codec = factory.getCodecByName("gzip");
    checkCodec("default factory for gzip codec", GzipCodec.class, codec);
    codec = factory.getCodecByName("GZIP");
    checkCodec("default factory for gzip codec", GzipCodec.class, codec);
    codec = factory.getCodecByName("GZIPCodec");
    checkCodec("default factory for gzip codec", GzipCodec.class, codec);
    codec = factory.getCodecByName("gzipcodec");
    checkCodec("default factory for gzip codec", GzipCodec.class, codec);
    Class klass = factory.getCodecClassByName("gzipcodec");
    assertEquals(GzipCodec.class, klass);

    codec = factory.getCodec(new Path("/tmp/foo.bz2"));
    checkCodec("default factory for .bz2", BZip2Codec.class, codec);
    codec = factory.getCodecByClassName(BZip2Codec.class.getCanonicalName());
    checkCodec("default factory for bzip2 codec", BZip2Codec.class, codec);
    codec = factory.getCodecByName("bzip2");
    checkCodec("default factory for bzip2 codec", BZip2Codec.class, codec);
    codec = factory.getCodecByName("bzip2codec");
    checkCodec("default factory for bzip2 codec", BZip2Codec.class, codec);
    codec = factory.getCodecByName("BZIP2");
    checkCodec("default factory for bzip2 codec", BZip2Codec.class, codec);
    codec = factory.getCodecByName("BZIP2CODEC");
    checkCodec("default factory for bzip2 codec", BZip2Codec.class, codec);

    codec = factory.getCodecByClassName(DeflateCodec.class.getCanonicalName());
    checkCodec("default factory for deflate codec", DeflateCodec.class, codec);
    codec = factory.getCodecByName("deflate");
    checkCodec("default factory for deflate codec", DeflateCodec.class, codec);
    codec = factory.getCodecByName("deflatecodec");
    checkCodec("default factory for deflate codec", DeflateCodec.class, codec);
    codec = factory.getCodecByName("DEFLATE");
    checkCodec("default factory for deflate codec", DeflateCodec.class, codec);
    codec = factory.getCodecByName("DEFLATECODEC");
    checkCodec("default factory for deflate codec", DeflateCodec.class, codec);

    factory = setClasses(new Class[0]);
    codec = factory.getCodec(new Path("/tmp/foo.bar"));
    assertEquals("empty codec bar codec", null, codec);
    codec = factory.getCodecByClassName(BarCodec.class.getCanonicalName());
    assertEquals("empty codec bar codec", null, codec);
    
    codec = factory.getCodec(new Path("/tmp/foo.gz"));
    assertEquals("empty codec gz codec", null, codec);
    codec = factory.getCodecByClassName(GzipCodec.class.getCanonicalName());
    assertEquals("empty codec gz codec", null, codec);
    
    codec = factory.getCodec(new Path("/tmp/foo.bz2"));
    assertEquals("empty factory for .bz2", null, codec);
    codec = factory.getCodecByClassName(BZip2Codec.class.getCanonicalName());
    assertEquals("empty factory for bzip2 codec", null, codec);
    
    factory = setClasses(new Class[]{BarCodec.class, FooCodec.class, 
                                     FooBarCodec.class});
    codec = factory.getCodec(new Path("/tmp/.foo.bar.gz"));
    assertEquals("full factory gz codec", null, codec);
    codec = factory.getCodecByClassName(GzipCodec.class.getCanonicalName());
    assertEquals("full codec gz codec", null, codec);
     
    codec = factory.getCodec(new Path("/tmp/foo.bz2"));
    assertEquals("full factory for .bz2", null, codec);
    codec = factory.getCodecByClassName(BZip2Codec.class.getCanonicalName());
    assertEquals("full codec bzip2 codec", null, codec);

    codec = factory.getCodec(new Path("/tmp/foo.bar"));
    checkCodec("full factory bar codec", BarCodec.class, codec);
    codec = factory.getCodecByClassName(BarCodec.class.getCanonicalName());
    checkCodec("full factory bar codec", BarCodec.class, codec);
    codec = factory.getCodecByName("bar");
    checkCodec("full factory bar codec", BarCodec.class, codec);
    codec = factory.getCodecByName("BAR");
    checkCodec("full factory bar codec", BarCodec.class, codec);

    codec = factory.getCodec(new Path("/tmp/foo/baz.foo.bar"));
    checkCodec("full factory foo bar codec", FooBarCodec.class, codec);
    codec = factory.getCodecByClassName(FooBarCodec.class.getCanonicalName());
    checkCodec("full factory foo bar codec", FooBarCodec.class, codec);
    codec = factory.getCodecByName("foobar");
    checkCodec("full factory foo bar codec", FooBarCodec.class, codec);
    codec = factory.getCodecByName("FOOBAR");
    checkCodec("full factory foo bar codec", FooBarCodec.class, codec);

    codec = factory.getCodec(new Path("/tmp/foo.foo"));
    checkCodec("full factory foo codec", FooCodec.class, codec);
    codec = factory.getCodecByClassName(FooCodec.class.getCanonicalName());
    checkCodec("full factory foo codec", FooCodec.class, codec);
    codec = factory.getCodecByName("foo");
    checkCodec("full factory foo codec", FooCodec.class, codec);
    codec = factory.getCodecByName("FOO");
    checkCodec("full factory foo codec", FooCodec.class, codec);
  }
}
