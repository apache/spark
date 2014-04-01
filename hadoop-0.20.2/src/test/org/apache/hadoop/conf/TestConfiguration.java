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
package org.apache.hadoop.conf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import junit.framework.TestCase;
import static org.junit.Assert.assertArrayEquals;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;


public class TestConfiguration extends TestCase {

  private Configuration conf;
  final static String CONFIG = new File("./test-config.xml").getAbsolutePath();
  final static String CONFIG2 = new File("./test-config2.xml").getAbsolutePath();
  final static Random RAN = new Random();

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    conf = new Configuration();
  }
  
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    new File(CONFIG).delete();
    new File(CONFIG2).delete();
  }
  
  private void startConfig() throws IOException{
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
  }

  private void endConfig() throws IOException{
    out.write("</configuration>\n");
    out.close();
  }

  private void addInclude(String filename) throws IOException{
    out.write("<xi:include href=\"" + filename + "\" xmlns:xi=\"http://www.w3.org/2001/XInclude\"  />\n ");
  }

  public void testVariableSubstitution() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    declareProperty("my.int", "${intvar}", "42");
    declareProperty("intvar", "42", "42");
    declareProperty("my.base", "/tmp/${user.name}", UNSPEC);
    declareProperty("my.file", "hello", "hello");
    declareProperty("my.suffix", ".txt", ".txt");
    declareProperty("my.relfile", "${my.file}${my.suffix}", "hello.txt");
    declareProperty("my.fullfile", "${my.base}/${my.file}${my.suffix}", UNSPEC);
    // check that undefined variables are returned as-is
    declareProperty("my.failsexpand", "a${my.undefvar}b", "a${my.undefvar}b");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);

    for (Prop p : props) {
      System.out.println("p=" + p.name);
      String gotVal = conf.get(p.name);
      String gotRawVal = conf.getRaw(p.name);
      assertEq(p.val, gotRawVal);
      if (p.expectEval == UNSPEC) {
        // expansion is system-dependent (uses System properties)
        // can't do exact match so just check that all variables got expanded
        assertTrue(gotVal != null && -1 == gotVal.indexOf("${"));
      } else {
        assertEq(p.expectEval, gotVal);
      }
    }
      
    // check that expansion also occurs for getInt()
    assertTrue(conf.getInt("intvar", -1) == 42);
    assertTrue(conf.getInt("my.int", -1) == 42);
  }

  public void testFinalParam() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    declareProperty("my.var", "", "", true);
    endConfig();
    Path fileResource = new Path(CONFIG);
    Configuration conf1 = new Configuration();
    conf1.addResource(fileResource);
    assertNull("my var is not null", conf1.get("my.var"));
	
    out=new BufferedWriter(new FileWriter(CONFIG2));
    startConfig();
    declareProperty("my.var", "myval", "myval", false);
    endConfig();
    fileResource = new Path(CONFIG2);

    Configuration conf2 = new Configuration(conf1);
    conf2.addResource(fileResource);
    assertNull("my var is not final", conf2.get("my.var"));
  }

  public static void assertEq(Object a, Object b) {
    System.out.println("assertEq: " + a + ", " + b);
    assertEquals(a, b);
  }

  static class Prop {
    String name;
    String val;
    String expectEval;
  }

  final String UNSPEC = null;
  ArrayList<Prop> props = new ArrayList<Prop>();

  void declareProperty(String name, String val, String expectEval)
    throws IOException {
    declareProperty(name, val, expectEval, false);
  }

  void declareProperty(String name, String val, String expectEval,
                       boolean isFinal)
    throws IOException {
    appendProperty(name, val, isFinal);
    Prop p = new Prop();
    p.name = name;
    p.val = val;
    p.expectEval = expectEval;
    props.add(p);
  }

  void appendProperty(String name, String val) throws IOException {
    appendProperty(name, val, false);
  }
 
  void appendProperty(String name, String val, boolean isFinal)
    throws IOException {
    out.write("<property>");
    out.write("<name>");
    out.write(name);
    out.write("</name>");
    out.write("<value>");
    out.write(val);
    out.write("</value>");
    if (isFinal) {
      out.write("<final>true</final>");
    }
    out.write("</property>\n");
  }
  
  public void testOverlay() throws IOException{
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("a","b");
    appendProperty("b","c");
    appendProperty("d","e");
    appendProperty("e","f", true);
    endConfig();

    out=new BufferedWriter(new FileWriter(CONFIG2));
    startConfig();
    appendProperty("a","b");
    appendProperty("b","d");
    appendProperty("e","e");
    endConfig();
    
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    
    //set dynamically something
    conf.set("c","d");
    conf.set("a","d");
    
    Configuration clone=new Configuration(conf);
    clone.addResource(new Path(CONFIG2));
    
    assertEquals(clone.get("a"), "d"); 
    assertEquals(clone.get("b"), "d"); 
    assertEquals(clone.get("c"), "d"); 
    assertEquals(clone.get("d"), "e"); 
    assertEquals(clone.get("e"), "f"); 
    
  }
  
  public void testCommentsInValue() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("my.comment", "this <!--comment here--> contains a comment");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    //two spaces one after "this", one before "contains"
    assertEquals("this  contains a comment", conf.get("my.comment"));
  }
  
  public void testTrim() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    String[] whitespaces = {"", " ", "\n", "\t"};
    String[] name = new String[100];
    for(int i = 0; i < name.length; i++) {
      name[i] = "foo" + i;
      StringBuilder prefix = new StringBuilder(); 
      StringBuilder postfix = new StringBuilder(); 
      for(int j = 0; j < 3; j++) {
        prefix.append(whitespaces[RAN.nextInt(whitespaces.length)]);
        postfix.append(whitespaces[RAN.nextInt(whitespaces.length)]);
      }
      
      appendProperty(prefix + name[i] + postfix, name[i] + ".value");
    }
    endConfig();

    conf.addResource(new Path(CONFIG));
    for(String n : name) {
      assertEquals(n + ".value", conf.get(n));
    }
  }

  public void testGetLocalPath() throws IOException {
    Configuration conf = new Configuration();
    String[] dirs = new String[]{"a", "b", "c"};
    for (int i = 0; i < dirs.length; i++) {
      dirs[i] = new Path(System.getProperty("test.build.data"), dirs[i])
          .toString();
    }
    conf.set("dirs", StringUtils.join(",", Arrays.<String>asList(dirs)));
    for (int i = 0; i < 1000; i++) {
      String localPath = conf.getLocalPath("dirs", "dir" + i).toString();
      assertTrue("Path doesn't end in specified dir: " + localPath,
        localPath.endsWith("dir" + i));
      assertFalse("Path has internal whitespace: " + localPath,
        localPath.contains(" "));
    }
  }

  public void testGetFile() throws IOException {
    Configuration conf = new Configuration();
    String[] dirs = new String[]{"a", "b", "c"};
    for (int i = 0; i < dirs.length; i++) {
      dirs[i] = new Path(System.getProperty("test.build.data"), dirs[i])
          .toString();
    }
    conf.set("dirs", StringUtils.join(",", Arrays.<String>asList(dirs)));
    for (int i = 0; i < 1000; i++) {
      String localPath = conf.getFile("dirs", "dir" + i).toString();
      assertTrue("Path doesn't end in specified dir: " + localPath,
        localPath.endsWith("dir" + i));
      assertFalse("Path has internal whitespace: " + localPath,
        localPath.contains(" "));
    }
  }

  public void testToString() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    
    String expectedOutput = 
      "Configuration: core-default.xml, core-site.xml," +
      " mapred-default.xml, mapred-site.xml, " + 
      fileResource.toString();
    assertEquals(expectedOutput, conf.toString());
  }

  public void testWriteXml() throws IOException {
    Configuration conf = new Configuration();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(); 
    conf.writeXml(baos);
    String result = baos.toString();
    assertTrue("Result has proper header", result.startsWith(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><configuration>"));
    assertTrue("Result has proper footer", result.endsWith("</configuration>"));
  }

  public void testIncludes() throws Exception {
    tearDown();
    System.out.println("XXX testIncludes");
    out=new BufferedWriter(new FileWriter(CONFIG2));
    startConfig();
    appendProperty("a","b");
    appendProperty("c","d");
    endConfig();

    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    addInclude(CONFIG2);
    appendProperty("e","f");
    appendProperty("g","h");
    endConfig();

    // verify that the includes file contains all properties
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    assertEquals(conf.get("a"), "b"); 
    assertEquals(conf.get("c"), "d"); 
    assertEquals(conf.get("e"), "f"); 
    assertEquals(conf.get("g"), "h"); 
    tearDown();
  }

  BufferedWriter out;
	
  public void testIntegerRanges() {
    Configuration conf = new Configuration();
    conf.set("first", "-100");
    conf.set("second", "4-6,9-10,27");
    conf.set("third", "34-");
    Configuration.IntegerRanges range = conf.getRange("first", null);
    System.out.println("first = " + range);
    assertEquals(true, range.isIncluded(0));
    assertEquals(true, range.isIncluded(1));
    assertEquals(true, range.isIncluded(100));
    assertEquals(false, range.isIncluded(101));
    range = conf.getRange("second", null);
    System.out.println("second = " + range);
    assertEquals(false, range.isIncluded(3));
    assertEquals(true, range.isIncluded(4));
    assertEquals(true, range.isIncluded(6));
    assertEquals(false, range.isIncluded(7));
    assertEquals(false, range.isIncluded(8));
    assertEquals(true, range.isIncluded(9));
    assertEquals(true, range.isIncluded(10));
    assertEquals(false, range.isIncluded(11));
    assertEquals(false, range.isIncluded(26));
    assertEquals(true, range.isIncluded(27));
    assertEquals(false, range.isIncluded(28));
    range = conf.getRange("third", null);
    System.out.println("third = " + range);
    assertEquals(false, range.isIncluded(33));
    assertEquals(true, range.isIncluded(34));
    assertEquals(true, range.isIncluded(100000000));
  }

  public void testHexValues() throws IOException{
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.hex1", "0x10");
    appendProperty("test.hex2", "0xF");
    appendProperty("test.hex3", "-0x10");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    assertEquals(16, conf.getInt("test.hex1", 0));
    assertEquals(16, conf.getLong("test.hex1", 0));
    assertEquals(15, conf.getInt("test.hex2", 0));
    assertEquals(15, conf.getLong("test.hex2", 0));
    assertEquals(-16, conf.getInt("test.hex3", 0));
    assertEquals(-16, conf.getLong("test.hex3", 0));

  }

  public void testIntegerValues() throws IOException{
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.int1", "20");
    appendProperty("test.int2", "020");
    appendProperty("test.int3", "-20");
    appendProperty("test.int4", " -20 ");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    assertEquals(20, conf.getInt("test.int1", 0));
    assertEquals(20, conf.getLong("test.int1", 0));
    assertEquals(20, conf.getInt("test.int2", 0));
    assertEquals(20, conf.getLong("test.int2", 0));
    assertEquals(-20, conf.getInt("test.int3", 0));
    assertEquals(-20, conf.getLong("test.int3", 0));
    assertEquals(-20, conf.getInt("test.int4", 0));
    assertEquals(-20, conf.getLong("test.int4", 0));
  }

  public void testBooleanValues() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.bool1", "true");
    appendProperty("test.bool2", "false");
    appendProperty("test.bool3", "  true ");
    appendProperty("test.bool4", " false ");
    appendProperty("test.bool5", "foo");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    assertEquals(true, conf.getBoolean("test.bool1", false));
    assertEquals(false, conf.getBoolean("test.bool2", true));
    assertEquals(true, conf.getBoolean("test.bool3", false));
    assertEquals(false, conf.getBoolean("test.bool4", true));
    assertEquals(true, conf.getBoolean("test.bool5", true));
  }
  
  public void testFloatValues() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.float1", "3.1415");
    appendProperty("test.float2", "003.1415");
    appendProperty("test.float3", "-3.1415");
    appendProperty("test.float4", " -3.1415 ");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    assertEquals(3.1415f, conf.getFloat("test.float1", 0.0f));
    assertEquals(3.1415f, conf.getFloat("test.float2", 0.0f));
    assertEquals(-3.1415f, conf.getFloat("test.float3", 0.0f));
    assertEquals(-3.1415f, conf.getFloat("test.float4", 0.0f));
  }
  
  public void testGetClass() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.class1", "java.lang.Integer");
    appendProperty("test.class2", " java.lang.Integer ");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    assertEquals("java.lang.Integer", conf.getClass("test.class1", null).getCanonicalName());
    assertEquals("java.lang.Integer", conf.getClass("test.class2", null).getCanonicalName());
  }
  
  public void testGetClasses() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.classes1", "java.lang.Integer,java.lang.String");
    appendProperty("test.classes2", " java.lang.Integer , java.lang.String ");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    String[] expectedNames = {"java.lang.Integer", "java.lang.String"};
    Class<?>[] defaultClasses = {};
    Class<?>[] classes1 = conf.getClasses("test.classes1", defaultClasses);
    Class<?>[] classes2 = conf.getClasses("test.classes2", defaultClasses);
    assertArrayEquals(expectedNames, extractClassNames(classes1));
    assertArrayEquals(expectedNames, extractClassNames(classes2));
  }
  
  private static String[] extractClassNames(Class<?>[] classes) {
    String[] classNames = new String[classes.length];
    for (int i = 0; i < classNames.length; i++) {
      classNames[i] = classes[i].getCanonicalName();
    }
    return classNames;
  }

  public void testPattern() throws IOException {
    out = new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.pattern1", "");
    appendProperty("test.pattern2", "(");
    appendProperty("test.pattern3", "a+b");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);

    Pattern defaultPattern = Pattern.compile("x+");
    // Return default if missing
    assertEquals(defaultPattern.pattern(),
                 conf.getPattern("xxxxx", defaultPattern).pattern());
    // Return null if empty and default is null
    assertNull(conf.getPattern("test.pattern1", null));
    // Return default for empty
    assertEquals(defaultPattern.pattern(),
                 conf.getPattern("test.pattern1", defaultPattern).pattern());
    // Return default for malformed
    assertEquals(defaultPattern.pattern(),
                 conf.getPattern("test.pattern2", defaultPattern).pattern());
    // Works for correct patterns
    assertEquals("a+b",
                 conf.getPattern("test.pattern3", defaultPattern).pattern());
  }

  enum Dingo { FOO, BAR };
  enum Yak { RAB, FOO };
  public void testEnum() throws IOException {
    Configuration conf = new Configuration();
    conf.setEnum("test.enum", Dingo.FOO);
    assertSame(Dingo.FOO, conf.getEnum("test.enum", Dingo.BAR));
    assertSame(Yak.FOO, conf.getEnum("test.enum", Yak.RAB));
    boolean fail = false;
    try {
      conf.setEnum("test.enum", Dingo.BAR);
      Yak y = conf.getEnum("test.enum", Yak.FOO);
    } catch (IllegalArgumentException e) {
      fail = true;
    }
    assertTrue(fail);
  }

  public void testReload() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.key1", "final-value1", true);
    appendProperty("test.key2", "value2");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    
    out=new BufferedWriter(new FileWriter(CONFIG2));
    startConfig();
    appendProperty("test.key1", "value1");
    appendProperty("test.key3", "value3");
    endConfig();
    Path fileResource1 = new Path(CONFIG2);
    conf.addResource(fileResource1);
    
    // add a few values via set.
    conf.set("test.key3", "value4");
    conf.set("test.key4", "value5");
    
    assertEquals("final-value1", conf.get("test.key1"));
    assertEquals("value2", conf.get("test.key2"));
    assertEquals("value4", conf.get("test.key3"));
    assertEquals("value5", conf.get("test.key4"));
    
    // change values in the test file...
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.key1", "final-value1");
    appendProperty("test.key3", "final-value3", true);
    endConfig();
    
    conf.reloadConfiguration();
    assertEquals("value1", conf.get("test.key1"));
    // overlayed property overrides.
    assertEquals("value4", conf.get("test.key3"));
    assertEquals(null, conf.get("test.key2"));
    assertEquals("value5", conf.get("test.key4"));
  }

  public void testSize() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set("a", "A");
    conf.set("b", "B");
    assertEquals(2, conf.size());
  }

  public void testClear() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set("a", "A");
    conf.set("b", "B");
    conf.clear();
    assertEquals(0, conf.size());
    assertFalse(conf.iterator().hasNext());
  }

  public static void main(String[] argv) throws Exception {
    junit.textui.TestRunner.main(new String[]{
      TestConfiguration.class.getName()
    });
  }

  static class JsonConfiguration {
    JsonProperty[] properties;

    public JsonProperty[] getProperties() {
      return properties;
    }

    public void setProperties(JsonProperty[] properties) {
      this.properties = properties;
    }
  }
  
  static class JsonProperty {
    String key;
    public String getKey() {
      return key;
    }
    public void setKey(String key) {
      this.key = key;
    }
    public String getValue() {
      return value;
    }
    public void setValue(String value) {
      this.value = value;
    }
    public boolean getIsFinal() {
      return isFinal;
    }
    public void setIsFinal(boolean isFinal) {
      this.isFinal = isFinal;
    }
    public String getResource() {
      return resource;
    }
    public void setResource(String resource) {
      this.resource = resource;
    }
    String value;
    boolean isFinal;
    String resource;
  }
  
  public void testDumpConfiguration () throws IOException {
    StringWriter outWriter = new StringWriter();
    Configuration.dumpConfiguration(conf, outWriter);
    String jsonStr = outWriter.toString();
    ObjectMapper mapper = new ObjectMapper();
    JsonConfiguration jconf = 
      mapper.readValue(jsonStr, JsonConfiguration.class);
    int defaultLength = jconf.getProperties().length;
    
    // add 3 keys to the existing configuration properties
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.key1", "value1");
    appendProperty("test.key2", "value2",true);
    appendProperty("test.key3", "value3");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    out.close();
    
    outWriter = new StringWriter();
    Configuration.dumpConfiguration(conf, outWriter);
    jsonStr = outWriter.toString();
    mapper = new ObjectMapper();
    jconf = mapper.readValue(jsonStr, JsonConfiguration.class);
    int length = jconf.getProperties().length;
    // check for consistency in the number of properties parsed in Json format.
    assertEquals(length, defaultLength+3);
    
    //change few keys in another resource file
    out=new BufferedWriter(new FileWriter(CONFIG2));
    startConfig();
    appendProperty("test.key1", "newValue1");
    appendProperty("test.key2", "newValue2");
    endConfig();
    Path fileResource1 = new Path(CONFIG2);
    conf.addResource(fileResource1);
    out.close();
    
    outWriter = new StringWriter();
    Configuration.dumpConfiguration(conf, outWriter);
    jsonStr = outWriter.toString();
    mapper = new ObjectMapper();
    jconf = mapper.readValue(jsonStr, JsonConfiguration.class);
    
    // put the keys and their corresponding attributes into a hashmap for their 
    // efficient retrieval
    HashMap<String,JsonProperty> confDump = new HashMap<String,JsonProperty>();
    for(JsonProperty prop : jconf.getProperties()) {
      confDump.put(prop.getKey(), prop);
    }
    // check if the value and resource of test.key1 is changed
    assertEquals("newValue1", confDump.get("test.key1").getValue());
    assertEquals(false, confDump.get("test.key1").getIsFinal());
    assertEquals(fileResource1.toString(),
        confDump.get("test.key1").getResource());
    // check if final parameter test.key2 is not changed, since it is first 
    // loaded as final parameter
    assertEquals("value2", confDump.get("test.key2").getValue());
    assertEquals(true, confDump.get("test.key2").getIsFinal());
    assertEquals(fileResource.toString(),
        confDump.get("test.key2").getResource());
    // check for other keys which are not modified later
    assertEquals("value3", confDump.get("test.key3").getValue());
    assertEquals(false, confDump.get("test.key3").getIsFinal());
    assertEquals(fileResource.toString(),
        confDump.get("test.key3").getResource());
    // check for resource to be "Unknown" for keys which are loaded using 'set' 
    // and expansion of properties
    conf.set("test.key4", "value4");
    conf.set("test.key5", "value5");
    conf.set("test.key6", "${test.key5}");
    outWriter = new StringWriter();
    Configuration.dumpConfiguration(conf, outWriter);
    jsonStr = outWriter.toString();
    mapper = new ObjectMapper();
    jconf = mapper.readValue(jsonStr, JsonConfiguration.class);
    confDump = new HashMap<String, JsonProperty>();
    for(JsonProperty prop : jconf.getProperties()) {
      confDump.put(prop.getKey(), prop);
    }
    assertEquals("value5",confDump.get("test.key6").getValue());
    assertEquals("Unknown", confDump.get("test.key4").getResource());
    outWriter.close();
  }
  
  public void testDumpConfiguratioWithoutDefaults() throws IOException {
    // check for case when default resources are not loaded
    Configuration config = new Configuration(false);
    StringWriter outWriter = new StringWriter();
    Configuration.dumpConfiguration(config, outWriter);
    String jsonStr = outWriter.toString();
    ObjectMapper mapper = new ObjectMapper();
    JsonConfiguration jconf = 
      mapper.readValue(jsonStr, JsonConfiguration.class);
    
    //ensure that no properties are loaded.
    assertEquals(0, jconf.getProperties().length);
    
    // add 2 keys
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.key1", "value1");
    appendProperty("test.key2", "value2",true);
    endConfig();
    Path fileResource = new Path(CONFIG);
    config.addResource(fileResource);
    out.close();
    
    outWriter = new StringWriter();
    Configuration.dumpConfiguration(config, outWriter);
    jsonStr = outWriter.toString();
    mapper = new ObjectMapper();
    jconf = mapper.readValue(jsonStr, JsonConfiguration.class);
    
    HashMap<String, JsonProperty>confDump = new HashMap<String, JsonProperty>();
    for (JsonProperty prop : jconf.getProperties()) {
      confDump.put(prop.getKey(), prop);
    }
    //ensure only 2 keys are loaded
    assertEquals(2,jconf.getProperties().length);
    //ensure the values are consistent
    assertEquals(confDump.get("test.key1").getValue(),"value1");
    assertEquals(confDump.get("test.key2").getValue(),"value2");
    //check the final tag
    assertEquals(false, confDump.get("test.key1").getIsFinal());
    assertEquals(true, confDump.get("test.key2").getIsFinal());
    //check the resource for each property
    for (JsonProperty prop : jconf.getProperties()) {
      assertEquals(fileResource.toString(),prop.getResource());
    }
  }
  
  public void testGetValByRegex() {
    Configuration conf = new Configuration();
    String key1 = "t.abc.key1";
    String key2 = "t.abc.key2";
    String key3 = "tt.abc.key3";
    String key4 = "t.abc.ey3";
    conf.set(key1, "value1");
    conf.set(key2, "value2");
    conf.set(key3, "value3");
    conf.set(key4, "value3");
    
    Map<String,String> res = conf.getValByRegex("^t\\..*\\.key\\d");
    assertTrue("Conf didn't get key " + key1, res.containsKey(key1));
    assertTrue("Conf didn't get key " + key2, res.containsKey(key2));
    assertTrue("Picked out wrong key " + key3, !res.containsKey(key3));
    assertTrue("Picked out wrong key " + key4, !res.containsKey(key4));
  }

  public void testUnset() {
    Configuration conf = new Configuration();
    conf.set("foo", "bar");
    assertNotNull(conf.get("foo"));
    conf.unset("foo");
    assertNull(conf.get("foo"));
  }

}

