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

package org.apache.hadoop.record.compiler;

import java.util.Map;


/**
 */
public class JMap extends JCompType {
  
  static private int level = 0;
  
  static private String getLevel() { return Integer.toString(level); }
  
  static private void incrLevel() { level++; }
  
  static private void decrLevel() { level--; }
  
  static private String getId(String id) { return id+getLevel(); }
  
  private JType keyType;
  private JType valueType;
  
  class JavaMap extends JavaCompType {
    
    JType.JavaType key;
    JType.JavaType value;
    
    JavaMap(JType.JavaType key, JType.JavaType value) {
      super("java.util.TreeMap<"+key.getWrapperType()+","+value.getWrapperType()+">",
            "Map",
            "java.util.TreeMap<"+key.getWrapperType()+","+value.getWrapperType()+">",
            "TypeID.RIOType.MAP");
      this.key = key;
      this.value = value;
    }
    
    String getTypeIDObjectString() {
      return "new org.apache.hadoop.record.meta.MapTypeID(" + 
        key.getTypeIDObjectString() + ", " + 
        value.getTypeIDObjectString() + ")";
    }

    void genSetRTIFilter(CodeBuffer cb, Map<String, Integer> nestedStructMap) {
      key.genSetRTIFilter(cb, nestedStructMap);
      value.genSetRTIFilter(cb, nestedStructMap);
    }

    void genCompareTo(CodeBuffer cb, String fname, String other) {
      String setType = "java.util.Set<"+key.getWrapperType()+"> ";
      String iterType = "java.util.Iterator<"+key.getWrapperType()+"> ";
      cb.append("{\n");
      cb.append(setType+getId(Consts.RIO_PREFIX + "set1")+" = "+
          fname+".keySet();\n");
      cb.append(setType+getId(Consts.RIO_PREFIX + "set2")+" = "+
          other+".keySet();\n");
      cb.append(iterType+getId(Consts.RIO_PREFIX + "miter1")+" = "+
                getId(Consts.RIO_PREFIX + "set1")+".iterator();\n");
      cb.append(iterType+getId(Consts.RIO_PREFIX + "miter2")+" = "+
                getId(Consts.RIO_PREFIX + "set2")+".iterator();\n");
      cb.append("for(; "+getId(Consts.RIO_PREFIX + "miter1")+".hasNext() && "+
                getId(Consts.RIO_PREFIX + "miter2")+".hasNext();) {\n");
      cb.append(key.getType()+" "+getId(Consts.RIO_PREFIX + "k1")+
                " = "+getId(Consts.RIO_PREFIX + "miter1")+".next();\n");
      cb.append(key.getType()+" "+getId(Consts.RIO_PREFIX + "k2")+
                " = "+getId(Consts.RIO_PREFIX + "miter2")+".next();\n");
      key.genCompareTo(cb, getId(Consts.RIO_PREFIX + "k1"), 
          getId(Consts.RIO_PREFIX + "k2"));
      cb.append("if (" + Consts.RIO_PREFIX + "ret != 0) { return " + 
          Consts.RIO_PREFIX + "ret; }\n");
      cb.append("}\n");
      cb.append(Consts.RIO_PREFIX + "ret = ("+getId(Consts.RIO_PREFIX + "set1")+
          ".size() - "+getId(Consts.RIO_PREFIX + "set2")+".size());\n");
      cb.append("}\n");
    }
    
    void genReadMethod(CodeBuffer cb, String fname, String tag, boolean decl) {
      if (decl) {
        cb.append(getType()+" "+fname+";\n");
      }
      cb.append("{\n");
      incrLevel();
      cb.append("org.apache.hadoop.record.Index " + 
          getId(Consts.RIO_PREFIX + "midx")+" = " + 
          Consts.RECORD_INPUT + ".startMap(\""+tag+"\");\n");
      cb.append(fname+"=new "+getType()+"();\n");
      cb.append("for (; !"+getId(Consts.RIO_PREFIX + "midx")+".done(); "+
          getId(Consts.RIO_PREFIX + "midx")+".incr()) {\n");
      key.genReadMethod(cb, getId(Consts.RIO_PREFIX + "k"),
          getId(Consts.RIO_PREFIX + "k"), true);
      value.genReadMethod(cb, getId(Consts.RIO_PREFIX + "v"), 
          getId(Consts.RIO_PREFIX + "v"), true);
      cb.append(fname+".put("+getId(Consts.RIO_PREFIX + "k")+","+
          getId(Consts.RIO_PREFIX + "v")+");\n");
      cb.append("}\n");
      cb.append(Consts.RECORD_INPUT + ".endMap(\""+tag+"\");\n");
      decrLevel();
      cb.append("}\n");
    }
    
    void genWriteMethod(CodeBuffer cb, String fname, String tag) {
      String setType = "java.util.Set<java.util.Map.Entry<"+
        key.getWrapperType()+","+value.getWrapperType()+">> ";
      String entryType = "java.util.Map.Entry<"+
        key.getWrapperType()+","+value.getWrapperType()+"> ";
      String iterType = "java.util.Iterator<java.util.Map.Entry<"+
        key.getWrapperType()+","+value.getWrapperType()+">> ";
      cb.append("{\n");
      incrLevel();
      cb.append(Consts.RECORD_OUTPUT + ".startMap("+fname+",\""+tag+"\");\n");
      cb.append(setType+getId(Consts.RIO_PREFIX + "es")+" = "+
          fname+".entrySet();\n");
      cb.append("for("+iterType+getId(Consts.RIO_PREFIX + "midx")+" = "+
          getId(Consts.RIO_PREFIX + "es")+".iterator(); "+
          getId(Consts.RIO_PREFIX + "midx")+".hasNext();) {\n");
      cb.append(entryType+getId(Consts.RIO_PREFIX + "me")+" = "+
          getId(Consts.RIO_PREFIX + "midx")+".next();\n");
      cb.append(key.getType()+" "+getId(Consts.RIO_PREFIX + "k")+" = "+
          getId(Consts.RIO_PREFIX + "me")+".getKey();\n");
      cb.append(value.getType()+" "+getId(Consts.RIO_PREFIX + "v")+" = "+
          getId(Consts.RIO_PREFIX + "me")+".getValue();\n");
      key.genWriteMethod(cb, getId(Consts.RIO_PREFIX + "k"), 
          getId(Consts.RIO_PREFIX + "k"));
      value.genWriteMethod(cb, getId(Consts.RIO_PREFIX + "v"), 
          getId(Consts.RIO_PREFIX + "v"));
      cb.append("}\n");
      cb.append(Consts.RECORD_OUTPUT + ".endMap("+fname+",\""+tag+"\");\n");
      cb.append("}\n");
      decrLevel();
    }
    
    void genSlurpBytes(CodeBuffer cb, String b, String s, String l) {
      cb.append("{\n");
      incrLevel();
      cb.append("int "+getId("mi")+
                " = org.apache.hadoop.record.Utils.readVInt("+b+", "+s+");\n");
      cb.append("int "+getId("mz")+
                " = org.apache.hadoop.record.Utils.getVIntSize("+getId("mi")+");\n");
      cb.append(s+"+="+getId("mz")+"; "+l+"-="+getId("mz")+";\n");
      cb.append("for (int "+getId("midx")+" = 0; "+getId("midx")+
                " < "+getId("mi")+"; "+getId("midx")+"++) {");
      key.genSlurpBytes(cb, b, s, l);
      value.genSlurpBytes(cb, b, s, l);
      cb.append("}\n");
      decrLevel();
      cb.append("}\n");
    }
    
    void genCompareBytes(CodeBuffer cb) {
      cb.append("{\n");
      incrLevel();
      cb.append("int "+getId("mi1")+
                " = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n");
      cb.append("int "+getId("mi2")+
                " = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n");
      cb.append("int "+getId("mz1")+
                " = org.apache.hadoop.record.Utils.getVIntSize("+getId("mi1")+");\n");
      cb.append("int "+getId("mz2")+
                " = org.apache.hadoop.record.Utils.getVIntSize("+getId("mi2")+");\n");
      cb.append("s1+="+getId("mz1")+"; s2+="+getId("mz2")+
                "; l1-="+getId("mz1")+"; l2-="+getId("mz2")+";\n");
      cb.append("for (int "+getId("midx")+" = 0; "+getId("midx")+
                " < "+getId("mi1")+" && "+getId("midx")+" < "+getId("mi2")+
                "; "+getId("midx")+"++) {");
      key.genCompareBytes(cb);
      value.genSlurpBytes(cb, "b1", "s1", "l1");
      value.genSlurpBytes(cb, "b2", "s2", "l2");
      cb.append("}\n");
      cb.append("if ("+getId("mi1")+" != "+getId("mi2")+
                ") { return ("+getId("mi1")+"<"+getId("mi2")+")?-1:0; }\n");
      decrLevel();
      cb.append("}\n");
    }
  }
  
  class CppMap extends CppCompType {
    
    JType.CppType key;
    JType.CppType value;
    
    CppMap(JType.CppType key, JType.CppType value) {
      super("::std::map< "+key.getType()+", "+ value.getType()+" >");
      this.key = key;
      this.value = value;
    }
    
    String getTypeIDObjectString() {
      return "new ::hadoop::MapTypeID(" + 
        key.getTypeIDObjectString() + ", " + 
        value.getTypeIDObjectString() + ")";
    }

    void genSetRTIFilter(CodeBuffer cb) {
      key.genSetRTIFilter(cb);
      value.genSetRTIFilter(cb);
    }

  }
  
  /** Creates a new instance of JMap */
  public JMap(JType t1, JType t2) {
    setJavaType(new JavaMap(t1.getJavaType(), t2.getJavaType()));
    setCppType(new CppMap(t1.getCppType(), t2.getCppType()));
    setCType(new CType());
    keyType = t1;
    valueType = t2;
  }
  
  String getSignature() {
    return "{" + keyType.getSignature() + valueType.getSignature() +"}";
  }
}
